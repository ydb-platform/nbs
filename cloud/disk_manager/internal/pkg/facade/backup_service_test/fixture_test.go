package tests

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/stretchr/testify/require"
	nbsproto "github.com/ydb-platform/nbs/cloud/blockstore/public/api/protos"
	nbssdk "github.com/ydb-platform/nbs/cloud/blockstore/public/sdk/go/client"
	dm "github.com/ydb-platform/nbs/cloud/disk_manager/api"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/backup"
	dpproto "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/protos"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage/compressor"
	storagemetrics "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage/metrics"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/facade/testcommon"
	metrics "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/monitoring/metrics"
	sdk "github.com/ydb-platform/nbs/cloud/disk_manager/pkg/client"
	"github.com/ydb-platform/nbs/cloud/tasks/persistence"
)

const chunkSize = 4 << 20
const blockSize = 4096

type backupObjects interface {
	GetObject(context.Context, string) (persistence.S3Object, error)
}
type backupReader struct {
	source backupObjects
	size   uint64
	ids    []string
}

func openBackup(ctx context.Context, source backupObjects, key, id string) (*backupReader, error) {
	object, err := source.GetObject(ctx, key)
	if err != nil {
		return nil, err
	}
	var meta struct {
		ID             string `json:"id"`
		Size           uint64 `json:"size"`
		EncryptionMode uint32 `json:"encryption_mode"`
	}
	if err = json.Unmarshal(object.Data, &meta); err != nil {
		return nil, err
	}
	if meta.ID != id || meta.Size == 0 || meta.Size%chunkSize != 0 || meta.EncryptionMode != 0 {
		return nil, fmt.Errorf("unsupported or inconsistent backup metadata: %+v", meta)
	}
	object, err = source.GetObject(ctx, backup.ChunkMapKey(id))
	if err != nil {
		return nil, err
	}
	var mapping dpproto.BackupChunkMap
	if err = proto.Unmarshal(object.Data, &mapping); err != nil {
		return nil, err
	}
	if uint64(len(mapping.ChunkIds)) != meta.Size/chunkSize {
		return nil, fmt.Errorf("incomplete chunk map: %d entries for %d bytes", len(mapping.ChunkIds), meta.Size)
	}
	return &backupReader{source: source, size: meta.Size, ids: mapping.ChunkIds}, nil
}
func (r *backupReader) chunk(ctx context.Context, index int) ([]byte, error) {
	result := make([]byte, chunkSize)
	if r.ids[index] == "" {
		return result, nil
	}
	object, err := r.source.GetObject(ctx, backup.ChunkKey(r.ids[index]))
	if err != nil {
		return nil, err
	}
	checksum := object.Metadata["Checksum"]
	if checksum == nil {
		return nil, fmt.Errorf("missing checksum for %s", r.ids[index])
	}
	want, err := strconv.ParseUint(*checksum, 10, 32)
	if err != nil {
		return nil, err
	}
	format := ""
	if value := object.Metadata["Compression"]; value != nil {
		format = *value
	}
	if format == "" && len(object.Data) != chunkSize {
		return nil, fmt.Errorf("truncated raw chunk %s: %d", r.ids[index], len(object.Data))
	}
	err = compressor.Decompress(format, object.Data, result, storagemetrics.New(metrics.NewEmptyRegistry(), "backup-oracle"))
	if err != nil {
		return nil, err
	}
	if uint32(want) != crc32.ChecksumIEEE(result) {
		return nil, fmt.Errorf("checksum mismatch for %s", r.ids[index])
	}
	return result, nil
}

// Expected bytes depend only on the independently chosen generation and offset.
func pattern(generation, index int) []byte {
	b := make([]byte, chunkSize)
	seed := uint64(0)
	if generation != 0 {
		switch index {
		case 0:
			seed = 19
		case 1:
			seed = 37
		case 3:
			seed = 83
		}
	}
	if generation == 2 {
		if index == 1 {
			seed = 101
		}
		if index == 3 {
			seed = 0
		}
		if index == 4 {
			seed = 131
		}
	}
	if seed == 0 {
		return b
	}
	for i := range b {
		seed ^= seed << 13
		seed ^= seed >> 7
		seed ^= seed << 17
		b[i] = byte(seed)
	}
	return b
}
func equalChunk(t *testing.T, want, got []byte, offset uint64) {
	t.Helper()
	require.True(t, bytes.Equal(want, got), "byte mismatch at chunk offset %d: expected SHA256 %x, actual %x", offset, sha256.Sum256(want), sha256.Sum256(got))
}

type faultRule struct {
	ID             string `json:"id"`
	Route          string `json:"route"`
	Method         string `json:"method"`
	Contains       string `json:"contains"`
	DiskID         string `json:"disk_id"`
	Mode           string `json:"mode"`
	BytesPerSecond int64  `json:"bytes_per_second"`
}
type faultEvent struct {
	Sequence                          int
	Route, Method, Key, Rule, Outcome string
	Started, Ended                    time.Time
	Bytes                             int
}
type fixture struct {
	t                                                         *testing.T
	ctx                                                       context.Context
	dm                                                        sdk.Client
	nbs                                                       *nbssdk.Client
	backup                                                    *backup.S3
	kind                                                      dm.DiskKind
	size                                                      uint64
	serial                                                    int
	period, createT0, backupT0, restoreT0, deleteT0, cancelT0 time.Duration
	readRate, snapshotRate, writeRate                         float64
}

func newFixture(t *testing.T, kind dm.DiskKind) *fixture {
	t.Helper()
	ctx, cancel := context.WithTimeout(testcommon.NewContext(), 60*time.Minute)
	t.Cleanup(cancel)
	client, err := testcommon.NewClient(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { client.Close() })
	timeout := 5 * time.Second
	nbs, err := nbssdk.NewClient(&nbssdk.GrpcClientOpts{
		Endpoint:    "localhost:" + os.Getenv("DISK_MANAGER_RECIPE_NBS_PORT"),
		Credentials: &nbssdk.ClientCredentials{RootCertsFile: os.Getenv("DISK_MANAGER_RECIPE_ROOT_CERTS_FILE")},
		Timeout:     &timeout, ClientId: t.Name(),
	}, &nbssdk.DurableClientOpts{Timeout: &timeout}, nbssdk.NewStderrLog(nbssdk.LOG_ERROR))
	require.NoError(t, err)
	t.Cleanup(func() { _ = nbs.Close() })
	var source *backup.S3
	seconds := 10
	if port := os.Getenv("DISK_MANAGER_BACKUP_S3_PORT"); port != "" {
		endpoint := "http://localhost:" + port
		s3, err := persistence.NewS3Client(endpoint, "test", persistence.NewS3Credentials("test", "test"), 2*time.Second, metrics.NewEmptyRegistry(), 0, nil, nil)
		require.NoError(t, err)
		require.NoError(t, s3.CreateBucket(ctx, "backup"))
		source, err = backup.NewS3(s3, "backup", "", "", nil)
		require.NoError(t, err)
		seconds, err = strconv.Atoi(os.Getenv("DISK_MANAGER_BACKUP_MAX_PERIOD_SECONDS"))
		require.NoError(t, err)
		require.Positive(t, seconds)
	}
	size := uint64(16 << 20)
	if kind == dm.DiskKind_DISK_KIND_SSD_NONREPLICATED {
		size = 1 << 30
	}
	f := &fixture{t: t, ctx: ctx, dm: client, nbs: nbs, backup: source, kind: kind, size: size, period: time.Duration(seconds) * time.Second}
	t.Cleanup(func() { f.rules() })
	return f
}
func (f *fixture) id(suffix string) string {
	f.serial++
	return strings.ReplaceAll(f.t.Name(), "/", "-") + "-" + suffix + "-" + strconv.Itoa(f.serial)
}
func (f *fixture) req() context.Context { return testcommon.GetRequestContext(f.t, f.ctx) }
func diskID(id string) *dm.DiskId       { return &dm.DiskId{ZoneId: "zone-a", DiskId: id} }
func (f *fixture) wait(operation *dm.Operation, limit time.Duration) (*dm.Operation, error) {
	deadline := time.Now().Add(limit)
	ctx, cancel := context.WithDeadline(f.ctx, deadline)
	defer cancel()
	var latest *dm.Operation
	for {
		op, err := f.dm.GetOperation(ctx, &dm.GetOperationRequest{OperationId: operation.Id})
		if err != nil {
			return latest, err
		}
		latest = op
		if op.Done {
			if e := op.GetError(); e != nil {
				return op, fmt.Errorf("operation %s: %d %s", op.Id, e.Code, e.Message)
			}
			return op, nil
		}
		if time.Now().After(deadline) {
			return op, fmt.Errorf("operation %s did not finish before fixed deadline", op.Id)
		}
		select {
		case <-f.ctx.Done():
			return op, f.ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
}
func (f *fixture) success(op *dm.Operation, err error, limit time.Duration) {
	f.t.Helper()
	require.NoError(f.t, err)
	_, err = f.wait(op, limit)
	require.NoError(f.t, err)
}
func (f *fixture) empty(id string, size uint64) {
	op, err := f.dm.CreateDisk(f.req(), &dm.CreateDiskRequest{Src: &dm.CreateDiskRequest_SrcEmpty{SrcEmpty: &empty.Empty{}}, DiskId: diskID(id), Size: int64(size), BlockSize: blockSize, Kind: f.kind})
	f.success(op, err, 5*time.Minute)
}
func (f *fixture) session(id string) *nbssdk.Session {
	s := nbssdk.NewSession(*f.nbs, nbssdk.NewStderrLog(nbssdk.LOG_ERROR))
	err := s.MountVolume(f.ctx, id, &nbssdk.MountVolumeOpts{MountFlags: 1 << uint32(nbsproto.EMountFlag_MF_THROTTLING_DISABLED), AccessMode: nbsproto.EVolumeAccessMode_VOLUME_ACCESS_READ_WRITE, MountMode: nbsproto.EVolumeMountMode_VOLUME_MOUNT_REMOTE})
	require.NoError(f.t, err)
	return s
}
func (f *fixture) fill(id string, generation int) {
	f.fillChanged(id, -1, generation)
}

// A checkpoint tracks writes, so unchanged bytes must not be rewritten.
func (f *fixture) fillChanged(id string, previous, generation int) {
	s := f.session(id)
	defer s.Close()
	defer s.UnmountVolume(f.ctx)
	for index := 0; uint64(index*chunkSize) < f.size; index++ {
		if index > 4 {
			break
		}
		b := pattern(generation, index)
		if previous >= 0 && bytes.Equal(b, pattern(previous, index)) {
			continue
		}
		blocks := make([][]byte, chunkSize/blockSize)
		for k := range blocks {
			blocks[k] = b[k*blockSize : (k+1)*blockSize]
		}
		require.NoError(f.t, s.WriteBlocks(f.ctx, uint64(index*chunkSize/blockSize), blocks))
	}
}
func (f *fixture) verifyDisk(id string, size uint64, generation int) {
	params, err := f.dm.DescribeDisk(f.req(), &dm.DescribeDiskRequest{DiskId: diskID(id)})
	require.NoError(f.t, err)
	require.Equal(f.t, int64(size), params.Size)
	s := f.session(id)
	defer s.Close()
	defer s.UnmountVolume(f.ctx)
	for index := 0; uint64(index*chunkSize) < size; index++ {
		blocks, err := s.ReadBlocks(f.ctx, uint64(index*chunkSize/blockSize), chunkSize/blockSize, "")
		require.NoError(f.t, err)
		// NBS may encode zero blocks as empty buffers.
		got := make([]byte, chunkSize)
		require.Len(f.t, blocks, chunkSize/blockSize)
		for i, b := range blocks {
			require.True(f.t, len(b) == 0 || len(b) == blockSize)
			copy(got[i*blockSize:(i+1)*blockSize], b)
		}
		equalChunk(f.t, pattern(generation, index), got, uint64(index*chunkSize))
	}
}
func (f *fixture) createSnapshot(id, source string) (*dm.Operation, error) {
	return f.dm.CreateSnapshot(f.req(), &dm.CreateSnapshotRequest{Src: diskID(source), SnapshotId: id, FolderId: "backup-acceptance"})
}
func (f *fixture) backupReady(key, id string, generation int, limit time.Duration) *backupReader {
	deadline := time.Now().Add(limit)
	var last error
	ctx, cancel := context.WithDeadline(f.ctx, deadline)
	defer cancel()
	for {
		reader, err := openBackup(ctx, f.backup, key, id)
		if err == nil {
			for index := range reader.ids {
				var b []byte
				b, err = reader.chunk(ctx, index)
				if err != nil {
					break
				}
				equalChunk(f.t, pattern(generation, index), b, uint64(index*chunkSize))
			}
			if err == nil {
				return reader
			}
		}
		last = err
		if time.Now().After(deadline) {
			f.t.Fatalf("backup %s incomplete before fixed deadline: %v", id, last)
		}
		time.Sleep(100 * time.Millisecond)
	}
}
func (f *fixture) restoreBackup(reader *backupReader, generation int) string {
	original := f.ctx
	ctx, cancel := context.WithTimeout(original, f.window(f.restoreT0))
	f.ctx = ctx
	defer func() { f.ctx = original; cancel() }()
	id := f.id("from-backup")
	f.empty(id, reader.size)
	s := f.session(id)
	for index, chunkID := range reader.ids {
		if chunkID == "" {
			continue
		}
		data, err := reader.chunk(f.ctx, index)
		require.NoError(f.t, err)
		blocks := make([][]byte, chunkSize/blockSize)
		for i := range blocks {
			blocks[i] = data[i*blockSize : (i+1)*blockSize]
		}
		require.NoError(f.t, s.WriteBlocks(f.ctx, uint64(index*chunkSize/blockSize), blocks))
	}
	require.NoError(f.t, s.UnmountVolume(f.ctx))
	s.Close()
	f.verifyDisk(id, reader.size, generation)
	return id
}
func (f *fixture) restoreSnapshot(snapshot string, size uint64, generation int) string {
	original := f.ctx
	ctx, cancel := context.WithTimeout(original, f.window(f.restoreT0))
	f.ctx = ctx
	defer func() { f.ctx = original; cancel() }()
	id := f.id("from-snapshot")
	op, err := f.dm.CreateDisk(f.req(), &dm.CreateDiskRequest{Src: &dm.CreateDiskRequest_SrcSnapshotId{SrcSnapshotId: snapshot}, DiskId: diskID(id), Kind: f.kind, Size: int64(size), BlockSize: blockSize})
	f.success(op, err, f.window(f.restoreT0))
	f.verifyDisk(id, size, generation)
	return id
}
func (f *fixture) rules(rules ...faultRule) {
	if os.Getenv("DISK_MANAGER_BACKUP_FAULT_PORT") == "" {
		require.Empty(f.t, rules, "cannot inject without backup stand")
		return
	}
	if rules == nil {
		rules = []faultRule{}
	}
	data, err := json.Marshal(rules)
	require.NoError(f.t, err)
	req, err := http.NewRequest(http.MethodPut, "http://localhost:"+os.Getenv("DISK_MANAGER_BACKUP_FAULT_PORT"), bytes.NewReader(data))
	require.NoError(f.t, err)
	client := http.Client{Timeout: 5 * time.Second}
	resp, err := client.Do(req)
	require.NoError(f.t, err)
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	require.Equal(f.t, 200, resp.StatusCode, string(body))
}
func (f *fixture) events() []faultEvent {
	client := http.Client{Timeout: 5 * time.Second}
	r, err := client.Get("http://localhost:" + os.Getenv("DISK_MANAGER_BACKUP_FAULT_PORT"))
	require.NoError(f.t, err)
	defer r.Body.Close()
	var events []faultEvent
	require.NoError(f.t, json.NewDecoder(r.Body).Decode(&events))
	return events
}
func (f *fixture) hit(id string, limit time.Duration) faultEvent {
	deadline := time.Now().Add(limit)
	for {
		for _, e := range f.events() {
			if e.Rule == id && e.Outcome == "injection-enter" {
				return e
			}
		}
		require.True(f.t, time.Now().Before(deadline), "fault %s did not intersect its target phase", id)
		time.Sleep(100 * time.Millisecond)
	}
}
func (f *fixture) window(t0 time.Duration) time.Duration {
	if t0 == 0 {
		return 5 * time.Minute
	}
	return 10*t0 + 5*f.period
}
func maximum(a, b time.Duration) time.Duration {
	if a > b {
		return a
	}
	return b
}
func (f *fixture) calibrate() {
	for trial := 0; trial < 3; trial++ {
		source, id := f.id("control-disk"), f.id("control-snapshot")
		f.empty(source, f.size)
		writeStart := time.Now()
		f.fill(source, 1)
		written := f.size
		if written > 5*chunkSize {
			written = 5 * chunkSize
		}
		f.writeRate = minPositive(f.writeRate, float64(written)/time.Since(writeStart).Seconds())
		events := f.events()
		after := len(events)
		start := time.Now()
		op, err := f.createSnapshot(id, source)
		f.success(op, err, 5*time.Minute)
		f.createT0 = maximum(f.createT0, time.Since(start))
		readRate, _, _ := observedRate(f.events(), "nbs", "ReadBlocks", "", after)
		snapshotRate, _, _ := observedRate(f.events(), "primary", "PUT", "", after)
		require.Positive(f.t, readRate)
		require.Positive(f.t, snapshotRate)
		f.readRate = minPositive(f.readRate, readRate)
		f.snapshotRate = minPositive(f.snapshotRate, snapshotRate)
		start = time.Now()
		reader := f.backupReady(backup.SnapshotMetaKey(source, id), id, 1, 5*time.Minute)
		f.backupT0 = maximum(f.backupT0, time.Since(start))
		start = time.Now()
		restored := f.restoreBackup(reader, 1)
		f.restoreT0 = maximum(f.restoreT0, time.Since(start))
		start = time.Now()
		op, err = f.dm.DeleteDisk(f.req(), &dm.DeleteDiskRequest{DiskId: diskID(restored)})
		f.success(op, err, 5*time.Minute)
		f.deleteT0 = maximum(f.deleteT0, time.Since(start))
		op, err = f.dm.DeleteDisk(f.req(), &dm.DeleteDiskRequest{DiskId: diskID(source)})
		f.success(op, err, 5*time.Minute)
	}
	f.t.Logf("CALIBRATION kind=%v size=%d P=%v D=%v T0(create=%v,backup=%v,restore=%v,delete=%v) W(create=%v,backup=%v,restore=%v)", f.kind, f.size, f.period, maximum(60*time.Second, 3*f.period), f.createT0, f.backupT0, f.restoreT0, f.deleteT0, f.window(f.createT0), f.window(f.backupT0), f.window(f.restoreT0))
}

func observedRate(events []faultEvent, route, method, rule string, after int) (float64, time.Time, time.Time) {
	var first, last time.Time
	total := 0
	for _, e := range events {
		if e.Sequence <= after || e.Route != route || e.Method != method || (rule != "" && e.Rule != rule) || (e.Outcome != "ok" && e.Outcome != "200") || e.Bytes == 0 {
			continue
		}
		if first.IsZero() || e.Started.Before(first) {
			first = e.Started
		}
		if e.Ended.After(last) {
			last = e.Ended
		}
		total += e.Bytes
	}
	if !last.After(first) {
		return 0, first, last
	}
	return float64(total) / last.Sub(first).Seconds(), first, last
}
func minPositive(old, value float64) float64 {
	if old == 0 || value < old {
		return value
	}
	return old
}
