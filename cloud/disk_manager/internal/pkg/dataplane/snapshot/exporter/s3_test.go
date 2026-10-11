package exporter

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	common "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/common"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/types"
	task_errors "github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/persistence"
)

type s3TestSource struct {
	storage.Storage // Unexpected calls (especially mutations) fail the test.
	meta            storage.SnapshotMeta
	entries         []storage.ChunkMapEntry
	readyErr        error
	mapErr          error
	readErr         error
	readyCalls      int
	mapCalls        int
	readCalls       atomic.Int32
	activeReads     atomic.Int32
	blockReads      bool
	readStarted     chan struct{}
}

func (s *s3TestSource) CheckSnapshotReady(
	context.Context,
	string,
) (storage.SnapshotMeta, error) {

	s.readyCalls++
	return s.meta, s.readyErr
}

func (s *s3TestSource) ReadChunkMap(
	ctx context.Context,
	snapshotID string,
	start uint32,
) (<-chan storage.ChunkMapEntry, <-chan error) {

	s.mapCalls++
	return newChunkMapChannels(
		filterPartitionEntriesFrom(s.entries, start),
		s.mapErr,
	)
}

func (s *s3TestSource) ReadChunk(
	ctx context.Context,
	chunk *common.Chunk,
) error {

	s.readCalls.Add(1)
	s.activeReads.Add(1)
	defer s.activeReads.Add(-1)
	if s.blockReads {
		s.readStarted <- struct{}{}
		<-ctx.Done()
		return ctx.Err()
	}

	for i := range chunk.Data {
		chunk.Data[i] = chunkDataByte(chunk.Index)
	}

	return s.readErr
}

type s3TestUpload struct {
	key     string
	data    []byte
	options persistence.S3PutObjectOptions
}

type s3TestDestination struct {
	exists       bool
	bucketErr    error
	uploads      []s3TestUpload
	objects      map[string][]byte
	put          func(string, []byte) error
	beforePut    func(string, persistence.S3Object)
	readErr      error
	readMismatch bool
	readCalls    int
}

func newS3TestDestination() *s3TestDestination {
	return &s3TestDestination{exists: true, objects: make(map[string][]byte)}
}

func (d *s3TestDestination) BucketExists(
	context.Context,
	string,
) (bool, error) {

	return d.exists, d.bucketErr
}

func (d *s3TestDestination) PutObjectWithOptions(
	ctx context.Context,
	bucket string,
	key string,
	object persistence.S3Object,
	options persistence.S3PutObjectOptions,
) error {

	if err := ctx.Err(); err != nil {
		return err
	}

	if d.beforePut != nil {
		d.beforePut(key, object)
	}

	data := append([]byte(nil), object.Data...)
	d.uploads = append(d.uploads, s3TestUpload{key, data, options})
	if d.put != nil {
		if err := d.put(key, data); err != nil {
			return err
		}
	}

	d.objects[key] = data
	return nil
}

func (d *s3TestDestination) GetObjectWithLimit(
	ctx context.Context,
	bucket string,
	key string,
	limit int64,
) ([]byte, error) {

	d.readCalls++
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	if d.readErr != nil {
		return nil, d.readErr
	}

	if d.readMismatch {
		return []byte("different"), nil
	}

	data := d.objects[key]
	if int64(len(data)) > limit {
		return nil, fmt.Errorf("read limit exceeded")
	}

	return data, nil
}

func s3TestOptions() S3ExportOptions {
	return S3ExportOptions{
		Bucket:          "destination",
		Prefix:          "/parent/",
		PartSize:        2 * chunkSize,
		ReadWorkerCount: 2,
	}
}

func TestS3PartitionPlanning(t *testing.T) {
	for _, tc := range []struct {
		name     string
		size     uint64
		chunks   uint32
		partSize uint64
		count    uint32
		invalid  bool
	}{
		{"empty", 0, 0, chunkSize, 0, false},
		{"partial", 7, 1, chunkSize, 1, false},
		{"exact", 4 * chunkSize, 4, 2 * chunkSize, 2, false},
		{"balanced", 5*chunkSize - 1, 5, 2 * chunkSize, 3, false},
		{"max part", 2*MaxS3PartSize + 1, 513, MaxS3PartSize, 3, false},
		{
			"max chunks",
			uint64(math.MaxUint32) * chunkSize,
			math.MaxUint32,
			chunkSize,
			math.MaxUint32,
			false,
		},

		{"zero part", 0, 0, 0, 0, true},
		{"unaligned part", 1, 1, chunkSize + 1, 0, true},
		{"oversized part", 1, 1, MaxS3PartSize + chunkSize, 0, true},
		{"overflow size", math.MaxUint64, math.MaxUint32, chunkSize, 0, true},
		{
			"too many chunks",
			uint64(math.MaxUint32)*chunkSize + 1,
			math.MaxUint32,
			chunkSize,
			0,
			true,
		},

		{"missing chunks", chunkSize + 1, 1, chunkSize, 0, true},
		{"extra chunks", chunkSize, 2, chunkSize, 0, true},
		{"invalid empty", 0, 1, chunkSize, 0, true},
	} {
		t.Run(
			tc.name,
			func(t *testing.T) {
				count, err := s3PartitionCount(
					storage.SnapshotMeta{Size: tc.size, ChunkCount: tc.chunks},
					tc.partSize,
				)
				if tc.invalid {
					require.Error(t, err)
					return
				}

				require.NoError(t, err)
				require.Equal(t, tc.count, count)
				if count == 0 {
					return
				}

				for _, number := range []uint32{1, (count-1)/2 + 1, count} {
					start, end, err := partitionChunkRange(tc.chunks, number, count)
					require.NoError(t, err)
					length, err := partitionSize(tc.size, start, end)
					require.NoError(t, err)
					require.Positive(t, length)
					require.LessOrEqual(t, length, tc.partSize)
				}
			},
		)
	}

	require.Error(t, ValidateReadWorkers(math.MaxInt))
}

func TestS3ExportReconstructionAndDistinctBackups(t *testing.T) {
	source := &s3TestSource{
		meta: storage.SnapshotMeta{Size: 4*chunkSize + 123, ChunkCount: 5},
		entries: []storage.ChunkMapEntry{
			{ChunkIndex: 0, ChunkID: "data-0"},
			{ChunkIndex: 1},
			{ChunkIndex: 2, ChunkID: "data-2", StoredInS3: true},
			{ChunkIndex: 4, ChunkID: "data-4"},
		},
	}

	dest := newS3TestDestination()
	options := s3TestOptions()
	for invocation := 0; invocation < 2; invocation++ {
		before := len(dest.uploads)
		manifest, err := ExportToS3(newContext(), source, "snapshot", dest, options)
		require.NoError(t, err)
		require.Equal(t, uint32(3), manifest.PartCount)
		require.Equal(t, "partitioned-raw-image", manifest.Format)
		require.Equal(t, uint32(1), manifest.FormatVersion)
		require.NoError(t, validateS3Manifest(manifest))
		prefix := s3BackupPrefix(options.Prefix, "snapshot", manifest.BackupUUID)
		require.True(t, strings.HasPrefix(prefix, "parent/snapshot-backup-"))
		var reconstructed bytes.Buffer
		for i, part := range manifest.Parts {
			data := dest.objects[prefix+part.Key]
			require.Equal(t, part.Length, uint64(len(data)))
			require.Equal(t, part.Offset, uint64(reconstructed.Len()))
			sum := sha256.Sum256(data)
			require.Equal(t, part.SHA256, hex.EncodeToString(sum[:]))
			reconstructed.Write(data)
			require.Equal(t, prefix+part.Key, dest.uploads[before+i].key)
			require.Equal(
				t,
				"application/octet-stream",
				dest.uploads[before+i].options.ContentType,
			)
			require.True(t, dest.uploads[before+i].options.ContentMD5)
		}

		var stdout bytes.Buffer
		_, err = ExportPartitionToWriterWithReadWorkers(
			newContext(),
			source,
			"snapshot",
			&stdout,
			1,
			1,
			2,
		)
		require.NoError(t, err)
		require.Equal(t, stdout.Bytes(), reconstructed.Bytes())
		require.Equal(
			t,
			make([]byte, chunkSize),
			reconstructed.Bytes()[chunkSize:2*chunkSize],
		)
		require.Equal(
			t,
			make([]byte, chunkSize),
			reconstructed.Bytes()[3*chunkSize:4*chunkSize],
		)
		last := dest.uploads[len(dest.uploads)-1]
		require.Equal(t, prefix+"manifest.json", last.key)
		require.Equal(t, "application/json", last.options.ContentType)
		require.True(t, last.options.ContentMD5)
		var stored S3Manifest
		require.NoError(t, json.Unmarshal(last.data, &stored))
		require.Equal(t, manifest, stored)
	}

	require.NotEqual(t, dest.uploads[0].key, dest.uploads[4].key)
	require.Len(t, dest.objects, 8)
}

func TestS3ExportEmptyAndEncryptionMetadata(t *testing.T) {
	for _, mode := range []types.EncryptionMode{
		types.EncryptionMode_NO_ENCRYPTION,
		types.EncryptionMode_ENCRYPTION_AES_XTS,
		types.EncryptionMode_ENCRYPTION_WITH_ROOT_KMS_PROVIDED_KEY,
	} {
		t.Run(
			mode.String(),
			func(t *testing.T) {
				source := &s3TestSource{
					meta: storage.SnapshotMeta{
						Encryption: &types.EncryptionDesc{
							Mode: mode,
							Key: &types.EncryptionDesc_KeyHash{
								KeyHash: []byte("secret-key-material"),
							},
						},
					},
				}

				dest := newS3TestDestination()
				manifest, err := ExportToS3(
					newContext(),
					source,
					"snapshot",
					dest,
					s3TestOptions(),
				)
				require.NoError(t, err)
				require.Empty(t, manifest.Parts)
				require.NotNil(t, manifest.Parts)
				require.Zero(t, source.mapCalls)
				require.Len(t, dest.uploads, 1)
				require.Equal(t, mode.String(), manifest.EncryptionMode)
				expected := "plaintext"
				if mode == types.EncryptionMode_ENCRYPTION_AES_XTS {
					expected = "ciphertext"
				}

				require.Equal(t, expected, manifest.RawDataInterpretation)
				require.NotContains(t, string(dest.uploads[0].data), "secret-key-material")
			},
		)
	}
}

func TestS3ExportEarlyFailuresDoNotReadPayload(t *testing.T) {
	for _, tc := range []string{
		"not-ready",
		"metadata",
		"missing-bucket",
		"bucket-permission",
		"workers",
		"part-size",
		"canceled",
	} {
		t.Run(
			tc,
			func(t *testing.T) {
				source := &s3TestSource{
					meta: storage.SnapshotMeta{Size: chunkSize, ChunkCount: 1},
				}

				dest := newS3TestDestination()
				options := s3TestOptions()
				ctx, cancel := context.WithCancel(newContext())
				defer cancel()
				switch tc {
				case "not-ready":
					source.readyErr = errors.New("not ready")
				case "metadata":
					source.meta.ChunkCount = 0
				case "missing-bucket":
					dest.exists = false
				case "bucket-permission":
					dest.bucketErr = errors.New("access denied")
				case "workers":
					options.ReadWorkerCount = 0
				case "part-size":
					options.PartSize = 1
				case "canceled":
					cancel()
				}

				_, err := ExportToS3(ctx, source, "snapshot", dest, options)
				require.Error(t, err)
				require.Zero(t, source.mapCalls)
				require.Zero(t, source.readCalls.Load())
				require.Empty(t, dest.uploads)
			},
		)
	}
}

func TestS3ExportFailuresPreventManifest(t *testing.T) {
	for _, tc := range []string{"read", "map", "upload", "cancel-after-part"} {
		t.Run(
			tc,
			func(t *testing.T) {
				source := &s3TestSource{
					meta:    storage.SnapshotMeta{Size: 2 * chunkSize, ChunkCount: 2},
					entries: []storage.ChunkMapEntry{{ChunkIndex: 0, ChunkID: "data"}},
				}

				dest := newS3TestDestination()
				options := s3TestOptions()
				options.PartSize = chunkSize
				ctx, cancel := context.WithCancel(newContext())
				defer cancel()
				switch tc {
				case "read":
					source.readErr = errors.New("source disappeared")
				case "map":
					source.mapErr = errors.New("map failed")
				case "upload":
					dest.put = func(key string, data []byte) error {
						if strings.HasSuffix(key, "part-00000002.raw") {
							return task_errors.NewNonRetriableErrorf("access denied")
						}

						return nil
					}
				case "cancel-after-part":
					dest.put = func(string, []byte) error {
						cancel()
						return nil
					}
				}

				_, err := ExportToS3(ctx, source, "snapshot", dest, options)
				require.Error(t, err)
				require.Contains(t, err.Error(), "snapshot-backup-")
				for key := range dest.objects {
					require.False(t, strings.HasSuffix(key, "manifest.json"))
				}

				require.Zero(t, source.activeReads.Load())
				require.Zero(t, dest.readCalls)
			},
		)
	}
}

func TestS3ExportAmbiguousManifestPublication(t *testing.T) {
	for _, tc := range []string{
		"confirmed",
		"unavailable",
		"different",
		"permanent",
	} {
		t.Run(
			tc,
			func(t *testing.T) {
				dest := newS3TestDestination()
				dest.put = func(key string, data []byte) error {
					if tc == "permanent" {
						return task_errors.NewNonRetriableErrorf("denied")
					}

					dest.objects[key] = data // Remote success, response lost.
					return task_errors.NewRetriableErrorf("connection reset")
				}

				if tc == "unavailable" {
					dest.readErr = errors.New("read denied")
				}

				dest.readMismatch = tc == "different"
				_, err := ExportToS3(
					newContext(),
					&s3TestSource{},
					"snapshot",
					dest,
					s3TestOptions(),
				)
				if tc == "confirmed" {
					require.NoError(t, err)
				} else {
					require.Error(t, err)
					require.Contains(t, err.Error(), "publication is uncertain")
				}

				require.Equal(t, 1, dest.readCalls)
			},
		)
	}
}

func TestS3ExportCancellationWaitsForReadWorkers(t *testing.T) {
	source := &s3TestSource{
		meta: storage.SnapshotMeta{Size: 2 * chunkSize, ChunkCount: 2},
		entries: []storage.ChunkMapEntry{
			{ChunkIndex: 0, ChunkID: "a"},
			{ChunkIndex: 1, ChunkID: "b"},
		},

		blockReads:  true,
		readStarted: make(chan struct{}, 2),
	}

	dest := newS3TestDestination()
	ctx, cancel := context.WithCancel(newContext())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := ExportToS3(
			ctx,
			source,
			"snapshot",
			dest,
			s3TestOptions(),
		)
		done <- err
	}()

	select {
	case <-source.readStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("read did not start")
	}

	cancel()
	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(5 * time.Second):
		t.Fatal("export did not stop")
	}

	require.Zero(t, source.activeReads.Load())
	require.Empty(t, dest.uploads)
}

func TestS3BoundedWriterAndCoverageValidation(t *testing.T) {
	var buffer bytes.Buffer
	writer := boundedPartWriter{buffer: &buffer, remaining: 3}
	_, err := writer.Write([]byte("1234"))
	require.Error(t, err)
	require.Zero(t, buffer.Len())
	_, err = writer.Write([]byte("123"))
	require.NoError(t, err)
	_, err = writer.Write([]byte("4"))
	require.Error(t, err)
	sum := sha256.Sum256([]byte("a"))
	valid := S3Manifest{
		Size:        1,
		MaxPartSize: chunkSize,
		PartCount:   1,
		Parts: []S3ManifestPart{
			{
				Number: 1,
				Key:    "part-00000001.raw",
				Length: 1,
				SHA256: hex.EncodeToString(sum[:]),
			},
		},
	}

	require.NoError(t, validateS3Manifest(valid))
	for _, tc := range []string{
		"gap",
		"length",
		"count",
		"checksum",
		"overflow",
		"overlap",
	} {
		manifest := valid
		manifest.Parts = append([]S3ManifestPart(nil), valid.Parts...)
		switch tc {
		case "gap":
			manifest.Parts[0].Offset = 1
		case "length":
			manifest.Size = 2
		case "count":
			manifest.PartCount = 2
		case "checksum":
			manifest.Parts[0].SHA256 = "wrong"
		case "overflow":
			manifest.Parts[0].Length = math.MaxUint64
		case "overlap":
			manifest.Parts = append(manifest.Parts, manifest.Parts[0])
			manifest.PartCount = 2
		}

		require.Error(t, validateS3Manifest(manifest), tc)
	}
}

func TestS3ExportReusesOneBufferAndUploadsBeforeReadingNextPart(t *testing.T) {
	source := &s3TestSource{
		meta: storage.SnapshotMeta{Size: 5 * chunkSize, ChunkCount: 5},
	}

	dest := newS3TestDestination()
	var bufferStart *byte
	partCount := 0
	dest.beforePut = func(key string, object persistence.S3Object) {
		if strings.HasSuffix(key, "manifest.json") {
			require.Equal(t, 3, partCount)
			require.Len(t, dest.objects, 3)
			return
		}

		partCount++
		require.Equal(t, partCount, source.mapCalls)
		require.Zero(t, source.activeReads.Load())
		require.LessOrEqual(t, len(object.Data), 2*chunkSize)
		if bufferStart == nil {
			bufferStart = &object.Data[0]
		} else {
			require.True(t, bufferStart == &object.Data[0], "part buffer was not reused")
		}
	}

	_, err := ExportToS3(newContext(), source, "snapshot", dest, s3TestOptions())
	require.NoError(t, err)
	require.Equal(t, 1, source.readyCalls)
}
