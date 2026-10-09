package tests

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/stretchr/testify/require"
	disk_manager "github.com/ydb-platform/nbs/cloud/disk_manager/api"
	internal_client "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/client"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/backup"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/protos"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage/compressor"
	snapshot_metrics "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage/metrics"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/facade/testcommon"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/monitoring/metrics"
	"github.com/ydb-platform/nbs/cloud/tasks/persistence"
)

const backupChunkSize = 4 * 1024 * 1024

// This suite starts real CP/DP processes, YDB and NBS via the recipe. Only the
// backup S3 is emulated. It never schedules backup tasks directly: the regular
// production scheduler must discover each snapshot created through the API.
func TestSnapshotServiceBackupRoundTrip(t *testing.T) {
	ctx, cancel := context.WithTimeout(testcommon.NewContext(), 8*time.Minute)
	defer cancel()
	client, err := testcommon.NewClient(ctx)
	require.NoError(t, err)
	defer client.Close()
	var restartFiles []string
	if testcommon.IsNemesisEnabled() {
		// Random-restart smoke coverage only. A restart log does not prove a
		// crash between a specific S3 PUT and the task's persistent checkpoint.
		t.Log("Nemesis smoke: verify backup correctness with random CP/DP restarts; no deterministic crash boundary is asserted")
		require.NoError(t, json.Unmarshal([]byte(os.Getenv("DISK_MANAGER_RECIPE_RESTART_TIMINGS_FILES")), &restartFiles))
		require.NotEmpty(t, restartFiles)
	}
	restartSizes := make(map[string]int64)
	for _, path := range restartFiles {
		if info, err := os.Stat(path); err == nil {
			restartSizes[path] = info.Size()
		}
	}

	port := os.Getenv("DISK_MANAGER_RECIPE_BACKUP_S3_PORT")
	require.NotEmpty(t, port, "the suite requires recipe --backup")
	s3, err := persistence.NewS3Client(
		"http://localhost:"+port, "test",
		persistence.S3Credentials{ID: "test", Secret: "test"},
		2*time.Second, metrics.NewEmptyRegistry(), 0, nil, nil,
	)
	require.NoError(t, err)
	exists, err := s3.BucketExists(ctx, "snapshot-backup")
	require.NoError(t, err)
	if !exists {
		require.NoError(t, s3.CreateBucket(ctx, "snapshot-backup"))
	}
	var kek []byte
	var kekID string
	if path := os.Getenv("DISK_MANAGER_RECIPE_BACKUP_KEK_FILE"); path != "" {
		kek, err = os.ReadFile(path)
		require.NoError(t, err)
		kekID = "recipe-test-key"
	}
	reader, err := backup.NewS3(s3, "snapshot-backup", "recipe", kekID, kek)
	require.NoError(t, err)

	diskID := t.Name()
	const diskSize = 4 * backupChunkSize
	operation, err := client.CreateDisk(testcommon.GetRequestContext(t, ctx), &disk_manager.CreateDiskRequest{
		Src:    &disk_manager.CreateDiskRequest_SrcEmpty{SrcEmpty: &empty.Empty{}},
		DiskId: &disk_manager.DiskId{DiskId: diskID, ZoneId: "zone-a"},
		Size:   diskSize, BlockSize: 4096, Kind: disk_manager.DiskKind_DISK_KIND_SSD,
	})
	require.NoError(t, err)
	require.NoError(t, internal_client.WaitOperation(ctx, client, operation.Id))
	nbs := testcommon.NewNbsTestingClient(t, ctx, "zone-a")

	// Each snapshot has its own byte-for-byte reference. The second overwrites
	// one chunk and zeroes another; the third makes no changes at all. The
	// fourth clears the whole disk, including its first and last blocks.
	expected := bytes.Repeat([]byte{0x31}, diskSize)
	clear(expected[backupChunkSize : 2*backupChunkSize])
	var references [][]byte
	var snapshotIDs []string
	var previousChunkIDs []string
	for generation := 0; generation < 4; generation++ {
		var changedChunks []int
		switch generation {
		case 0:
			changedChunks = []int{0, 1, 2, 3}
		case 1:
			copy(expected[:backupChunkSize], bytes.Repeat([]byte{0x72}, backupChunkSize))
			clear(expected[2*backupChunkSize : 3*backupChunkSize])
			// Do not rewrite unchanged bytes: doing so marks all chunks dirty
			// and would turn this into another full-snapshot test.
			changedChunks = []int{0, 2}
		case 3:
			clear(expected)
			changedChunks = []int{0, 1, 2, 3}
		}
		if len(changedChunks) != 0 {
			session, err := nbs.MountRW(ctx, diskID, 0, 0, nil)
			require.NoError(t, err)
			for _, chunkIndex := range changedChunks {
				offset := chunkIndex * backupChunkSize
				data := expected[offset : offset+backupChunkSize]
				if bytes.Equal(data, make([]byte, backupChunkSize)) {
					err = session.Zero(ctx, uint64(offset/4096), backupChunkSize/4096)
				} else {
					err = session.Write(ctx, uint64(offset/4096), data)
				}
				require.NoError(t, err)
			}
			session.Close(ctx)
		}
		snapshotID := fmt.Sprintf("%s-%d", t.Name(), generation)
		operation, err = client.CreateSnapshot(testcommon.GetRequestContext(t, ctx), &disk_manager.CreateSnapshotRequest{
			Src:        &disk_manager.DiskId{DiskId: diskID, ZoneId: "zone-a"},
			SnapshotId: snapshotID, FolderId: "folder",
		})
		require.NoError(t, err)
		require.NoError(t, internal_client.WaitOperation(ctx, client, operation.Id))
		waitForBackupMap(t, ctx, reader, snapshotID)
		if generation == 0 && len(kek) != 0 {
			withoutKey, err := backup.NewS3(s3, "snapshot-backup", "recipe", "", nil)
			require.NoError(t, err)
			_, err = withoutKey.GetObject(ctx, backup.ChunkMapKey(snapshotID))
			require.Error(t, err, "encrypted backup must not be readable without its KEK")
			wrongKey := bytes.Clone(kek)
			wrongKey[0] ^= 1
			wrongReader, err := backup.NewS3(s3, "snapshot-backup", "recipe", kekID, wrongKey)
			require.NoError(t, err)
			_, err = wrongReader.GetObject(ctx, backup.ChunkMapKey(snapshotID))
			require.Error(t, err, "encrypted backup must not be readable with a wrong KEK")
		}
		chunkIDs := requireBackupContent(t, ctx, reader, diskID, snapshotID, expected)
		switch generation {
		case 1:
			require.NotEmpty(t, previousChunkIDs[3])
			require.Equal(t, previousChunkIDs[3], chunkIDs[3], "an unchanged nonzero chunk must be shared with the previous snapshot")
			require.NotEmpty(t, previousChunkIDs[0])
			require.NotEmpty(t, chunkIDs[0])
			require.NotEqual(t, previousChunkIDs[0], chunkIDs[0], "rewritten bytes must not reuse the old chunk")
			require.Empty(t, chunkIDs[2], "zeroed bytes must not retain the previous data chunk")
		case 2:
			require.Equal(t, previousChunkIDs, chunkIDs, "a snapshot without writes must reuse its predecessor's chunk map")
		}
		previousChunkIDs = chunkIDs
		references = append(references, bytes.Clone(expected))
		snapshotIDs = append(snapshotIDs, snapshotID)
	}

	// Delete all primary snapshots and the source disk before reading again.
	// The readback path below has no primary storage client to fall back to.
	for _, snapshotID := range snapshotIDs {
		operation, err = client.DeleteSnapshot(testcommon.GetRequestContext(t, ctx), &disk_manager.DeleteSnapshotRequest{SnapshotId: snapshotID})
		require.NoError(t, err)
		require.NoError(t, internal_client.WaitOperation(ctx, client, operation.Id))
	}
	testcommon.DeleteDisk(t, ctx, client, diskID)
	for i, snapshotID := range snapshotIDs {
		requireBackupContent(t, ctx, reader, diskID, snapshotID, references[i])
	}
	for _, path := range restartFiles {
		info, err := os.Stat(path)
		require.NoError(t, err, "Nemesis must actually restart every CP/DP process")
		require.Greater(t, info.Size(), restartSizes[path], "no restart occurred during this test: %s", path)
	}
}

func waitForBackupMap(t *testing.T, ctx context.Context, reader *backup.S3, snapshotID string) {
	t.Helper()
	var lastErr error
	require.Eventually(t, func() bool {
		_, lastErr = reader.GetObject(ctx, backup.ChunkMapKey(snapshotID))
		return lastErr == nil
	}, 2*time.Minute, time.Second, "backup map was not published for %s", snapshotID)
	require.NoError(t, lastErr)
}

func requireBackupContent(t *testing.T, ctx context.Context, reader *backup.S3, diskID, snapshotID string, expected []byte) []string {
	t.Helper()
	object, err := reader.GetObject(ctx, backup.SnapshotMetaKey(diskID, snapshotID))
	require.NoError(t, err)
	var meta backup.SnapshotMeta
	require.NoError(t, json.Unmarshal(object.Data, &meta))
	require.Equal(t, snapshotID, meta.ID)
	require.Equal(t, diskID, meta.DiskID)
	require.EqualValues(t, len(expected), meta.Size)

	object, err = reader.GetObject(ctx, backup.ChunkMapKey(snapshotID))
	require.NoError(t, err)
	var chunkMap protos.BackupChunkMap
	require.NoError(t, proto.Unmarshal(object.Data, &chunkMap))
	require.Len(t, chunkMap.ChunkIds, len(expected)/backupChunkSize)
	restored := make([]byte, len(expected))
	for i, chunkID := range chunkMap.ChunkIds {
		if chunkID == "" {
			continue
		}
		chunk, err := reader.GetObject(ctx, backup.ChunkKey(chunkID))
		require.NoError(t, err, "published map references missing chunk %s", chunkID)
		var compression string
		if value := chunk.Metadata["Compression"]; value != nil {
			compression = *value
		}
		buffer := restored[i*backupChunkSize : (i+1)*backupChunkSize]
		require.NoError(t, compressor.Decompress(compression, chunk.Data, buffer,
			snapshot_metrics.New(metrics.NewEmptyRegistry(), "backup-test")))
		checksum := chunk.Metadata["Checksum"]
		require.NotNil(t, checksum)
		value, err := strconv.ParseUint(*checksum, 10, 32)
		require.NoError(t, err)
		require.EqualValues(t, value, crc32.ChecksumIEEE(buffer), "chunk %s", chunkID)
	}
	require.True(t, bytes.Equal(expected, restored), "backup %s does not match its source bytes", snapshotID)
	return chunkMap.ChunkIds
}
