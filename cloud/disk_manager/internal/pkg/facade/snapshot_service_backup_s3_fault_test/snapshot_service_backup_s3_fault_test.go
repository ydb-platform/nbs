package tests

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/golang/protobuf/ptypes/empty"
	"github.com/stretchr/testify/require"
	disk_manager "github.com/ydb-platform/nbs/cloud/disk_manager/api"
	internal_client "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/client"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/backup"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/facade/testcommon"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/monitoring/metrics"
	"github.com/ydb-platform/nbs/cloud/tasks/persistence"
)

type backupFaultStatus struct {
	Active           bool `json:"active"`
	Hits             int  `json:"hits"`
	UpstreamAccepted int  `json:"upstream_accepted"`
}

func backupFaultControl(t *testing.T, method, path string, body []byte) backupFaultStatus {
	t.Helper()
	port := os.Getenv("DISK_MANAGER_RECIPE_BACKUP_S3_CONTROL_PORT")
	require.NotEmpty(t, port, "the suite requires recipe --backup-fault-proxy")
	request, err := http.NewRequest(method, "http://127.0.0.1:"+port+path, bytes.NewReader(body))
	require.NoError(t, err)
	request.Header.Set("Content-Type", "application/json")
	response, err := (&http.Client{Timeout: 2 * time.Second}).Do(request)
	require.NoError(t, err)
	defer response.Body.Close()
	require.True(t, response.StatusCode >= 200 && response.StatusCode < 300,
		"proxy control %s %s returned %d", method, path, response.StatusCode)
	var status backupFaultStatus
	if method == http.MethodGet {
		require.NoError(t, json.NewDecoder(io.LimitReader(response.Body, 4096)).Decode(&status))
	}
	return status
}

// The fault affects only backup PUTs. Real primary Read/Write/CreateSnapshot
// must complete while the fault is active; then the real scheduler must catch
// up without manually scheduling, requeueing or modifying YDB task state.
func TestSnapshotServicePrimaryOperationsDuringBackupS3Outage(t *testing.T) {
	testPrimaryOperationsDuringBackupS3Fault(t, "fail503")
}

func TestSnapshotServicePrimaryOperationsDuringBackupS3HeldWrite(t *testing.T) {
	testPrimaryOperationsDuringBackupS3Fault(t, "hold-before-write")
}

func testPrimaryOperationsDuringBackupS3Fault(t *testing.T, mode string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(testcommon.NewContext(), 5*time.Minute)
	defer cancel()
	client, err := testcommon.NewClient(ctx)
	require.NoError(t, err)
	defer client.Close()

	port := os.Getenv("DISK_MANAGER_RECIPE_BACKUP_S3_READ_PORT")
	require.NotEmpty(t, port)
	s3, err := persistence.NewS3Client("http://localhost:"+port, "test",
		persistence.S3Credentials{ID: "test", Secret: "test"},
		2*time.Second, metrics.NewEmptyRegistry(), 0, nil, nil)
	require.NoError(t, err)
	exists, err := s3.BucketExists(ctx, "snapshot-backup")
	require.NoError(t, err)
	if !exists {
		require.NoError(t, s3.CreateBucket(ctx, "snapshot-backup"))
	}
	reader, err := backup.NewS3(s3, "snapshot-backup", "recipe", "", nil)
	require.NoError(t, err)
	diskID := t.Name()
	const size = 2 * backupChunkSize
	operation, err := client.CreateDisk(testcommon.GetRequestContext(t, ctx), &disk_manager.CreateDiskRequest{
		Src:    &disk_manager.CreateDiskRequest_SrcEmpty{SrcEmpty: &empty.Empty{}},
		DiskId: &disk_manager.DiskId{DiskId: diskID, ZoneId: "zone-a"},
		Size:   size, BlockSize: 4096, Kind: disk_manager.DiskKind_DISK_KIND_SSD,
	})
	require.NoError(t, err)
	require.NoError(t, internal_client.WaitOperation(ctx, client, operation.Id))
	nbs := testcommon.NewNbsTestingClient(t, ctx, "zone-a")
	expected := bytes.Repeat([]byte{0x63}, size)
	session, err := nbs.MountRW(ctx, diskID, 0, 0, nil)
	require.NoError(t, err)
	for offset := 0; offset < size; offset += backupChunkSize {
		require.NoError(t, session.Write(ctx, uint64(offset/4096), expected[offset:offset+backupChunkSize]))
	}
	session.Close(ctx)

	t.Cleanup(func() { backupFaultControl(t, http.MethodPost, "/reset", nil) })
	fault, err := json.Marshal(map[string]interface{}{
		"mode": mode, "method": "PUT",
		"path_prefix": "/snapshot-backup/recipe/chunks/", "ttl_seconds": 180,
	})
	require.NoError(t, err)
	backupFaultControl(t, http.MethodPost, "/fault", fault)
	snapshotID := t.Name() + "-before-write"
	primaryCtx, primaryCancel := context.WithTimeout(ctx, 45*time.Second)
	operation, err = client.CreateSnapshot(testcommon.GetRequestContext(t, primaryCtx), &disk_manager.CreateSnapshotRequest{
		Src:        &disk_manager.DiskId{DiskId: diskID, ZoneId: "zone-a"},
		SnapshotId: snapshotID, FolderId: "folder",
	})
	require.NoError(t, err)
	require.NoError(t, internal_client.WaitOperation(primaryCtx, client, operation.Id))
	primaryCancel()
	require.Eventually(t, func() bool {
		status := backupFaultControl(t, http.MethodGet, "/status", nil)
		return status.Active && status.Hits > 0
	}, 30*time.Second, 100*time.Millisecond, "the dataplane must reach a backup chunk PUT under the injected fault")

	// Primary data path stays usable even while background backups cannot write.
	primaryCtx, primaryCancel = context.WithTimeout(ctx, 30*time.Second)
	session, err = nbs.MountRW(primaryCtx, diskID, 0, 0, nil)
	require.NoError(t, err)
	changedBlock := bytes.Repeat([]byte{0x29}, 4096)
	require.NoError(t, session.Write(primaryCtx, 0, changedBlock))
	readback := make([]byte, 4096)
	var zero bool
	require.NoError(t, session.Read(primaryCtx, 0, 1, "", readback, &zero))
	require.Equal(t, changedBlock, readback)
	session.Close(primaryCtx)
	primaryCancel()

	// Start another API snapshot only after observing the blocked/failing
	// chunk PUT, so CP availability is checked during dataplane backup work.
	secondExpected := bytes.Clone(expected)
	copy(secondExpected, changedBlock)
	secondSnapshotID := t.Name() + "-during-fault"
	primaryCtx, primaryCancel = context.WithTimeout(ctx, 45*time.Second)
	operation, err = client.CreateSnapshot(testcommon.GetRequestContext(t, primaryCtx), &disk_manager.CreateSnapshotRequest{
		Src:        &disk_manager.DiskId{DiskId: diskID, ZoneId: "zone-a"},
		SnapshotId: secondSnapshotID, FolderId: "folder",
	})
	require.NoError(t, err)
	require.NoError(t, internal_client.WaitOperation(primaryCtx, client, operation.Id))
	primaryCancel()

	status := backupFaultControl(t, http.MethodGet, "/status", nil)
	require.True(t, status.Active, "TTL expiry must not silently turn this into a healthy-S3 test")
	require.Positive(t, status.Hits)
	require.Zero(t, status.UpstreamAccepted, "no backup chunk PUT may reach S3 before the fault is reset")
	_, err = reader.GetObject(ctx, backup.ChunkMapKey(snapshotID))
	require.ErrorContains(t, err, "s3 object not found", "an incomplete backup must not publish its final chunk map")
	backupFaultControl(t, http.MethodPost, "/reset", nil)

	waitForBackupMap(t, ctx, reader, snapshotID)
	// The backup must contain bytes from snapshot creation, not the later write.
	requireBackupContent(t, ctx, reader, diskID, snapshotID, expected)
	waitForBackupMap(t, ctx, reader, secondSnapshotID)
	requireBackupContent(t, ctx, reader, diskID, secondSnapshotID, secondExpected)
	for _, id := range []string{snapshotID, secondSnapshotID} {
		operation, err = client.DeleteSnapshot(testcommon.GetRequestContext(t, ctx), &disk_manager.DeleteSnapshotRequest{SnapshotId: id})
		require.NoError(t, err)
		require.NoError(t, internal_client.WaitOperation(ctx, client, operation.Id))
	}
	testcommon.DeleteDisk(t, ctx, client, diskID)
}
