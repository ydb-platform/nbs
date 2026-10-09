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
	tasks_storage "github.com/ydb-platform/nbs/cloud/tasks/storage"
)

type exhaustedBackupFaultStatus struct {
	Active bool `json:"active"`
	Hits   int  `json:"hits"`
}

func exhaustionFaultControl(t *testing.T, ctx context.Context, method, path string, body []byte) exhaustedBackupFaultStatus {
	t.Helper()
	port := os.Getenv("DISK_MANAGER_RECIPE_BACKUP_S3_CONTROL_PORT")
	require.NotEmpty(t, port, "the suite requires recipe --backup-fault-proxy")
	request, err := http.NewRequestWithContext(ctx, method, "http://127.0.0.1:"+port+path, bytes.NewReader(body))
	require.NoError(t, err)
	request.Header.Set("Content-Type", "application/json")
	response, err := (&http.Client{Timeout: 2 * time.Second}).Do(request)
	require.NoError(t, err)
	defer response.Body.Close()
	require.True(t, response.StatusCode >= 200 && response.StatusCode < 300,
		"proxy control %s %s returned %d", method, path, response.StatusCode)
	var status exhaustedBackupFaultStatus
	if method == http.MethodGet {
		require.NoError(t, json.NewDecoder(io.LimitReader(response.Body, 4096)).Decode(&status))
	}
	return status
}

// A short S3 outage and a terminal task failure are different cases. This
// regression waits for real persisted cancellation after the configured retry
// limit, restores S3, and requires automatic recovery without rescheduling tasks
// by hand or editing any queue. It is expected to fail on f32903815a.
func TestSnapshotServiceAutomaticBackupAfterRetryExhaustion(t *testing.T) {
	ctx, cancel := context.WithTimeout(testcommon.NewContext(), 8*time.Minute)
	defer cancel()
	require.Equal(t, "3", os.Getenv("DISK_MANAGER_RECIPE_BACKUP_TASK_MAX_RETRIABLE_ERRORS"),
		"this regression requires recipe --backup-task-max-retriable-errors 3")
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

	expected := bytes.Repeat([]byte{0x65}, backupChunkSize)
	createSnapshot := func(diskID, snapshotID string) {
		operation, err := client.CreateDisk(testcommon.GetRequestContext(t, ctx), &disk_manager.CreateDiskRequest{
			Src:    &disk_manager.CreateDiskRequest_SrcEmpty{SrcEmpty: &empty.Empty{}},
			DiskId: &disk_manager.DiskId{DiskId: diskID, ZoneId: "zone-a"},
			Size:   int64(len(expected)), BlockSize: 4096, Kind: disk_manager.DiskKind_DISK_KIND_SSD,
		})
		require.NoError(t, err)
		require.NoError(t, internal_client.WaitOperation(ctx, client, operation.Id))
		nbs := testcommon.NewNbsTestingClient(t, ctx, "zone-a")
		session, err := nbs.MountRW(ctx, diskID, 0, 0, nil)
		require.NoError(t, err)
		require.NoError(t, session.Write(ctx, 0, expected))
		session.Close(ctx)
		operation, err = client.CreateSnapshot(testcommon.GetRequestContext(t, ctx), &disk_manager.CreateSnapshotRequest{
			Src:        &disk_manager.DiskId{DiskId: diskID, ZoneId: "zone-a"},
			SnapshotId: snapshotID, FolderId: "folder",
		})
		require.NoError(t, err)
		require.NoError(t, internal_client.WaitOperation(ctx, client, operation.Id))
	}

	diskID, snapshotID := t.Name()+"-disk", t.Name()+"-snapshot"
	// Target only this snapshot's CP metadata PUT. This fails before a
	// dataplane child is scheduled and avoids depending on other regressions.
	fault, err := json.Marshal(map[string]interface{}{
		"mode": "fail503", "method": "PUT", "ttl_seconds": 180,
		"path_prefix": "/snapshot-backup/recipe/" + backup.SnapshotMetaKey(diskID, snapshotID),
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cleanupCancel()
		exhaustionFaultControl(t, cleanupCtx, http.MethodPost, "/reset", nil)
	})
	exhaustionFaultControl(t, ctx, http.MethodPost, "/fault", fault)
	createSnapshot(diskID, snapshotID)

	taskStorage := testcommon.NewTaskStorage(t, ctx)
	var terminal tasks_storage.TaskState
	var taskReadErr error
	require.Eventually(t, func() bool {
		pollCtx, pollCancel := context.WithTimeout(ctx, 5*time.Second)
		defer pollCancel()
		terminal, taskReadErr = taskStorage.GetTaskByIdempotencyKey(pollCtx, "backup_snapshot_"+snapshotID, "")
		return taskReadErr == nil && terminal.Status == tasks_storage.TaskStatusCancelled
	}, 60*time.Second, 100*time.Millisecond, "backup must reach persisted terminal cancellation, not just encounter an S3 error")
	require.NoError(t, taskReadErr)
	require.Equal(t, "snapshots.BackupSnapshot", terminal.TaskType)
	require.EqualValues(t, 3, terminal.RetriableErrorCount, "recipe must apply the task retry limit, not an SDK retry limit")
	require.Contains(t, terminal.ErrorMessage, "ServiceUnavailable")
	status := exhaustionFaultControl(t, ctx, http.MethodGet, "/status", nil)
	require.True(t, status.Active, "fault TTL must not expire before cancellation is observed")
	require.GreaterOrEqual(t, status.Hits, 4, "initial attempt plus three retries must reach the proxy")
	_, err = reader.GetObject(ctx, backup.ChunkMapKey(snapshotID))
	require.ErrorContains(t, err, "s3 object not found")

	resourceStorage, closeStorage := testcommon.NewResourceStorage(t, ctx)
	defer closeStorage()
	queued, err := resourceStorage.ListSnapshotsToBackup(ctx, 100)
	require.NoError(t, err)
	t.Logf("Terminal backup: task=%s retries=%d error=%q; pending snapshots=%v",
		terminal.ID, terminal.RetriableErrorCount, terminal.ErrorMessage, queued)

	exhaustionFaultControl(t, ctx, http.MethodPost, "/reset", nil)
	require.False(t, exhaustionFaultControl(t, ctx, http.MethodGet, "/status", nil).Active)
	// A new, unrelated disk proves that S3 and the regular production backup
	// scheduler work after recovery. It shares no chunks with the failed backup.
	healthyDiskID, healthySnapshotID := t.Name()+"-healthy-disk", t.Name()+"-healthy-snapshot"
	createSnapshot(healthyDiskID, healthySnapshotID)
	waitForBackupMap(t, ctx, reader, healthySnapshotID)
	requireBackupContent(t, ctx, reader, healthyDiskID, healthySnapshotID, expected)

	// Strict acceptance criterion: the original snapshot must not be silently
	// abandoned once the dependency recovers. No manual queue/task writes here.
	waitForBackupMap(t, ctx, reader, snapshotID)
	requireBackupContent(t, ctx, reader, diskID, snapshotID, expected)
	for _, id := range []string{snapshotID, healthySnapshotID} {
		operation, err := client.DeleteSnapshot(testcommon.GetRequestContext(t, ctx), &disk_manager.DeleteSnapshotRequest{SnapshotId: id})
		require.NoError(t, err)
		require.NoError(t, internal_client.WaitOperation(ctx, client, operation.Id))
	}
	testcommon.DeleteDisk(t, ctx, client, diskID)
	testcommon.DeleteDisk(t, ctx, client, healthyDiskID)
}
