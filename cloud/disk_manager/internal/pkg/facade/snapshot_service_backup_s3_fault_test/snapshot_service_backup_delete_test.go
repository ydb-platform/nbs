package tests

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
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

func TestSnapshotServiceDeleteDuringBlockedBackupChunkPUT(t *testing.T) {
	testDeleteDuringBackupChunkPUT(t, "hold-before-write")
}

func TestSnapshotServiceDeleteAfterAcceptedBackupChunkPUT(t *testing.T) {
	testDeleteDuringBackupChunkPUT(t, "hold-after-success")
}

// On f32903815a BackupSnapshotData does not take LockSnapshot (see its Cancel
// TODO). DeleteSnapshot can therefore finish while backup is unfinished. The
// existing storage lock contract is also explicit: DeletingSnapshot interrupts
// while another task owns LockTaskID. Do not invent an unconditional promise
// that deletion waits for backup. A race may leave no completed backup, but any
// published final chunk map must describe the complete original snapshot.
// This case does not assert orphan-object/backup-queue garbage collection.
func testDeleteDuringBackupChunkPUT(t *testing.T, mode string) {
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
	expected := bytes.Repeat([]byte{0x47}, backupChunkSize)
	nbs := testcommon.NewNbsTestingClient(t, ctx, "zone-a")
	createSnapshot := func(diskID, snapshotID string) {
		operation, err := client.CreateDisk(testcommon.GetRequestContext(t, ctx), &disk_manager.CreateDiskRequest{
			Src:    &disk_manager.CreateDiskRequest_SrcEmpty{SrcEmpty: &empty.Empty{}},
			DiskId: &disk_manager.DiskId{DiskId: diskID, ZoneId: "zone-a"},
			Size:   int64(len(expected)), BlockSize: 4096, Kind: disk_manager.DiskKind_DISK_KIND_SSD,
		})
		require.NoError(t, err)
		require.NoError(t, internal_client.WaitOperation(ctx, client, operation.Id))
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

	fault, err := json.Marshal(map[string]interface{}{
		"mode": mode, "method": "PUT", "ttl_seconds": 180,
		"path_prefix": "/snapshot-backup/recipe/chunks/",
	})
	require.NoError(t, err)
	t.Cleanup(func() { backupFaultControl(t, http.MethodPost, "/reset", nil) })
	backupFaultControl(t, http.MethodPost, "/fault", fault)
	diskID, snapshotID := t.Name()+"-disk", t.Name()+"-snapshot"
	createSnapshot(diskID, snapshotID)
	require.Eventually(t, func() bool {
		status := backupFaultControl(t, http.MethodGet, "/status", nil)
		if !status.Active || status.Hits == 0 {
			return false
		}
		return mode != "hold-after-success" || status.UpstreamAccepted > 0
	}, 30*time.Second, 100*time.Millisecond, "DeleteSnapshot must race with an observed chunk PUT, not just a queued backup")
	_, err = reader.GetObject(ctx, backup.ChunkMapKey(snapshotID))
	require.ErrorContains(t, err, "s3 object not found", "the held backup must not already be complete")

	// Read-only task inspection proves that both real CP and DP backup tasks
	// exist. It neither creates tasks nor edits their persistent state.
	taskStorage := testcommon.NewTaskStorage(t, ctx)
	cpBackup, err := taskStorage.GetTaskByIdempotencyKey(ctx, "backup_snapshot_"+snapshotID, "")
	require.NoError(t, err)
	require.Equal(t, "snapshots.BackupSnapshot", cpBackup.TaskType)
	dpBackup, err := taskStorage.GetTaskByIdempotencyKey(ctx, fmt.Sprintf("%s_%s_backup", cpBackup.ID, snapshotID), "")
	require.NoError(t, err)
	require.Equal(t, "dataplane.BackupSnapshotData", dpBackup.TaskType)
	meta, err := testcommon.GetSnapshotMeta(t, ctx, snapshotID)
	require.NoError(t, err)
	require.NotNil(t, meta)
	require.True(t, meta.Ready)
	backupOwnsLock := meta.LockTaskID != ""
	if backupOwnsLock {
		require.Equal(t, dpBackup.ID, meta.LockTaskID, "an unrelated lock must not explain the observed deletion wait")
	}
	deleteOperation, err := client.DeleteSnapshot(testcommon.GetRequestContext(t, ctx), &disk_manager.DeleteSnapshotRequest{SnapshotId: snapshotID})
	require.NoError(t, err)
	if backupOwnsLock {
		// Prove that deletion has reached its real DP task while backup owns
		// the lock; an arbitrary sleep or a merely submitted API call cannot.
		require.Eventually(t, func() bool {
			deletion, err := taskStorage.GetTaskByIdempotencyKey(ctx, deleteOperation.Id, "")
			return err == nil && deletion.TaskType == "dataplane.DeleteSnapshot" && deletion.LastHost != ""
		}, 30*time.Second, 100*time.Millisecond)
		operation, err := client.GetOperation(ctx, &disk_manager.GetOperationRequest{OperationId: deleteOperation.Id})
		require.NoError(t, err)
		require.False(t, operation.Done, "deletion must wait while the backup task owns the snapshot lock")
		meta, err = testcommon.GetSnapshotMeta(t, ctx, snapshotID)
		require.NoError(t, err)
		require.NotNil(t, meta)
		require.True(t, meta.Ready)
		require.Equal(t, dpBackup.ID, meta.LockTaskID)
	} else {
		deleteCtx, deleteCancel := context.WithTimeout(ctx, 45*time.Second)
		err = internal_client.WaitOperation(deleteCtx, client, deleteOperation.Id)
		deleteCancel()
		require.NoError(t, err, "without a snapshot lock deletion must not stall behind a failed backup")
	}
	status := backupFaultControl(t, http.MethodGet, "/status", nil)
	require.True(t, status.Active, "fault TTL must not expire before the deletion boundary is observed")
	require.Positive(t, status.Hits)
	if mode == "hold-before-write" {
		require.Zero(t, status.UpstreamAccepted)
	} else {
		require.Positive(t, status.UpstreamAccepted)
	}
	backupFaultControl(t, http.MethodPost, "/reset", nil)
	require.NoError(t, internal_client.WaitOperation(ctx, client, deleteOperation.Id))

	// Wait for both tasks to end so an absent map is not mistaken for a
	// backup that is still running and could publish a broken map later.
	var terminalDP tasks_storage.TaskState
	require.Eventually(t, func() bool {
		pollCtx, pollCancel := context.WithTimeout(ctx, 5*time.Second)
		defer pollCancel()
		terminalCP, cpErr := taskStorage.GetTask(pollCtx, cpBackup.ID)
		var dpErr error
		terminalDP, dpErr = taskStorage.GetTask(pollCtx, dpBackup.ID)
		return cpErr == nil && dpErr == nil && tasks_storage.IsEnded(terminalCP.Status) && tasks_storage.IsEnded(terminalDP.Status)
	}, time.Minute, 100*time.Millisecond, "deletion must leave no running CP/DP backup task for the deleted snapshot")
	checkPublishedBackup := func() {
		_, err := reader.GetObject(ctx, backup.ChunkMapKey(snapshotID))
		if err != nil {
			require.ErrorContains(t, err, "s3 object not found")
			require.Equal(t, tasks_storage.TaskStatusCancelled, terminalDP.Status,
				"a successfully finished DP backup must have a complete final map")
			return
		}
		requireBackupContent(t, ctx, reader, diskID, snapshotID, expected)
	}
	checkPublishedBackup()

	// A fresh disk avoids incremental links to the deleted snapshot. Its
	// successful backup proves that normal scheduler work resumes after reset.
	healthyDiskID, healthySnapshotID := t.Name()+"-healthy-disk", t.Name()+"-healthy-snapshot"
	createSnapshot(healthyDiskID, healthySnapshotID)
	waitForBackupMap(t, ctx, reader, healthySnapshotID)
	requireBackupContent(t, ctx, reader, healthyDiskID, healthySnapshotID, expected)
	checkPublishedBackup()
	operation, err := client.DeleteSnapshot(testcommon.GetRequestContext(t, ctx), &disk_manager.DeleteSnapshotRequest{SnapshotId: healthySnapshotID})
	require.NoError(t, err)
	require.NoError(t, internal_client.WaitOperation(ctx, client, operation.Id))
	testcommon.DeleteDisk(t, ctx, client, healthyDiskID)
	testcommon.DeleteDisk(t, ctx, client, diskID)
}
