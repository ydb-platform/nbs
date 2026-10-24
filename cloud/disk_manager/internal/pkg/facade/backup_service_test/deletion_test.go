package tests

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	dm "github.com/ydb-platform/nbs/cloud/disk_manager/api"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/backup"
)

func TestBackupDeleteAndCancelDuringCopy(t *testing.T) {
	f := newFixture(t, dm.DiskKind_DISK_KIND_SSD)
	f.calibrate()
	var deleteT0 time.Duration
	survivorSource, survivor := f.id("survivor-source"), f.id("survivor")
	f.empty(survivorSource, f.size)
	f.fill(survivorSource, 1)
	op, err := f.createSnapshot(survivor, survivorSource)
	f.success(op, err, f.window(f.createT0))
	f.backupReady(backup.SnapshotMetaKey(survivorSource, survivor), survivor, 1, f.window(f.backupT0))
	for trial := 0; trial < 3; trial++ {
		id := f.id("delete-control")
		op, err = f.createSnapshot(id, survivorSource)
		f.success(op, err, f.window(f.createT0))
		f.backupReady(backup.SnapshotMetaKey(survivorSource, id), id, 1, f.window(f.backupT0))
		start := time.Now()
		op, err = f.dm.DeleteSnapshot(f.req(), &dm.DeleteSnapshotRequest{SnapshotId: id})
		f.success(op, err, 5*time.Minute)
		deleteT0 = maximum(deleteT0, time.Since(start))
	}
	t.Logf("DELETE_SNAPSHOT_CALIBRATION R=%d T0=%v P=%v D=%v W=%v", f.size, deleteT0, f.period, maximum(60*time.Second, 3*f.period), f.window(deleteT0))
	source, id := f.id("source"), f.id("deleting")
	f.empty(source, f.size)
	f.fill(source, 1)
	follower := faultRule{ID: f.id("copy-barrier"), Route: "backup", Method: "PUT", Contains: "." + id + ".", Mode: "gate"}
	deleting := faultRule{ID: f.id("delete-barrier"), Route: "nbs", Method: "DeleteCheckpoint", DiskID: source, Mode: "gate"}
	f.rules(follower, deleting)
	created, err := f.createSnapshot(id, source)
	f.success(created, err, f.window(f.createT0))
	f.hit(follower.ID, f.window(f.backupT0))
	f.incomplete(source, id)
	// The public Create is already done while background backup is still running.
	// Cancel cannot undo its successful result or act as an undocumented Backup API.
	cancelled, cancelErr := f.dm.CancelOperation(f.req(), &dm.CancelOperationRequest{OperationId: created.Id})
	require.NoError(t, cancelErr)
	require.True(t, cancelled.Done)
	require.Nil(t, cancelled.GetError())
	f.deleteDisk(f.restoreSnapshot(id, f.size, 1))
	removed, err := f.dm.DeleteSnapshot(f.req(), &dm.DeleteSnapshotRequest{SnapshotId: id})
	require.NoError(t, err)
	f.hit(deleting.ID, f.window(deleteT0))
	current, err := f.dm.GetOperation(f.ctx, &dm.GetOperationRequest{OperationId: removed.Id})
	require.NoError(t, err)
	require.False(t, current.Done)
	// Snapshot.Delete is explicitly scheduled as non-cancellable.
	_, err = f.dm.CancelOperation(f.req(), &dm.CancelOperationRequest{OperationId: removed.Id})
	require.ErrorContains(t, err, "non-cancellable")
	time.Sleep(maximum(60*time.Second, 3*f.period))
	f.rules()
	_, err = f.wait(removed, f.window(deleteT0))
	require.NoError(t, err)
	// Source/primary loss after deletion may leave an incomplete follower object.
	// Such remnants must not be accepted as a restorable image.
	reader, readErr := openBackup(f.ctx, f.backup, backup.SnapshotMetaKey(source, id), id)
	if readErr == nil {
		complete := true
		for index := range reader.ids {
			data, err := reader.chunk(f.ctx, index)
			if err != nil {
				complete = false
				break
			}
			equalChunk(t, pattern(1, index), data, uint64(index*chunkSize))
		}
		t.Logf("DELETION_BACKUP_OBSERVATION metadata=true full_copy=%v; partial objects are not restore evidence", complete)
	} else {
		t.Logf("DELETION_BACKUP_OBSERVATION no acceptable metadata/map: %v", readErr)
	}
	f.deleteDisk(source)
	f.deleteDisk(survivorSource)
	f.copies(survivor, survivorSource, 1, f.size)
}
