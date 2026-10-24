package tests

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	dm "github.com/ydb-platform/nbs/cloud/disk_manager/api"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/backup"
)

func (f *fixture) deleteDisk(id string) {
	op, err := f.dm.DeleteDisk(f.req(), &dm.DeleteDiskRequest{DiskId: diskID(id)})
	f.success(op, err, f.window(f.deleteT0))
}
func (f *fixture) copies(id, source string, generation int, size uint64) {
	primary := f.restoreSnapshot(id, size, generation)
	f.deleteDisk(primary)
	// The source disk is never read by this adapter, and all primary S3
	// operations are rejected while it reads and reconstructs the copy.
	f.rules(faultRule{ID: f.id("primary-off"), Route: "primary", Mode: "error"})
	reader := f.backupReady(backup.SnapshotMetaKey(source, id), id, generation, f.window(f.backupT0))
	restored := f.restoreBackup(reader, generation)
	f.deleteDisk(restored)
	f.rules()
}
func testBackupRestore(t *testing.T, kind dm.DiskKind) {
	f := newFixture(t, kind)
	source := f.id("disk")
	f.empty(source, f.size)
	snapshots := []string{}
	maps := [][]string{}
	for generation := 0; generation < 3; generation++ {
		f.fillChanged(source, generation-1, generation)
		id := f.id("snapshot")
		op, err := f.createSnapshot(id, source)
		f.success(op, err, 5*time.Minute)
		reader := f.backupReady(backup.SnapshotMetaKey(source, id), id, generation, 5*time.Minute)
		require.Equal(t, f.size, reader.size)
		snapshots = append(snapshots, id)
		maps = append(maps, reader.ids)
	}
	require.NotEmpty(t, maps[1][0])
	if kind == dm.DiskKind_DISK_KIND_SSD {
		require.Equal(t, maps[1][0], maps[2][0], "unchanged chunk must be inherited")
	} else {
		// DR-based disks intentionally use full snapshots (lockBaseSnapshot).
		require.NotEqual(t, maps[1][0], maps[2][0], "NRD must use independent full-copy chunks")
	}
	require.NotEqual(t, maps[1][1], maps[2][1], "overwritten chunk must change")
	require.Empty(t, maps[2][3], "zeroed chunk must stay zero")
	f.deleteDisk(source)
	for generation, id := range snapshots {
		f.copies(id, source, generation, f.size)
	}
	op, err := f.dm.DeleteSnapshot(f.req(), &dm.DeleteSnapshotRequest{SnapshotId: snapshots[1]})
	f.success(op, err, 5*time.Minute)
	// Deleting the base must preserve both the descendant and its inherited
	// backup chunks; this is checked after deletion completes.
	f.copies(snapshots[2], source, 2, f.size)
}
func TestBackupRestoreSSD(t *testing.T) { testBackupRestore(t, dm.DiskKind_DISK_KIND_SSD) }
func TestBackupRestoreNRD(t *testing.T) {
	testBackupRestore(t, dm.DiskKind_DISK_KIND_SSD_NONREPLICATED)
}

func (f *fixture) incomplete(source, id string) {
	reader, err := openBackup(f.ctx, f.backup, backup.SnapshotMetaKey(source, id), id)
	if err != nil {
		return
	}
	for i := range reader.ids {
		if _, err = reader.chunk(f.ctx, i); err != nil {
			return
		}
	}
	f.t.Fatalf("partial/injected backup %s was accepted as complete", id)
}
func (f *fixture) holdFault(source, id string, op *dm.Operation, backupFault bool) {
	deadline := time.Now().Add(maximum(60*time.Second, 3*f.period))
	for time.Now().Before(deadline) {
		current, err := f.dm.GetOperation(f.ctx, &dm.GetOperationRequest{OperationId: op.Id})
		require.NoError(f.t, err)
		if backupFault {
			require.Nil(f.t, current.GetError())
		} else {
			require.False(f.t, current.Done, "false completion during source fault")
		}
		f.incomplete(source, id)
		time.Sleep(250 * time.Millisecond)
	}
}
func (f *fixture) temporaryFailure(route, method, contains, mode string) {
	source, id := f.id("fault-disk"), f.id("fault-snapshot")
	f.empty(source, f.size)
	f.fill(source, 1)
	contains = strings.ReplaceAll(contains, "%s", id)
	rule := faultRule{ID: f.id("rule"), Route: route, Method: method, Contains: contains, Mode: mode}
	f.rules(rule)
	op, err := f.createSnapshot(id, source)
	require.NoError(f.t, err)
	f.hit(rule.ID, f.window(f.createT0)+f.window(f.backupT0))
	follower := route == "backup" || route == "primary" && method == "GET"
	if route == "backup" {
		_, err = f.wait(op, f.window(f.createT0))
		require.NoError(f.t, err)
		// A failed follower must not roll back the ready primary snapshot.
		restored := f.restoreSnapshot(id, f.size, 1)
		f.deleteDisk(restored)
	}
	if mode == "lost-reply" {
		// A successfully persisted map may already be readable before its reply.
		f.backupReady(backup.SnapshotMetaKey(source, id), id, 1, f.window(f.backupT0))
		time.Sleep(maximum(60*time.Second, 3*f.period))
	} else {
		f.holdFault(source, id, op, follower)
	}
	f.rules()
	_, err = f.wait(op, f.window(f.createT0))
	require.NoError(f.t, err)
	f.backupReady(backup.SnapshotMetaKey(source, id), id, 1, f.window(f.backupT0))
	f.deleteDisk(source)
	f.copies(id, source, 1, f.size)
}
func TestBackupTemporaryFailuresSSD(t *testing.T) {
	f := newFixture(t, dm.DiskKind_DISK_KIND_SSD)
	f.calibrate()
	// Each injection has its own observed intersection and fixed D/W.
	for _, c := range []struct{ name, route, method, contains, mode string }{
		{"checkpoint", "nbs", "CreateCheckpoint", "", "gate"},
		{"read", "nbs", "ReadBlocks", "", "gate"},
		{"primary_write", "primary", "PUT", "snapshot/chunks", "error"},
		{"backup_meta", "backup", "PUT", "/%s/meta.json", "error"},
		{"source_chunk_read", "primary", "GET", "snapshot/chunks", "error"},
		{"backup_chunk", "backup", "PUT", ".%s.", "error"},
		{"backup_map", "backup", "PUT", "/chunk_maps/%s", "error"},
		{"backup_lost_reply", "backup", "PUT", "/chunk_maps/%s", "lost-reply"},
	} {
		t.Run(c.name, func(t *testing.T) {
			previous := f.t
			f.t = t
			defer func() { f.t = previous; f.rules() }()
			f.temporaryFailure(c.route, c.method, c.contains, c.mode)
		})
	}
}
