package tests

import (
	"github.com/stretchr/testify/require"
	dm "github.com/ydb-platform/nbs/cloud/disk_manager/api"
	"os"
	"testing"
	"time"
)

func TestBackupDisabledPublicRoundtrip(t *testing.T) {
	require.Empty(t, os.Getenv("DISK_MANAGER_BACKUP_S3_PORT"))
	f := newFixture(t, dm.DiskKind_DISK_KIND_SSD)
	source, id := f.id("disk"), f.id("snapshot")
	f.empty(source, f.size)
	f.fill(source, 1)
	op, err := f.createSnapshot(id, source)
	f.success(op, err, 5*time.Minute)
	op, err = f.dm.DeleteDisk(f.req(), &dm.DeleteDiskRequest{DiskId: diskID(source)})
	f.success(op, err, 5*time.Minute)
	restored := f.restoreSnapshot(id, f.size, 1)
	op, err = f.dm.DeleteDisk(f.req(), &dm.DeleteDiskRequest{DiskId: diskID(restored)})
	f.success(op, err, 5*time.Minute)
}
