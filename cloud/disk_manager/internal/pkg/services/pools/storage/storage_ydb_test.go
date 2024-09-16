package storage

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/monitoring/metrics"
	pools_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/pools/config"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/types"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/logging"
	"github.com/ydb-platform/nbs/cloud/tasks/persistence"
	persistence_config "github.com/ydb-platform/nbs/cloud/tasks/persistence/config"
)

////////////////////////////////////////////////////////////////////////////////

func makeDefaultConfig() *pools_config.PoolsConfig {
	maxActiveSlots := uint32(10)
	maxBaseDisksInflight := uint32(5)
	maxBaseDiskUnits := uint32(100)

	return &pools_config.PoolsConfig{
		MaxActiveSlots:       &maxActiveSlots,
		MaxBaseDisksInflight: &maxBaseDisksInflight,
		MaxBaseDiskUnits:     &maxBaseDiskUnits,
	}
}

////////////////////////////////////////////////////////////////////////////////

func newContext() context.Context {
	return logging.SetLogger(
		context.Background(),
		logging.NewStderrLogger(logging.DebugLevel),
	)
}

func newYDB(ctx context.Context) (*persistence.YDBClient, error) {
	endpoint := fmt.Sprintf(
		"localhost:%v",
		os.Getenv("DISK_MANAGER_RECIPE_YDB_PORT"),
	)
	database := "/Root"
	rootPath := "disk_manager"

	return persistence.NewYDBClient(
		ctx,
		&persistence_config.PersistenceConfig{
			Endpoint: &endpoint,
			Database: &database,
			RootPath: &rootPath,
		},
		metrics.NewEmptyRegistry(),
	)
}

func newStorageWithConfig(
	t *testing.T,
	ctx context.Context,
	db *persistence.YDBClient,
	config *pools_config.PoolsConfig,
	registry metrics.Registry,
) Storage {

	folder := fmt.Sprintf("storage_ydb_test/%v", t.Name())
	config.StorageFolder = &folder

	err := CreateYDBTables(ctx, config, db, false /* dropUnusedColumns */)
	require.NoError(t, err)

	storage, err := NewStorage(config, db, registry)
	require.NoError(t, err)

	return storage
}

func newStorage(
	t *testing.T,
	ctx context.Context,
	db *persistence.YDBClient,
) Storage {

	return newStorageWithConfig(t, ctx, db, makeDefaultConfig(), metrics.NewEmptyRegistry())
}

////////////////////////////////////////////////////////////////////////////////

func baseDiskShouldBeDeletedSoon(
	t *testing.T,
	ctx context.Context,
	storage Storage,
	baseDisk BaseDisk,
) bool {

	toDelete, err := storage.GetBaseDisksToDelete(ctx, 100500)
	require.NoError(t, err)

	for _, d := range toDelete {
		if d.ID == baseDisk.ID {
			return true
		}
	}

	return false
}

////////////////////////////////////////////////////////////////////////////////

func relocateOverlayDisk(
	ctx context.Context,
	db *persistence.YDBClient,
	storage Storage,
	overlayDisk *types.Disk,
	targetZoneID string,
) (RebaseInfo, error) {

	var rebaseInfo RebaseInfo

	err := db.Execute(
		ctx,
		func(ctx context.Context, session *persistence.Session) error {
			tx, err := session.BeginRWTransaction(ctx)
			if err != nil {
				return err
			}
			defer tx.Rollback(ctx)

			rebaseInfo, err = storage.RelocateOverlayDiskTx(ctx, tx, overlayDisk, targetZoneID)
			if err != nil {
				return err
			}

			return tx.Commit(ctx)
		},
	)

	return rebaseInfo, err
}

////////////////////////////////////////////////////////////////////////////////

func TestStorageYDBReleaseNonExistent(t *testing.T) {
	ctx, cancel := context.WithCancel(newContext())
	defer cancel()

	db, err := newYDB(ctx)
	require.NoError(t, err)
	defer db.Close(ctx)

	storage := newStorage(t, ctx, db)

	slot := Slot{
		OverlayDisk: &types.Disk{
			ZoneId: "zone",
			DiskId: "overlay",
		},
	}

	baseDisk, err := storage.ReleaseBaseDiskSlot(ctx, slot.OverlayDisk)
	require.NoError(t, err)
	require.Empty(t, baseDisk)
	require.False(t, baseDiskShouldBeDeletedSoon(t, ctx, storage, baseDisk))

	err = storage.CheckConsistency(ctx)
	require.NoError(t, err)
}

func TestStorageYDBCreateBaseDisksPool(t *testing.T) {
	ctx, cancel := context.WithCancel(newContext())
	defer cancel()

	db, err := newYDB(ctx)
	require.NoError(t, err)
	defer db.Close(ctx)

	storage := newStorage(t, ctx, db)

	defaultConfig := makeDefaultConfig()
	poolCap := defaultConfig.GetMaxBaseDisksInflight() * defaultConfig.GetMaxActiveSlots()
	enlargedPoolCap := 2*poolCap - 1

	overlayDisks := make([]*types.Disk, 0)
	for i := 0; i < int(enlargedPoolCap); i++ {
		overlayDisks = append(overlayDisks, &types.Disk{
			ZoneId: "zone",
			DiskId: fmt.Sprintf("overlay%v", i),
		})
	}
	baseDisks := make([]BaseDisk, 0)

	// There is no pool configured, no disks to create.
	toSchedule, err := storage.TakeBaseDisksToSchedule(ctx)
	require.NoError(t, err)
	require.Empty(t, toSchedule)

	err = storage.ConfigurePool(ctx, "image", "zone", poolCap, 0)
	require.NoError(t, err)

	toSchedule, err = storage.TakeBaseDisksToSchedule(ctx)
	require.NoError(t, err)
	require.Equal(t, int(defaultConfig.GetMaxBaseDisksInflight()), len(toSchedule))

	seenIDs := make([]string, 0)
	for _, disk := range toSchedule {
		seenIDs = append(seenIDs, disk.ID)
		baseDisks = append(baseDisks, disk)

		require.NotEmpty(t, disk.ID)
		require.Equal(t, "image", disk.ImageID)
		require.Equal(t, "zone", disk.ZoneID)
		// Note: we use image id as checkpoint id.
		require.Equal(t, "image", disk.CheckpointID)
		require.Empty(t, disk.CreateTaskID)
		require.False(t, disk.Ready)
	}

	// Check idempotency.
	toSchedule, err = storage.TakeBaseDisksToSchedule(ctx)
	require.NoError(t, err)
	require.ElementsMatch(t, baseDisks, toSchedule)

	// Base disks creation is not scheduled, so tell client to come back later.
	_, err = storage.AcquireBaseDiskSlot(
		ctx,
		"image",
		Slot{OverlayDisk: overlayDisks[0]},
	)
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.NewInterruptExecutionError()))

	scheduled := baseDisks[0:2]
	for i := 0; i < len(scheduled); i++ {
		scheduled[i].CreateTaskID = "create"
	}

	err = storage.BaseDisksScheduled(ctx, scheduled)
	require.NoError(t, err)

	// First 2 disks are scheduled, but other 3 are not.
	toSchedule, err = storage.TakeBaseDisksToSchedule(ctx)
	require.NoError(t, err)
	require.ElementsMatch(t, baseDisks[2:5], toSchedule)

	// Acquire base disk should be successfull as we have disks that are
	// creating right now.
	baseDisk, err := storage.AcquireBaseDiskSlot(
		ctx,
		"image",
		Slot{OverlayDisk: overlayDisks[0]},
	)
	require.NoError(t, err)
	require.Contains(t, seenIDs, baseDisk.ID)
	require.Equal(t, "image", baseDisk.ImageID)
	require.Equal(t, "zone", baseDisk.ZoneID)
	// Note: we use image id as checkpoint id.
	require.Equal(t, "image", baseDisk.CheckpointID)
	require.Equal(t, "create", baseDisk.CreateTaskID)
	require.False(t, baseDisk.Ready)

	// Check idempotency.
	actual, err := storage.AcquireBaseDiskSlot(
		ctx,
		"image",
		Slot{OverlayDisk: overlayDisks[0]},
	)
	require.NoError(t, err)
	require.Equal(t, baseDisk, actual)

	poolInfos, err := storage.GetReadyPoolInfos(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, len(poolInfos))
	require.Equal(t, "image", poolInfos[0].ImageID)
	require.Equal(t, "zone", poolInfos[0].ZoneID)
	require.Equal(t, uint64(1), poolInfos[0].AcquiredUnits)
	require.Equal(t, uint64(499), poolInfos[0].FreeUnits)
	require.Equal(t, uint32(50), poolInfos[0].Capacity)
	require.Equal(t, uint64(0), poolInfos[0].ImageSize)
	require.NotZero(t, poolInfos[0].CreatedAt)

	for _, disk := range baseDisks[2:5] {
		err = storage.BaseDiskCreated(ctx, disk)
		require.NoError(t, err)
	}

	scheduled = baseDisks[2:5]
	for i := 0; i < len(scheduled); i++ {
		scheduled[i].CreateTaskID = "create"
	}

	err = storage.BaseDisksScheduled(ctx, scheduled)
	require.NoError(t, err)

	// We acquired one slot.
	// New base disk should be created in order to replenish pool.
	toSchedule, err = storage.TakeBaseDisksToSchedule(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, len(toSchedule))
	seenIDs = append(seenIDs, toSchedule[0].ID)
	baseDisks = append(baseDisks, toSchedule...)
	err = storage.BaseDiskCreated(ctx, toSchedule[0])
	require.NoError(t, err)

	for _, disk := range baseDisks {
		err = storage.BaseDiskCreated(ctx, disk)
		require.NoError(t, err)
	}

	// Nothing to create, pool is full.
	toSchedule, err = storage.TakeBaseDisksToSchedule(ctx)
	require.NoError(t, err)
	require.Empty(t, toSchedule)

	// Enlarge pool.
	err = storage.ConfigurePool(ctx, "image", "zone", enlargedPoolCap, 0)
	require.NoError(t, err)

	toSchedule, err = storage.TakeBaseDisksToSchedule(ctx)
	require.NoError(t, err)
	require.Equal(t, int(defaultConfig.GetMaxBaseDisksInflight()-1), len(toSchedule))
	for _, disk := range toSchedule {
		require.NotContains(t, seenIDs, disk.ID)
		seenIDs = append(seenIDs, disk.ID)

		require.NotEmpty(t, disk.ID)
		require.Equal(t, "image", disk.ImageID)
		require.Equal(t, "zone", disk.ZoneID)
		// Note: we use image id as checkpoint id.
		require.Equal(t, "image", disk.CheckpointID)
		require.Empty(t, disk.CreateTaskID)
		require.False(t, disk.Ready)
	}

	for _, disk := range toSchedule {
		err = storage.BaseDiskCreated(ctx, disk)
		require.NoError(t, err)
	}

	// Nothing to create, pool is full.
	toSchedule, err = storage.TakeBaseDisksToSchedule(ctx)
	require.NoError(t, err)
	require.Empty(t, toSchedule)

	// Drain pool.
	for _, overlayDisk := range overlayDisks {
		baseDisk, err := storage.AcquireBaseDiskSlot(
			ctx,
			"image",
			Slot{OverlayDisk: overlayDisk},
		)
		require.NoError(t, err)
		require.Contains(t, seenIDs, baseDisk.ID)

		require.Equal(t, "image", baseDisk.ImageID)
		require.Equal(t, "zone", baseDisk.ZoneID)
		// Note: we use image id as checkpoint id.
		require.Equal(t, "image", baseDisk.CheckpointID)
		require.True(t, baseDisk.Ready)
	}

	// Check that pool is refilling.
	toSchedule, err = storage.TakeBaseDisksToSchedule(ctx)
	require.NoError(t, err)
	require.Equal(t, int(defaultConfig.GetMaxBaseDisksInflight()), len(toSchedule))

	for _, disk := range toSchedule {
		require.NotContains(t, seenIDs, disk.ID)
		seenIDs = append(seenIDs, disk.ID)

		require.NotEmpty(t, disk.ID)
		require.Equal(t, "image", disk.ImageID)
		require.Equal(t, "zone", disk.ZoneID)
		// Note: we use image id as checkpoint id.
		require.Equal(t, "image", disk.CheckpointID)
		require.Empty(t, disk.CreateTaskID)
		require.False(t, disk.Ready)
	}

	err = storage.CheckConsistency(ctx)
	require.NoError(t, err)
}

func TestStorageYDBPoolReusage1(t *testing.T) {
	ctx, cancel := context.WithCancel(newContext())
	defer cancel()

	db, err := newYDB(ctx)
	require.NoError(t, err)
	defer db.Close(ctx)

	storage := newStorage(t, ctx, db)

	err = storage.ConfigurePool(ctx, "image", "zone", 1, 0)
	require.NoError(t, err)

	slot1 := Slot{
		OverlayDisk: &types.Disk{
			ZoneId: "zone",
			DiskId: "overlay1",
		},
	}
	slot2 := Slot{
		OverlayDisk: &types.Disk{
			ZoneId: "zone",
			DiskId: "overlay2",
		},
	}

	baseDisks, err := storage.TakeBaseDisksToSchedule(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, len(baseDisks))

	baseDisks[0].CreateTaskID = "create"
	err = storage.BaseDisksScheduled(ctx, baseDisks)
	require.NoError(t, err)

	actual, err := storage.AcquireBaseDiskSlot(ctx, "image", slot1)
	require.NoError(t, err)
	require.Equal(t, baseDisks[0], actual)

	actual, err = storage.ReleaseBaseDiskSlot(ctx, slot1.OverlayDisk)
	require.NoError(t, err)
	require.Equal(t, baseDisks[0], actual)
	require.False(t, baseDiskShouldBeDeletedSoon(t, ctx, storage, actual))

	// Check idempotency.
	actual, err = storage.ReleaseBaseDiskSlot(ctx, slot1.OverlayDisk)
	require.NoError(t, err)
	require.Equal(t, baseDisks[0], actual)
	require.False(t, baseDiskShouldBeDeletedSoon(t, ctx, storage, actual))

	actual, err = storage.AcquireBaseDiskSlot(ctx, "image", slot2)
	require.NoError(t, err)
	require.Equal(t, baseDisks[0], actual)

	err = storage.CheckConsistency(ctx)
	require.NoError(t, err)
}

func TestStorageYDBPoolReusage2(t *testing.T) {
	ctx, cancel := context.WithCancel(newContext())
	defer cancel()

	db, err := newYDB(ctx)
	require.NoError(t, err)
	defer db.Close(ctx)

	storage := newStorage(t, ctx, db)

	overlayDisks := make([]*types.Disk, 0)
	for i := 0; i < int(makeDefaultConfig().GetMaxActiveSlots()+1); i++ {
		overlayDisks = append(overlayDisks, &types.Disk{
			ZoneId: "zone",
			DiskId: fmt.Sprintf("overlay%v", i),
		})
	}

	err = storage.ConfigurePool(ctx, "image", "zone", 1, 0)
	require.NoError(t, err)

	baseDisks, err := storage.TakeBaseDisksToSchedule(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, len(baseDisks))

	baseDisks[0].CreateTaskID = "create"
	err = storage.BaseDisksScheduled(ctx, baseDisks)
	require.NoError(t, err)

	// First, acquire all slots on base disk.
	for i := 0; i < int(makeDefaultConfig().GetMaxActiveSlots()); i++ {
		actual, err := storage.AcquireBaseDiskSlot(
			ctx,
			"image",
			Slot{OverlayDisk: overlayDisks[i]},
		)
		require.NoError(t, err)
		require.Equal(t, baseDisks[0], actual)
	}

	// Release one slot.
	actual, err := storage.ReleaseBaseDiskSlot(ctx, overlayDisks[0])
	require.NoError(t, err)
	require.Equal(t, baseDisks[0], actual)
	require.False(t, baseDiskShouldBeDeletedSoon(t, ctx, storage, actual))

	// Acquire slot for last overlay disk.
	actual, err = storage.AcquireBaseDiskSlot(
		ctx,
		"image",
		Slot{OverlayDisk: overlayDisks[len(overlayDisks)-1]},
	)
	require.NoError(t, err)
	require.Equal(t, baseDisks[0], actual)

	err = storage.CheckConsistency(ctx)
	require.NoError(t, err)
}

func TestStorageYDBDeletePool(t *testing.T) {
	ctx, cancel := context.WithCancel(newContext())
	defer cancel()

	db, err := newYDB(ctx)
	require.NoError(t, err)
	defer db.Close(ctx)

	storage := newStorage(t, ctx, db)

	// Should be no-op.
	err = storage.DeletePool(ctx, "image", "zone1")
	require.NoError(t, err)

	err = storage.ConfigurePool(ctx, "image", "zone1", 1, 0)
	require.NoError(t, err)

	err = storage.ConfigurePool(ctx, "image", "zone2", 1, 0)
	require.NoError(t, err)

	baseDisks, err := storage.TakeBaseDisksToSchedule(ctx)
	require.NoError(t, err)
	require.Equal(t, 2, len(baseDisks))
	require.NotEqual(t, baseDisks[0].ZoneID, baseDisks[1].ZoneID)

	for i := 0; i < len(baseDisks); i++ {
		baseDisks[i].CreateTaskID = "create"
	}
	err = storage.BaseDisksScheduled(ctx, baseDisks)
	require.NoError(t, err)

	var baseDiskFromZone1 BaseDisk
	if baseDisks[1].ZoneID == "zone1" {
		baseDiskFromZone1 = baseDisks[1]
	} else {
		baseDiskFromZone1 = baseDisks[0]
		require.Equal(t, "zone1", baseDisks[0].ZoneID)
	}

	err = storage.DeletePool(ctx, "image", "zone1")
	require.NoError(t, err)

	configured, err := storage.IsPoolConfigured(ctx, "image", "zone1")
	require.NoError(t, err)
	require.False(t, configured)

	configured, err = storage.IsPoolConfigured(ctx, "image", "zone2")
	require.NoError(t, err)
	require.True(t, configured)

	toDelete, err := storage.GetBaseDisksToDelete(ctx, 100500)
	require.NoError(t, err)
	require.Equal(t, 1, len(toDelete))
	require.Equal(t, baseDiskFromZone1, toDelete[0])

	// Check idempotency.
	err = storage.DeletePool(ctx, "image", "zone1")
	require.NoError(t, err)

	toDelete, err = storage.GetBaseDisksToDelete(ctx, 100500)
	require.NoError(t, err)
	require.Equal(t, 1, len(toDelete))
	require.Equal(t, baseDiskFromZone1, toDelete[0])

	toSchedule, err := storage.TakeBaseDisksToSchedule(ctx)
	require.NoError(t, err)
	require.Equal(t, 0, len(toSchedule))

	err = storage.BaseDiskCreated(ctx, baseDiskFromZone1)
	require.Error(t, err)
	require.False(t, errors.CanRetry(err))

	// Should not affect anything.
	err = storage.BaseDisksScheduled(ctx, baseDisks)
	require.NoError(t, err)

	err = storage.CheckConsistency(ctx)
	require.NoError(t, err)
}

func TestStorageYDBImageDeleting(t *testing.T) {
	ctx, cancel := context.WithCancel(newContext())
	defer cancel()

	db, err := newYDB(ctx)
	require.NoError(t, err)
	defer db.Close(ctx)

	storage := newStorage(t, ctx, db)

	err = storage.ConfigurePool(
		ctx,
		"image",
		"zone",
		2*makeDefaultConfig().GetMaxActiveSlots(),
		0,
	)
	require.NoError(t, err)

	configured, err := storage.IsPoolConfigured(ctx, "image", "zone")
	require.NoError(t, err)
	require.True(t, configured)

	slot := Slot{
		OverlayDisk: &types.Disk{
			ZoneId: "zone",
			DiskId: "overlay",
		},
	}

	baseDisks, err := storage.TakeBaseDisksToSchedule(ctx)
	require.NoError(t, err)
	require.Equal(t, 2, len(baseDisks))

	scheduled := []BaseDisk{baseDisks[0]}
	scheduled[0].CreateTaskID = "create"

	err = storage.BaseDisksScheduled(ctx, scheduled)
	require.NoError(t, err)

	actual, err := storage.AcquireBaseDiskSlot(ctx, "image", slot)
	require.NoError(t, err)
	require.Equal(t, scheduled[0], actual)

	err = storage.ImageDeleting(ctx, "image")
	require.NoError(t, err)

	configured, err = storage.IsPoolConfigured(ctx, "image", "zone")
	require.NoError(t, err)
	require.False(t, configured)

	toDelete, err := storage.GetBaseDisksToDelete(ctx, 100500)
	require.NoError(t, err)
	require.Equal(t, 1, len(toDelete))
	require.Equal(t, baseDisks[1], toDelete[0])

	// Check idempotency.
	err = storage.ImageDeleting(ctx, "image")
	require.NoError(t, err)

	toDelete, err = storage.GetBaseDisksToDelete(ctx, 100500)
	require.NoError(t, err)
	require.Equal(t, 1, len(toDelete))
	require.Equal(t, baseDisks[1], toDelete[0])

	toSchedule, err := storage.TakeBaseDisksToSchedule(ctx)
	require.NoError(t, err)
	require.Equal(t, 0, len(toSchedule))

	err = storage.BaseDiskCreated(ctx, baseDisks[0])
	require.NoError(t, err)

	err = storage.BaseDiskCreated(ctx, baseDisks[1])
	require.Error(t, err)
	require.False(t, errors.CanRetry(err))

	// Should not affect anything.
	err = storage.BaseDisksScheduled(ctx, baseDisks)
	require.NoError(t, err)

	// Acquired base disk should be deleted.
	actual, err = storage.ReleaseBaseDiskSlot(ctx, slot.OverlayDisk)
	require.NoError(t, err)
	require.Equal(t, scheduled[0].ID, actual.ID)
	require.True(t, baseDiskShouldBeDeletedSoon(t, ctx, storage, actual))

	// Check idempotency.
	actual, err = storage.ReleaseBaseDiskSlot(ctx, slot.OverlayDisk)
	require.NoError(t, err)
	require.Equal(t, scheduled[0].ID, actual.ID)
	require.True(t, baseDiskShouldBeDeletedSoon(t, ctx, storage, actual))

	err = storage.CheckConsistency(ctx)
	require.NoError(t, err)
}

func TestStorageYDBBaseDisksDeleted(t *testing.T) {
	ctx, cancel := context.WithCancel(newContext())
	defer cancel()

	db, err := newYDB(ctx)
	require.NoError(t, err)
	defer db.Close(ctx)

	storage := newStorage(t, ctx, db)

	err = storage.ConfigurePool(
		ctx,
		"image",
		"zone",
		3*makeDefaultConfig().GetMaxActiveSlots(),
		0,
	)
	require.NoError(t, err)

	slot := Slot{
		OverlayDisk: &types.Disk{
			ZoneId: "zone",
			DiskId: "overlay",
		},
	}

	baseDisks, err := storage.TakeBaseDisksToSchedule(ctx)
	require.NoError(t, err)
	require.Equal(t, 3, len(baseDisks))

	scheduled := []BaseDisk{baseDisks[0]}
	scheduled[0].CreateTaskID = "create"

	err = storage.BaseDisksScheduled(ctx, scheduled)
	require.NoError(t, err)

	actual, err := storage.AcquireBaseDiskSlot(ctx, "image", slot)
	require.NoError(t, err)
	require.Equal(t, scheduled[0], actual)

	err = storage.ImageDeleting(ctx, "image")
	require.NoError(t, err)

	toDelete, err := storage.GetBaseDisksToDelete(ctx, 100500)
	require.NoError(t, err)
	require.Equal(t, 2, len(toDelete))
	require.ElementsMatch(t, []BaseDisk{baseDisks[1], baseDisks[2]}, toDelete)

	// Check idempotency.
	toDelete, err = storage.GetBaseDisksToDelete(ctx, 100500)
	require.NoError(t, err)
	require.Equal(t, 2, len(toDelete))
	require.ElementsMatch(t, []BaseDisk{baseDisks[1], baseDisks[2]}, toDelete)

	err = storage.BaseDisksDeleted(ctx, toDelete)
	require.NoError(t, err)

	toDelete, err = storage.GetBaseDisksToDelete(ctx, 100500)
	require.NoError(t, err)
	require.Empty(t, toDelete)

	// Check idempotency.
	err = storage.BaseDisksDeleted(ctx, toDelete)
	require.NoError(t, err)

	toDelete, err = storage.GetBaseDisksToDelete(ctx, 100500)
	require.NoError(t, err)
	require.Empty(t, toDelete)

	// Check that acquired disk is not deleted.
	err = storage.BaseDiskCreated(ctx, baseDisks[0])
	require.NoError(t, err)

	err = storage.BaseDiskCreated(ctx, baseDisks[1])
	require.Error(t, err)
	require.False(t, errors.CanRetry(err))

	err = storage.BaseDiskCreated(ctx, baseDisks[2])
	require.Error(t, err)
	require.False(t, errors.CanRetry(err))

	// Should not affect anything.
	err = storage.BaseDisksScheduled(ctx, baseDisks)
	require.NoError(t, err)

	err = storage.CheckConsistency(ctx)
	require.NoError(t, err)
}

func TestStorageYDBBaseDiskCreationFailed(t *testing.T) {
	ctx, cancel := context.WithCancel(newContext())
	defer cancel()

	db, err := newYDB(ctx)
	require.NoError(t, err)
	defer db.Close(ctx)

	storage := newStorage(t, ctx, db)

	// There are no disks - nothing to do.
	err = storage.BaseDiskCreationFailed(ctx, BaseDisk{
		ID:      "id",
		ImageID: "image",
		ZoneID:  "zone",
	})
	require.NoError(t, err)

	err = storage.ConfigurePool(ctx, "image", "zone", 1, 0)
	require.NoError(t, err)

	baseDisks1, err := storage.TakeBaseDisksToSchedule(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, len(baseDisks1))

	err = storage.BaseDiskCreationFailed(ctx, baseDisks1[0])
	require.NoError(t, err)

	actual, err := storage.GetBaseDisksToDelete(ctx, 100500)
	require.NoError(t, err)
	require.Equal(t, 1, len(actual))
	require.Equal(t, baseDisks1[0], actual[0])

	baseDisks2, err := storage.TakeBaseDisksToSchedule(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, len(baseDisks2))
	require.NotEqual(t, baseDisks1[0].ID, baseDisks2[0].ID)

	// Check idempotency.
	err = storage.BaseDiskCreationFailed(ctx, baseDisks1[0])
	require.NoError(t, err)

	err = storage.BaseDisksScheduled(ctx, baseDisks2)
	require.NoError(t, err)

	err = storage.BaseDiskCreated(ctx, baseDisks2[0])
	require.NoError(t, err)

	err = storage.BaseDiskCreationFailed(ctx, baseDisks2[0])
	require.NoError(t, err)

	// Check idempotency.
	err = storage.BaseDiskCreated(ctx, baseDisks2[0])
	require.NoError(t, err)

	// Check idempotency.
	actual, err = storage.GetBaseDisksToDelete(ctx, 100500)
	require.NoError(t, err)
	require.Equal(t, 1, len(actual))
	require.Equal(t, baseDisks1[0], actual[0])

	err = storage.CheckConsistency(ctx)
	require.NoError(t, err)
}

func TestStorageYDBAcquireShouldUseBaseDiskAllocationUnits(t *testing.T) {
	ctx, cancel := context.WithCancel(newContext())
	defer cancel()

	db, err := newYDB(ctx)
	require.NoError(t, err)
	defer db.Close(ctx)

	storage := newStorage(t, ctx, db)

	err = storage.ConfigurePool(ctx, "image", "zone", 1, 0)
	require.NoError(t, err)

	baseDisks, err := storage.TakeBaseDisksToSchedule(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, len(baseDisks))

	baseDisks[0].CreateTaskID = "create"
	err = storage.BaseDisksScheduled(ctx, baseDisks)
	require.NoError(t, err)

	err = storage.BaseDiskCreated(ctx, baseDisks[0])
	require.NoError(t, err)

	// 4 TB.
	size := uint64(4 << 40)
	allottedUnits := computeAllottedUnits(&slot{
		overlayDiskKind: types.DiskKind_DISK_KIND_SSD,
		overlayDiskSize: size,
	})
	defaultConfig := makeDefaultConfig()
	units := uint64(defaultConfig.GetMaxBaseDiskUnits())
	slotCount := int(units / allottedUnits)
	if units%allottedUnits != 0 {
		// Floor the number.
		slotCount++
	}
	// Otherwise test is invalid.
	require.Less(t, slotCount, int(defaultConfig.GetMaxActiveSlots()))

	// Check that base disk is reused several times.
	for i := 0; i < 2; i++ {
		slots := make([]*Slot, 0)
		for j := 0; j < slotCount; j++ {
			slots = append(slots, &Slot{
				OverlayDisk: &types.Disk{
					ZoneId: "zone",
					DiskId: fmt.Sprintf("overlay%v_%v", i, j),
				},
				OverlayDiskKind: types.DiskKind_DISK_KIND_SSD,
				OverlayDiskSize: size,
			})
		}

		for _, slot := range slots {
			if i == 0 {
				actual, err := storage.TakeBaseDisksToSchedule(ctx)
				require.NoError(t, err)
				require.Empty(t, actual)
			} else {
				actual, err := storage.TakeBaseDisksToSchedule(ctx)
				require.NoError(t, err)
				require.Equal(t, 1, len(actual))
				require.NotEqual(t, baseDisks[0].ID, actual[0].ID)
			}

			acquired, err := storage.AcquireBaseDiskSlot(ctx, "image", *slot)
			require.NoError(t, err)
			require.Equal(t, baseDisks[0].ID, acquired.ID)
		}

		actual, err := storage.TakeBaseDisksToSchedule(ctx)
		require.NoError(t, err)
		require.Equal(t, 1, len(actual))
		require.NotEqual(t, baseDisks[0].ID, actual[0].ID)

		for _, slot := range slots {
			actual, err := storage.ReleaseBaseDiskSlot(ctx, slot.OverlayDisk)
			require.NoError(t, err)
			require.Equal(t, baseDisks[0].ID, actual.ID)
		}
	}

	err = storage.CheckConsistency(ctx)
	require.NoError(t, err)
}

func TestStorageYDBRebaseOverlayDisk1(t *testing.T) {
	ctx, cancel := context.WithCancel(newContext())
	defer cancel()

	db, err := newYDB(ctx)
	require.NoError(t, err)
	defer db.Close(ctx)

	storage := newStorage(t, ctx, db)

	err = storage.ConfigurePool(
		ctx,
		"image",
		"zone",
		makeDefaultConfig().GetMaxActiveSlots()+1,
		0,
	)
	require.NoError(t, err)

	baseDisks, err := storage.TakeBaseDisksToSchedule(ctx)
	require.NoError(t, err)
	require.Equal(t, 2, len(baseDisks))

	for i := 0; i < len(baseDisks); i++ {
		baseDisks[i].CreateTaskID = "create"
	}
	err = storage.BaseDisksScheduled(ctx, baseDisks)
	require.NoError(t, err)

	slot := Slot{
		OverlayDisk: &types.Disk{
			ZoneId: "zone",
			DiskId: "overlay",
		},
	}

	source, err := storage.AcquireBaseDiskSlot(ctx, "image", slot)
	require.NoError(t, err)
	require.Contains(t, baseDisks, source)

	var target BaseDisk

	for i := 0; i < len(baseDisks); i++ {
		baseDisks[i].Ready = true

		if baseDisks[i].ID != source.ID {
			target = baseDisks[i]
		}
	}

	err = storage.OverlayDiskRebasing(ctx, RebaseInfo{
		OverlayDisk:      slot.OverlayDisk,
		TargetBaseDiskID: target.ID,
	})
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.NewInterruptExecutionError()))

	for _, baseDisk := range baseDisks {
		err = storage.BaseDiskCreated(ctx, baseDisk)
		require.NoError(t, err)
	}

	err = storage.OverlayDiskRebasing(ctx, RebaseInfo{
		OverlayDisk:      slot.OverlayDisk,
		TargetBaseDiskID: target.ID,
	})
	require.NoError(t, err)

	// Wrong generation won't work.
	err = storage.OverlayDiskRebasing(ctx, RebaseInfo{
		OverlayDisk:      slot.OverlayDisk,
		TargetBaseDiskID: target.ID,
		SlotGeneration:   100500,
	})
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.NewEmptyNonRetriableError()))
	require.ErrorContains(t, err, "wrong generation")

	// Check idempotency.
	err = storage.OverlayDiskRebasing(ctx, RebaseInfo{
		OverlayDisk:      slot.OverlayDisk,
		TargetBaseDiskID: target.ID,
	})
	require.NoError(t, err)

	// Wrong generation won't work.
	err = storage.OverlayDiskRebased(ctx, RebaseInfo{
		OverlayDisk:      slot.OverlayDisk,
		TargetBaseDiskID: target.ID,
		SlotGeneration:   100500,
	})
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.NewEmptyNonRetriableError()))
	require.ErrorContains(t, err, "wrong generation")

	err = storage.OverlayDiskRebased(ctx, RebaseInfo{
		OverlayDisk:      slot.OverlayDisk,
		TargetBaseDiskID: target.ID,
	})
	require.NoError(t, err)

	// Check idempotency.
	err = storage.OverlayDiskRebased(ctx, RebaseInfo{
		OverlayDisk:      slot.OverlayDisk,
		TargetBaseDiskID: target.ID,
		SlotGeneration:   100500,
	})
	require.NoError(t, err)

	// Check idempotency.
	err = storage.OverlayDiskRebasing(ctx, RebaseInfo{
		OverlayDisk:      slot.OverlayDisk,
		TargetBaseDiskID: target.ID,
	})
	require.NoError(t, err)

	actual, err := storage.AcquireBaseDiskSlot(ctx, "image", slot)
	require.NoError(t, err)
	require.Equal(t, target, actual)

	actual, err = storage.ReleaseBaseDiskSlot(ctx, slot.OverlayDisk)
	require.NoError(t, err)
	require.Equal(t, target, actual)

	err = storage.OverlayDiskRebasing(ctx, RebaseInfo{
		OverlayDisk:      slot.OverlayDisk,
		TargetBaseDiskID: target.ID,
	})
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.NewEmptyNonRetriableError()))

	err = storage.CheckConsistency(ctx)
	require.NoError(t, err)
}

func TestStorageYDBRebaseOverlayDisk2(t *testing.T) {
	ctx, cancel := context.WithCancel(newContext())
	defer cancel()

	db, err := newYDB(ctx)
	require.NoError(t, err)
	defer db.Close(ctx)

	storage := newStorage(t, ctx, db)

	err = storage.ConfigurePool(
		ctx,
		"image",
		"zone",
		makeDefaultConfig().GetMaxActiveSlots()+1,
		0,
	)
	require.NoError(t, err)

	baseDisks, err := storage.TakeBaseDisksToSchedule(ctx)
	require.NoError(t, err)
	require.Equal(t, 2, len(baseDisks))

	for i := 0; i < len(baseDisks); i++ {
		baseDisks[i].CreateTaskID = "create"
	}
	err = storage.BaseDisksScheduled(ctx, baseDisks)
	require.NoError(t, err)

	for i := 0; i < len(baseDisks); i++ {
		baseDisks[i].Ready = true
		err = storage.BaseDiskCreated(ctx, baseDisks[i])
		require.NoError(t, err)
	}

	slot := Slot{
		OverlayDisk: &types.Disk{
			ZoneId: "zone",
			DiskId: "overlay",
		},
	}

	source, err := storage.AcquireBaseDiskSlot(ctx, "image", slot)
	require.NoError(t, err)
	require.Contains(t, baseDisks, source)

	var target BaseDisk
	for _, baseDisk := range baseDisks {
		if baseDisk.ID != source.ID {
			target = baseDisk
		}
	}

	err = storage.OverlayDiskRebasing(ctx, RebaseInfo{
		OverlayDisk:      slot.OverlayDisk,
		TargetBaseDiskID: target.ID,
	})
	require.NoError(t, err)

	// Release slot before rebase is finished.
	actual, err := storage.ReleaseBaseDiskSlot(ctx, slot.OverlayDisk)
	require.NoError(t, err)
	require.Equal(t, source, actual)

	err = storage.OverlayDiskRebased(ctx, RebaseInfo{
		OverlayDisk:      slot.OverlayDisk,
		TargetBaseDiskID: target.ID,
	})
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.NewEmptyNonRetriableError()))

	err = storage.OverlayDiskRebasing(ctx, RebaseInfo{
		OverlayDisk:      slot.OverlayDisk,
		TargetBaseDiskID: target.ID,
	})
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.NewEmptyNonRetriableError()))

	err = storage.CheckConsistency(ctx)
	require.NoError(t, err)
}

func TestStorageYDBRelocateOverlayDisk(t *testing.T) {
	ctx, cancel := context.WithCancel(newContext())
	defer cancel()

	db, err := newYDB(ctx)
	require.NoError(t, err)
	defer db.Close(ctx)

	storage := newStorage(t, ctx, db)

	err = storage.ConfigurePool(
		ctx,
		"image",
		"zone",
		makeDefaultConfig().GetMaxActiveSlots()+1,
		0,
	)
	require.NoError(t, err)

	err = storage.ConfigurePool(
		ctx,
		"image",
		"other",
		makeDefaultConfig().GetMaxActiveSlots()+1,
		0,
	)
	require.NoError(t, err)

	baseDisks, err := storage.TakeBaseDisksToSchedule(ctx)
	require.NoError(t, err)
	require.Equal(t, 4, len(baseDisks))

	for i := 0; i < len(baseDisks); i++ {
		baseDisks[i].CreateTaskID = "create"
	}
	err = storage.BaseDisksScheduled(ctx, baseDisks)
	require.NoError(t, err)

	slot := Slot{
		OverlayDisk: &types.Disk{
			ZoneId: "zone",
			DiskId: "overlay",
		},
	}

	source, err := storage.AcquireBaseDiskSlot(ctx, "image", slot)
	require.NoError(t, err)
	require.Contains(t, baseDisks, source)

	for _, baseDisk := range baseDisks {
		err = storage.BaseDiskCreated(ctx, baseDisk)
		require.NoError(t, err)
	}

	for i := 0; i < len(baseDisks); i++ {
		baseDisks[i].Ready = true
	}

	var baseDisksInOtherZoneIds []string
	for _, baseDisk := range baseDisks {
		if baseDisk.ZoneID == "other" {
			baseDisksInOtherZoneIds = append(baseDisksInOtherZoneIds, baseDisk.ID)
		}
	}

	relocateInfo, err := relocateOverlayDisk(
		ctx,
		db,
		storage,
		slot.OverlayDisk,
		"other",
	)
	require.NoError(t, err)
	require.Contains(t, baseDisksInOtherZoneIds, relocateInfo.TargetBaseDiskID)

	// Check idempotency.
	relocateInfo, err = relocateOverlayDisk(
		ctx,
		db,
		storage,
		slot.OverlayDisk,
		"other",
	)
	require.NoError(t, err)
	require.Contains(t, baseDisksInOtherZoneIds, relocateInfo.TargetBaseDiskID)

	err = storage.OverlayDiskRebasing(ctx, RebaseInfo{
		OverlayDisk:      relocateInfo.OverlayDisk,
		TargetZoneID:     relocateInfo.TargetZoneID,
		TargetBaseDiskID: relocateInfo.TargetBaseDiskID,
		SlotGeneration:   relocateInfo.SlotGeneration,
	})
	require.NoError(t, err)

	// Check idempotency.
	err = storage.OverlayDiskRebasing(ctx, RebaseInfo{
		OverlayDisk:      relocateInfo.OverlayDisk,
		TargetZoneID:     relocateInfo.TargetZoneID,
		TargetBaseDiskID: relocateInfo.TargetBaseDiskID,
		SlotGeneration:   relocateInfo.SlotGeneration,
	})
	require.NoError(t, err)

	err = storage.OverlayDiskRebased(ctx, RebaseInfo{
		OverlayDisk:      relocateInfo.OverlayDisk,
		TargetZoneID:     relocateInfo.TargetZoneID,
		TargetBaseDiskID: relocateInfo.TargetBaseDiskID,
		SlotGeneration:   relocateInfo.SlotGeneration,
	})
	require.NoError(t, err)

	err = storage.CheckConsistency(ctx)
	require.NoError(t, err)
}

func TestStorageYDBRelocateOverlayDiskWithoutPool(t *testing.T) {
	ctx, cancel := context.WithCancel(newContext())
	defer cancel()

	db, err := newYDB(ctx)
	require.NoError(t, err)
	defer db.Close(ctx)

	storage := newStorage(t, ctx, db)

	err = storage.ConfigurePool(
		ctx,
		"image",
		"zone",
		makeDefaultConfig().GetMaxActiveSlots()+1,
		0,
	)
	require.NoError(t, err)

	baseDisks, err := storage.TakeBaseDisksToSchedule(ctx)
	require.NoError(t, err)
	require.Equal(t, 2, len(baseDisks))

	for i := 0; i < len(baseDisks); i++ {
		baseDisks[i].CreateTaskID = "create"
	}
	err = storage.BaseDisksScheduled(ctx, baseDisks)
	require.NoError(t, err)

	slot := Slot{
		OverlayDisk: &types.Disk{
			ZoneId: "zone",
			DiskId: "overlay",
		},
	}

	source, err := storage.AcquireBaseDiskSlot(ctx, "image", slot)
	require.NoError(t, err)
	require.Contains(t, baseDisks, source)

	for _, baseDisk := range baseDisks {
		err = storage.BaseDiskCreated(ctx, baseDisk)
		require.NoError(t, err)
	}

	for i := 0; i < len(baseDisks); i++ {
		baseDisks[i].Ready = true
	}

	relocateInfo, err := relocateOverlayDisk(
		ctx,
		db,
		storage,
		slot.OverlayDisk,
		"other",
	)
	require.NoError(t, err)
	require.EqualValues(t, relocateInfo, RebaseInfo{})
}

func TestStorageYDBRebaseOverlayDiskDuringRelocating(t *testing.T) {
	ctx, cancel := context.WithCancel(newContext())
	defer cancel()

	db, err := newYDB(ctx)
	require.NoError(t, err)
	defer db.Close(ctx)

	storage := newStorage(t, ctx, db)

	err = storage.ConfigurePool(
		ctx,
		"image",
		"zone",
		makeDefaultConfig().GetMaxActiveSlots()+1,
		0,
	)
	require.NoError(t, err)

	err = storage.ConfigurePool(
		ctx,
		"image",
		"other",
		makeDefaultConfig().GetMaxActiveSlots()+1,
		0,
	)
	require.NoError(t, err)

	baseDisks, err := storage.TakeBaseDisksToSchedule(ctx)
	require.NoError(t, err)
	require.Equal(t, 4, len(baseDisks))

	for i := 0; i < len(baseDisks); i++ {
		baseDisks[i].CreateTaskID = "create"
	}
	err = storage.BaseDisksScheduled(ctx, baseDisks)
	require.NoError(t, err)

	slot := Slot{
		OverlayDisk: &types.Disk{
			ZoneId: "zone",
			DiskId: "overlay",
		},
	}

	source, err := storage.AcquireBaseDiskSlot(ctx, "image", slot)
	require.NoError(t, err)
	require.Contains(t, baseDisks, source)

	for _, baseDisk := range baseDisks {
		err = storage.BaseDiskCreated(ctx, baseDisk)
		require.NoError(t, err)
	}

	for i := 0; i < len(baseDisks); i++ {
		baseDisks[i].Ready = true
	}

	var targetBaseDiskID string
	var baseDisksInOtherZoneIDs []string
	for _, baseDisk := range baseDisks {
		if baseDisk.ZoneID == "other" {
			baseDisksInOtherZoneIDs = append(baseDisksInOtherZoneIDs, baseDisk.ID)
		} else if baseDisk.ID != source.ID {
			targetBaseDiskID = baseDisk.ID
		}
	}

	relocateInfo, err := relocateOverlayDisk(
		ctx,
		db,
		storage,
		slot.OverlayDisk,
		"other",
	)
	require.NoError(t, err)
	require.Contains(t, baseDisksInOtherZoneIDs, relocateInfo.TargetBaseDiskID)

	err = storage.OverlayDiskRebasing(ctx, RebaseInfo{
		OverlayDisk:      relocateInfo.OverlayDisk,
		TargetZoneID:     relocateInfo.TargetZoneID,
		TargetBaseDiskID: relocateInfo.TargetBaseDiskID,
		SlotGeneration:   relocateInfo.SlotGeneration,
	})
	require.NoError(t, err)

	err = storage.OverlayDiskRebasing(ctx, RebaseInfo{
		OverlayDisk:      slot.OverlayDisk,
		TargetBaseDiskID: targetBaseDiskID,
	})
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.NewEmptyNonRetriableError()))
	require.ErrorContains(t, err, "wrong generation")

	err = storage.OverlayDiskRebasing(ctx, RebaseInfo{
		OverlayDisk:      slot.OverlayDisk,
		TargetBaseDiskID: targetBaseDiskID,
		SlotGeneration:   relocateInfo.SlotGeneration,
	})
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.NewEmptyNonRetriableError()))
	require.ErrorContains(t, err, "another rebase or relocate is in progress for slot")

	err = storage.CheckConsistency(ctx)
	require.NoError(t, err)
}

func TestStorageYDBRetiringOverlayDiskDuringRelocating(t *testing.T) {
	ctx, cancel := context.WithCancel(newContext())
	defer cancel()

	db, err := newYDB(ctx)
	require.NoError(t, err)
	defer db.Close(ctx)

	storage := newStorage(t, ctx, db)

	err = storage.ConfigurePool(
		ctx,
		"image",
		"zone",
		makeDefaultConfig().GetMaxActiveSlots()+1,
		0,
	)
	require.NoError(t, err)

	err = storage.ConfigurePool(
		ctx,
		"image",
		"other",
		makeDefaultConfig().GetMaxActiveSlots()+1,
		0,
	)
	require.NoError(t, err)

	baseDisks, err := storage.TakeBaseDisksToSchedule(ctx)
	require.NoError(t, err)
	require.Equal(t, 4, len(baseDisks))

	for i := 0; i < len(baseDisks); i++ {
		baseDisks[i].CreateTaskID = "create"
	}
	err = storage.BaseDisksScheduled(ctx, baseDisks)
	require.NoError(t, err)

	slot := Slot{
		OverlayDisk: &types.Disk{
			ZoneId: "zone",
			DiskId: "overlay",
		},
	}

	source, err := storage.AcquireBaseDiskSlot(ctx, "image", slot)
	require.NoError(t, err)
	require.Contains(t, baseDisks, source)

	for _, baseDisk := range baseDisks {
		err = storage.BaseDiskCreated(ctx, baseDisk)
		require.NoError(t, err)
	}

	for i := 0; i < len(baseDisks); i++ {
		baseDisks[i].Ready = true
	}

	var baseDisksInOtherZoneIDs []string
	for _, baseDisk := range baseDisks {
		if baseDisk.ZoneID == "other" {
			baseDisksInOtherZoneIDs = append(baseDisksInOtherZoneIDs, baseDisk.ID)
		}
	}

	relocateInfo, err := relocateOverlayDisk(
		ctx,
		db,
		storage,
		slot.OverlayDisk,
		"other",
	)
	require.NoError(t, err)
	require.Contains(t, baseDisksInOtherZoneIDs, relocateInfo.TargetBaseDiskID)

	err = storage.OverlayDiskRebased(ctx, RebaseInfo{
		OverlayDisk:      relocateInfo.OverlayDisk,
		TargetZoneID:     relocateInfo.TargetZoneID,
		TargetBaseDiskID: relocateInfo.TargetBaseDiskID,
		SlotGeneration:   relocateInfo.SlotGeneration,
	})
	require.NoError(t, err)

	_, err = storage.RetireBaseDisk(
		ctx,
		relocateInfo.TargetBaseDiskID,
		nil, // srcDisk
		0,   // useImageSize
	)
	require.NoError(t, err)

	// Should be idempotent.
	err = storage.OverlayDiskRebased(ctx, RebaseInfo{
		OverlayDisk:      relocateInfo.OverlayDisk,
		TargetZoneID:     relocateInfo.TargetZoneID,
		TargetBaseDiskID: relocateInfo.TargetBaseDiskID,
		SlotGeneration:   relocateInfo.SlotGeneration,
	})
	require.NoError(t, err)

	err = storage.CheckConsistency(ctx)
	require.NoError(t, err)
}

func TestStorageYDBRelocatingOverlayDiskAfterRelocatingToAnotherZone(
	t *testing.T,
) {

	ctx, cancel := context.WithCancel(newContext())
	defer cancel()

	db, err := newYDB(ctx)
	require.NoError(t, err)
	defer db.Close(ctx)

	storage := newStorage(t, ctx, db)

	err = storage.ConfigurePool(
		ctx,
		"image",
		"zone",
		makeDefaultConfig().GetMaxActiveSlots()+1,
		0,
	)
	require.NoError(t, err)

	err = storage.ConfigurePool(
		ctx,
		"image",
		"other",
		makeDefaultConfig().GetMaxActiveSlots()+1,
		0,
	)
	require.NoError(t, err)

	err = storage.ConfigurePool(
		ctx,
		"image",
		"another",
		makeDefaultConfig().GetMaxActiveSlots()+1,
		0,
	)
	require.NoError(t, err)

	baseDisks, err := storage.TakeBaseDisksToSchedule(ctx)
	require.NoError(t, err)
	require.Equal(t, 6, len(baseDisks))

	for i := 0; i < len(baseDisks); i++ {
		baseDisks[i].CreateTaskID = "create"
	}
	err = storage.BaseDisksScheduled(ctx, baseDisks)
	require.NoError(t, err)

	slot := Slot{
		OverlayDisk: &types.Disk{
			ZoneId: "zone",
			DiskId: "overlay",
		},
	}

	source, err := storage.AcquireBaseDiskSlot(ctx, "image", slot)
	require.NoError(t, err)
	require.Contains(t, baseDisks, source)

	for _, baseDisk := range baseDisks {
		err = storage.BaseDiskCreated(ctx, baseDisk)
		require.NoError(t, err)
	}

	for i := 0; i < len(baseDisks); i++ {
		baseDisks[i].Ready = true
	}

	var baseDisksInOtherZoneIds []string
	var baseDisksInAnotherZoneIds []string
	for _, baseDisk := range baseDisks {
		if baseDisk.ZoneID == "other" {
			baseDisksInOtherZoneIds = append(baseDisksInOtherZoneIds, baseDisk.ID)
		} else if baseDisk.ZoneID == "another" {
			baseDisksInAnotherZoneIds = append(baseDisksInAnotherZoneIds, baseDisk.ID)
		}
	}

	relocateInfo, err := relocateOverlayDisk(
		ctx,
		db,
		storage,
		slot.OverlayDisk,
		"other",
	)
	require.NoError(t, err)
	require.Contains(t, baseDisksInOtherZoneIds, relocateInfo.TargetBaseDiskID)

	err = storage.OverlayDiskRebasing(ctx, RebaseInfo{
		OverlayDisk:      relocateInfo.OverlayDisk,
		TargetZoneID:     relocateInfo.TargetZoneID,
		TargetBaseDiskID: relocateInfo.TargetBaseDiskID,
		SlotGeneration:   relocateInfo.SlotGeneration,
	})
	require.NoError(t, err)
	outdatedRelocateInfo := relocateInfo

	relocateInfo, err = relocateOverlayDisk(
		ctx,
		db,
		storage,
		slot.OverlayDisk,
		"another",
	)
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.NewInterruptExecutionError()))

	relocateInfo, err = relocateOverlayDisk(
		ctx,
		db,
		storage,
		slot.OverlayDisk,
		"another",
	)
	require.NoError(t, err)
	require.Contains(t, baseDisksInAnotherZoneIds, relocateInfo.TargetBaseDiskID)

	err = storage.OverlayDiskRebased(ctx, RebaseInfo{
		OverlayDisk:      outdatedRelocateInfo.OverlayDisk,
		TargetZoneID:     outdatedRelocateInfo.TargetZoneID,
		TargetBaseDiskID: outdatedRelocateInfo.TargetBaseDiskID,
		SlotGeneration:   outdatedRelocateInfo.SlotGeneration,
	})
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.NewEmptyNonRetriableError()))
	require.ErrorContains(t, err, "wrong generation")

	err = storage.OverlayDiskRebasing(ctx, RebaseInfo{
		OverlayDisk:      relocateInfo.OverlayDisk,
		TargetZoneID:     relocateInfo.TargetZoneID,
		TargetBaseDiskID: relocateInfo.TargetBaseDiskID,
		SlotGeneration:   relocateInfo.SlotGeneration,
	})
	require.NoError(t, err)

	err = storage.OverlayDiskRebased(ctx, RebaseInfo{
		OverlayDisk:      relocateInfo.OverlayDisk,
		TargetZoneID:     relocateInfo.TargetZoneID,
		TargetBaseDiskID: relocateInfo.TargetBaseDiskID,
		SlotGeneration:   relocateInfo.SlotGeneration,
	})
	require.NoError(t, err)

	err = storage.CheckConsistency(ctx)
	require.NoError(t, err)
}

func TestStorageYDBAbortOverlayDiskRebasing(t *testing.T) {
	ctx, cancel := context.WithCancel(newContext())
	defer cancel()

	db, err := newYDB(ctx)
	require.NoError(t, err)
	defer db.Close(ctx)

	storage := newStorage(t, ctx, db)

	err = storage.ConfigurePool(
		ctx,
		"image",
		"zone",
		2*makeDefaultConfig().GetMaxActiveSlots()+1,
		0,
	)
	require.NoError(t, err)

	baseDisks, err := storage.TakeBaseDisksToSchedule(ctx)
	require.NoError(t, err)
	require.Equal(t, 3, len(baseDisks))

	for i := 0; i < len(baseDisks); i++ {
		baseDisks[i].CreateTaskID = "create"
	}
	err = storage.BaseDisksScheduled(ctx, baseDisks)
	require.NoError(t, err)

	slot := Slot{
		OverlayDisk: &types.Disk{
			ZoneId: "zone",
			DiskId: "overlay",
		},
	}

	source, err := storage.AcquireBaseDiskSlot(ctx, "image", slot)
	require.NoError(t, err)
	require.Contains(t, baseDisks, source)

	var target BaseDisk
	var anotherTarget BaseDisk

	for i := 0; i < len(baseDisks); i++ {
		baseDisks[i].Ready = true

		if baseDisks[i].ID == source.ID {
			continue
		}

		if len(target.ID) == 0 {
			target = baseDisks[i]
		} else {
			anotherTarget = baseDisks[i]
		}
	}

	for _, baseDisk := range baseDisks {
		if baseDisk.ID != target.ID {
			err = storage.BaseDiskCreated(ctx, baseDisk)
			require.NoError(t, err)
		}
	}

	err = storage.OverlayDiskRebasing(ctx, RebaseInfo{
		OverlayDisk:      slot.OverlayDisk,
		TargetBaseDiskID: target.ID,
	})
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.NewInterruptExecutionError()))

	// Rebasing to another target is forbidden.
	err = storage.OverlayDiskRebasing(ctx, RebaseInfo{
		OverlayDisk:      slot.OverlayDisk,
		TargetBaseDiskID: anotherTarget.ID,
	})
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.NewEmptyNonRetriableError()))

	err = storage.BaseDiskCreationFailed(ctx, target)
	require.NoError(t, err)

	// Current rebase is aborted.
	err = storage.OverlayDiskRebasing(ctx, RebaseInfo{
		OverlayDisk:      slot.OverlayDisk,
		TargetBaseDiskID: target.ID,
	})
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.NewEmptyNonRetriableError()))

	// Another rebase is permitted to go.
	err = storage.OverlayDiskRebasing(ctx, RebaseInfo{
		OverlayDisk:      slot.OverlayDisk,
		TargetBaseDiskID: anotherTarget.ID,
	})
	require.NoError(t, err)

	err = storage.CheckConsistency(ctx)
	require.NoError(t, err)
}

func TestStorageYDBRetireBaseDisks(t *testing.T) {
	ctx, cancel := context.WithCancel(newContext())
	defer cancel()

	db, err := newYDB(ctx)
	require.NoError(t, err)
	defer db.Close(ctx)

	storage := newStorage(t, ctx, db)

	baseDiskCount := 6

	slots := make([]Slot, 0)
	defaultConfig := makeDefaultConfig()
	maxActiveSlots := int(defaultConfig.GetMaxActiveSlots())
	for i := 0; i < baseDiskCount*(maxActiveSlots-1); i++ {
		slots = append(slots, Slot{
			OverlayDisk: &types.Disk{
				ZoneId: "zone",
				DiskId: fmt.Sprintf("overlay%v", i),
			},
		})
	}

	rebaseInfos, err := storage.RetireBaseDisk(
		ctx,
		"unexisting",
		nil, // srcDisk
		0,   // useImageSize
	)
	require.NoError(t, err)
	require.Empty(t, rebaseInfos)

	err = storage.ConfigurePool(
		ctx,
		"image",
		"zone",
		uint32(baseDiskCount)*defaultConfig.GetMaxActiveSlots(),
		0,
	)
	require.NoError(t, err)

	oldBaseDisks := make([]BaseDisk, 0)

	for {
		actual, err := storage.TakeBaseDisksToSchedule(ctx)
		require.NoError(t, err)

		for i := 0; i < len(actual); i++ {
			actual[i].CreateTaskID = "create"
		}

		err = storage.BaseDisksScheduled(ctx, actual)
		require.NoError(t, err)

		for _, disk := range actual {
			err = storage.BaseDiskCreated(ctx, disk)
			require.NoError(t, err)

			oldBaseDisks = append(oldBaseDisks, disk)
		}

		actual, err = storage.TakeBaseDisksToSchedule(ctx)
		require.NoError(t, err)

		if len(actual) == 0 {
			break
		}
	}
	require.Equal(t, baseDiskCount, len(oldBaseDisks))

	baseDiskIDs := make(map[string]string)
	for _, slot := range slots {
		baseDisk, err := storage.AcquireBaseDiskSlot(ctx, "image", slot)
		require.NoError(t, err)
		require.NotEmpty(t, baseDisk.ID)

		baseDiskIDs[slot.OverlayDisk.DiskId] = baseDisk.ID
	}

	newBaseDisks := make([]BaseDisk, 0)

	for _, baseDisk := range oldBaseDisks {
		rebaseInfos, err := storage.RetireBaseDisk(
			ctx,
			baseDisk.ID,
			nil, // srcDisk
			0,   // useImageSize
		)
		require.NoError(t, err)
		require.NotEmpty(t, rebaseInfos)

		for _, info := range rebaseInfos {
			id, ok := baseDiskIDs[info.OverlayDisk.DiskId]
			require.True(t, ok)
			require.NotEqual(t, id, info.TargetBaseDiskID)

			baseDiskIDs[info.OverlayDisk.DiskId] = info.TargetBaseDiskID
		}

		// Check idempotency.
		actual, err := storage.RetireBaseDisk(
			ctx,
			baseDisk.ID,
			nil, // srcDisk
			0,   // useImageSize
		)
		require.NoError(t, err)
		require.ElementsMatch(t, rebaseInfos, actual)

		retired, err := storage.IsBaseDiskRetired(ctx, baseDisk.ID)
		require.NoError(t, err)
		require.False(t, retired)

		toSchedule, err := storage.TakeBaseDisksToSchedule(ctx)
		require.NoError(t, err)

		for i := 0; i < len(toSchedule); i++ {
			toSchedule[i].CreateTaskID = "create"
			require.NotContains(t, oldBaseDisks, toSchedule[i])
		}

		err = storage.BaseDisksScheduled(ctx, toSchedule)
		require.NoError(t, err)

		for i := 0; i < len(toSchedule); i++ {
			err = storage.BaseDiskCreated(ctx, toSchedule[i])
			require.NoError(t, err)

			toSchedule[i].Ready = true
			newBaseDisks = append(newBaseDisks, toSchedule[i])
		}

		for _, info := range rebaseInfos {
			err := storage.OverlayDiskRebasing(ctx, info)
			require.NoError(t, err)

			err = storage.OverlayDiskRebased(ctx, info)
			require.NoError(t, err)
		}
	}

	for _, disk := range oldBaseDisks {
		retired, err := storage.IsBaseDiskRetired(ctx, disk.ID)
		require.NoError(t, err)
		require.True(t, retired)
	}

	toDelete, err := storage.GetBaseDisksToDelete(ctx, 100500)
	require.NoError(t, err)
	require.ElementsMatch(t, oldBaseDisks, toDelete)

	for _, slot := range slots {
		baseDisk, err := storage.AcquireBaseDiskSlot(ctx, "image", slot)
		require.NoError(t, err)
		require.NotEmpty(t, baseDisk.ID)
		require.Contains(t, newBaseDisks, baseDisk)

		id, ok := baseDiskIDs[slot.OverlayDisk.DiskId]
		require.True(t, ok)
		require.Equal(t, id, baseDisk.ID)
	}

	for _, slot := range slots {
		_, err = storage.ReleaseBaseDiskSlot(ctx, slot.OverlayDisk)
		require.NoError(t, err)
	}

	for _, disk := range newBaseDisks {
		retired, err := storage.IsBaseDiskRetired(ctx, disk.ID)
		require.NoError(t, err)
		require.False(t, retired)
	}

	err = storage.CheckConsistency(ctx)
	require.NoError(t, err)
}

func TestStorageYDBBaseDiskShouldNotBeFreeAfterRetireStarted(t *testing.T) {
	ctx, cancel := context.WithCancel(newContext())
	defer cancel()

	db, err := newYDB(ctx)
	require.NoError(t, err)
	defer db.Close(ctx)

	storage := newStorage(t, ctx, db)

	slot1 := Slot{
		OverlayDisk: &types.Disk{
			ZoneId: "zone",
			DiskId: "overlay1",
		},
	}
	slot2 := Slot{
		OverlayDisk: &types.Disk{
			ZoneId: "zone",
			DiskId: "overlay2",
		},
	}
	slot3 := Slot{
		OverlayDisk: &types.Disk{
			ZoneId: "zone",
			DiskId: "overlay3",
		},
	}

	err = storage.ConfigurePool(ctx, "image", "zone", 1, 0)
	require.NoError(t, err)

	baseDisks, err := storage.TakeBaseDisksToSchedule(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, len(baseDisks))
	baseDisks[0].CreateTaskID = "create"

	err = storage.BaseDisksScheduled(ctx, baseDisks)
	require.NoError(t, err)

	for _, disk := range baseDisks {
		err = storage.BaseDiskCreated(ctx, disk)
		require.NoError(t, err)
	}

	toSchedule, err := storage.TakeBaseDisksToSchedule(ctx)
	require.NoError(t, err)
	require.Empty(t, toSchedule)

	// Prevents base disk from deleting via retire.
	_, err = storage.AcquireBaseDiskSlot(ctx, "image", slot1)
	require.NoError(t, err)

	// Prevents base disk from deleting after releasing |slot1|.
	_, err = storage.AcquireBaseDiskSlot(ctx, "image", slot2)
	require.NoError(t, err)

	_, err = storage.RetireBaseDisk(
		ctx,
		baseDisks[0].ID,
		nil, // srcDisk
		0,   // useImageSize
	)
	require.NoError(t, err)

	_, err = storage.AcquireBaseDiskSlot(ctx, "image", slot3)
	require.Error(t, err)
	// Check that there are no free slots (on base disks).
	require.True(t, errors.Is(err, errors.NewInterruptExecutionError()))

	_, err = storage.ReleaseBaseDiskSlot(ctx, slot1.OverlayDisk)
	require.NoError(t, err)

	_, err = storage.AcquireBaseDiskSlot(ctx, "image", slot3)
	require.Error(t, err)
	// Check that there are no free slots (on base disks).
	require.True(t, errors.Is(err, errors.NewInterruptExecutionError()))

	err = storage.CheckConsistency(ctx)
	require.NoError(t, err)
}

func TestStorageYDBCreatePoolOnDemand(t *testing.T) {
	ctx, cancel := context.WithCancel(newContext())
	defer cancel()

	db, err := newYDB(ctx)
	require.NoError(t, err)
	defer db.Close(ctx)

	storage := newStorage(t, ctx, db)

	err = storage.ConfigurePool(ctx, "image", "zone", 0, 0)
	require.NoError(t, err)

	configured, err := storage.IsPoolConfigured(ctx, "image", "zone")
	require.NoError(t, err)
	require.True(t, configured)

	slot := Slot{
		OverlayDisk: &types.Disk{
			ZoneId: "zone",
			DiskId: "overlay",
		},
	}

	baseDisks, err := storage.TakeBaseDisksToSchedule(ctx)
	require.NoError(t, err)
	require.Equal(t, 0, len(baseDisks))

	_, err = storage.AcquireBaseDiskSlot(ctx, "image", slot)
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.NewInterruptExecutionError()))

	baseDisks, err = storage.TakeBaseDisksToSchedule(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, len(baseDisks))

	configured, err = storage.IsPoolConfigured(ctx, "image", "zone")
	require.NoError(t, err)
	require.True(t, configured)

	err = storage.CheckConsistency(ctx)
	require.NoError(t, err)
}

func TestStorageYDBCreatePoolWithImageSize(t *testing.T) {
	ctx, cancel := context.WithCancel(newContext())
	defer cancel()

	db, err := newYDB(ctx)
	require.NoError(t, err)
	defer db.Close(ctx)

	maxActiveSlots := uint32(640)
	maxBaseDisksInflight := uint32(1)
	maxBaseDiskUnits := uint32(640)
	config := &pools_config.PoolsConfig{
		MaxActiveSlots:       &maxActiveSlots,
		MaxBaseDisksInflight: &maxBaseDisksInflight,
		MaxBaseDiskUnits:     &maxBaseDiskUnits,
	}
	storage := newStorageWithConfig(t, ctx, db, config, metrics.NewEmptyRegistry())

	imageSize := uint64(10)
	err = storage.ConfigurePool(ctx, "image", "zone", 1, imageSize)
	require.NoError(t, err)

	baseDisks, err := storage.TakeBaseDisksToSchedule(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, len(baseDisks))
	require.Equal(t, baseDiskUnitSize, baseDisks[0].Size)

	// Check idempotency.
	baseDisks, err = storage.TakeBaseDisksToSchedule(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, len(baseDisks))
	require.Equal(t, baseDiskUnitSize, baseDisks[0].Size)

	baseDisks[0].CreateTaskID = "create"
	err = storage.BaseDisksScheduled(ctx, baseDisks)
	require.NoError(t, err)
	err = storage.BaseDiskCreated(ctx, baseDisks[0])
	require.NoError(t, err)

	for i := 0; i < int(30); i++ {
		_, err = storage.AcquireBaseDiskSlot(
			ctx,
			"image",
			Slot{
				OverlayDisk: &types.Disk{
					ZoneId: "zone",
					DiskId: fmt.Sprintf("overlay%v", i),
				},
			},
		)
		require.NoError(t, err)
	}

	_, err = storage.AcquireBaseDiskSlot(
		ctx,
		"image",
		Slot{
			OverlayDisk: &types.Disk{
				ZoneId: "zone",
				DiskId: "last",
			},
		},
	)
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.NewInterruptExecutionError()))

	baseDisks, err = storage.TakeBaseDisksToSchedule(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, len(baseDisks))

	err = storage.CheckConsistency(ctx)
	require.NoError(t, err)
}

func TestStorageYDBRetireBaseDiskForPoolWithImageSize(t *testing.T) {
	ctx, cancel := context.WithCancel(newContext())
	defer cancel()

	db, err := newYDB(ctx)
	require.NoError(t, err)
	defer db.Close(ctx)

	storage := newStorage(t, ctx, db)

	imageSize := uint64(10)
	err = storage.ConfigurePool(ctx, "image", "zone", 1, imageSize)
	require.NoError(t, err)

	baseDisks, err := storage.TakeBaseDisksToSchedule(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, len(baseDisks))
	require.Equal(t, baseDiskUnitSize, baseDisks[0].Size)

	baseDisks[0].CreateTaskID = "create"
	err = storage.BaseDisksScheduled(ctx, baseDisks)
	require.NoError(t, err)
	err = storage.BaseDiskCreated(ctx, baseDisks[0])
	require.NoError(t, err)

	actual, err := storage.TakeBaseDisksToSchedule(ctx)
	require.NoError(t, err)
	require.Equal(t, 0, len(actual))

	imageSize = uint64(192 << 30)
	err = storage.ConfigurePool(ctx, "image", "zone", 1, imageSize)
	require.NoError(t, err)

	_, err = storage.RetireBaseDisk(
		ctx,
		baseDisks[0].ID,
		nil, // srcDisk
		0,   // useImageSize
	)
	require.NoError(t, err)

	baseDisks, err = storage.TakeBaseDisksToSchedule(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, len(baseDisks))
	require.Equal(t, imageSize, baseDisks[0].Size)

	err = storage.CheckConsistency(ctx)
	require.NoError(t, err)
}

func TestStorageYDBRetireBaseDiskForDeletedPool(t *testing.T) {
	ctx, cancel := context.WithCancel(newContext())
	defer cancel()

	db, err := newYDB(ctx)
	require.NoError(t, err)
	defer db.Close(ctx)

	storage := newStorage(t, ctx, db)

	err = storage.ConfigurePool(ctx, "image", "zone", 1, 0)
	require.NoError(t, err)

	baseDisks, err := storage.TakeBaseDisksToSchedule(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, len(baseDisks))
	require.Equal(t, uint64(0), baseDisks[0].Size)

	baseDisks[0].CreateTaskID = "create"
	err = storage.BaseDisksScheduled(ctx, baseDisks)
	require.NoError(t, err)
	err = storage.BaseDiskCreated(ctx, baseDisks[0])
	require.NoError(t, err)

	actual, err := storage.TakeBaseDisksToSchedule(ctx)
	require.NoError(t, err)
	require.Equal(t, 0, len(actual))

	for i := 0; i < 2; i++ {
		_, err = storage.AcquireBaseDiskSlot(
			ctx,
			"image",
			Slot{
				OverlayDisk: &types.Disk{
					ZoneId: "zone",
					DiskId: fmt.Sprintf("disk%v", i),
				},
			},
		)
		require.NoError(t, err)
	}

	err = storage.DeletePool(ctx, "image", "zone")
	require.NoError(t, err)

	rebaseInfos, err := storage.RetireBaseDisk(
		ctx,
		baseDisks[0].ID,
		&types.Disk{
			ZoneId: baseDisks[0].ZoneID,
			DiskId: baseDisks[0].ID,
		},
		0, // useImageSize
	)
	require.NoError(t, err)

	require.Equal(t, 2, len(rebaseInfos))
	require.Equal(
		t,
		rebaseInfos[0].TargetBaseDiskID,
		rebaseInfos[1].TargetBaseDiskID,
	)

	baseDisks, err = storage.TakeBaseDisksToSchedule(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, len(baseDisks))
	require.Equal(t, uint64(0), baseDisks[0].Size)

	err = storage.CheckConsistency(ctx)
	require.NoError(t, err)
}

func TestStorageYDBRetireBaseDiskForDeletedPoolUsingImageSize(t *testing.T) {
	ctx, cancel := context.WithCancel(newContext())
	defer cancel()

	db, err := newYDB(ctx)
	require.NoError(t, err)
	defer db.Close(ctx)

	storage := newStorage(t, ctx, db)

	err = storage.ConfigurePool(ctx, "image", "zone", 1, 0)
	require.NoError(t, err)

	baseDisks, err := storage.TakeBaseDisksToSchedule(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, len(baseDisks))
	require.Equal(t, uint64(0), baseDisks[0].Size)

	baseDisks[0].CreateTaskID = "create"
	err = storage.BaseDisksScheduled(ctx, baseDisks)
	require.NoError(t, err)
	err = storage.BaseDiskCreated(ctx, baseDisks[0])
	require.NoError(t, err)

	actual, err := storage.TakeBaseDisksToSchedule(ctx)
	require.NoError(t, err)
	require.Equal(t, 0, len(actual))

	for i := 0; i < 2; i++ {
		_, err = storage.AcquireBaseDiskSlot(
			ctx,
			"image",
			Slot{
				OverlayDisk: &types.Disk{
					ZoneId: "zone",
					DiskId: fmt.Sprintf("disk%v", i),
				},
			},
		)
		require.NoError(t, err)
	}

	err = storage.DeletePool(ctx, "image", "zone")
	require.NoError(t, err)

	rebaseInfos, err := storage.RetireBaseDisk(
		ctx,
		baseDisks[0].ID,
		&types.Disk{
			ZoneId: baseDisks[0].ZoneID,
			DiskId: baseDisks[0].ID,
		},
		1, // useImageSize
	)
	require.NoError(t, err)

	require.Equal(t, 2, len(rebaseInfos))
	require.Equal(
		t,
		rebaseInfos[0].TargetBaseDiskID,
		rebaseInfos[1].TargetBaseDiskID,
	)

	baseDisks, err = storage.TakeBaseDisksToSchedule(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, len(baseDisks))
	require.Equal(t, baseDiskUnitSize, baseDisks[0].Size)

	err = storage.CheckConsistency(ctx)
	require.NoError(t, err)
}

func TestStorageYDBReleasingOneSlotShouldNotIncreaseSizeOfPoolIfBaseDiskIsStillUnfree(t *testing.T) {
	ctx, cancel := context.WithCancel(newContext())
	defer cancel()

	db, err := newYDB(ctx)
	require.NoError(t, err)
	defer db.Close(ctx)

	storage := newStorage(t, ctx, db)

	err = storage.ConfigurePool(ctx, "image", "zone", 1, 0)
	require.NoError(t, err)

	baseDisks, err := storage.TakeBaseDisksToSchedule(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, len(baseDisks))

	baseDisks[0].CreateTaskID = "create"
	err = storage.BaseDisksScheduled(ctx, baseDisks)
	require.NoError(t, err)

	_, err = storage.AcquireBaseDiskSlot(
		ctx,
		"image",
		Slot{
			OverlayDisk: &types.Disk{
				ZoneId: "zone",
				DiskId: "small1",
			},
		},
	)
	require.NoError(t, err)

	// Large overlay disk should be able to occupy whole base disk.
	size := uint64(32 << 40)
	allottedUnits := computeAllottedUnits(&slot{
		overlayDiskKind: types.DiskKind_DISK_KIND_SSD,
		overlayDiskSize: size,
	})
	// Otherwise test is invalid.
	require.Greater(
		t,
		allottedUnits,
		uint64(makeDefaultConfig().GetMaxBaseDiskUnits()),
	)

	_, err = storage.AcquireBaseDiskSlot(
		ctx,
		"image",
		Slot{
			OverlayDisk: &types.Disk{
				ZoneId: "zone",
				DiskId: "large",
			},
			OverlayDiskKind: types.DiskKind_DISK_KIND_SSD,
			OverlayDiskSize: size,
		},
	)
	require.NoError(t, err)

	_, err = storage.AcquireBaseDiskSlot(
		ctx,
		"image",
		Slot{
			OverlayDisk: &types.Disk{
				ZoneId: "zone",
				DiskId: "small2",
			},
		},
	)
	// Pool is full.
	require.True(t, errors.Is(err, errors.NewInterruptExecutionError()))

	_, err = storage.ReleaseBaseDiskSlot(
		ctx,
		&types.Disk{
			ZoneId: "zone",
			DiskId: "small1",
		},
	)
	require.NoError(t, err)

	// Check that pool is growing.
	actual, err := storage.TakeBaseDisksToSchedule(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, len(actual))
	require.NotEqual(t, baseDisks[0].ID, actual[0].ID)

	err = storage.CheckConsistency(ctx)
	require.NoError(t, err)
}

func TestStorageYDBReleaseSlotOnDeletedBaseDisk(t *testing.T) {
	ctx, cancel := context.WithCancel(newContext())
	defer cancel()

	db, err := newYDB(ctx)
	require.NoError(t, err)
	defer db.Close(ctx)

	storage := newStorage(t, ctx, db)

	err = storage.ConfigurePool(ctx, "image", "zone", 1, 0)
	require.NoError(t, err)

	slot := Slot{
		OverlayDisk: &types.Disk{
			ZoneId: "zone",
			DiskId: "overlay",
		},
	}

	baseDisks, err := storage.TakeBaseDisksToSchedule(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, len(baseDisks))

	baseDisks[0].CreateTaskID = "create"
	err = storage.BaseDisksScheduled(ctx, baseDisks)
	require.NoError(t, err)

	_, err = storage.AcquireBaseDiskSlot(ctx, "image", slot)
	require.NoError(t, err)

	err = storage.DeletePool(ctx, "image", "zone")
	require.NoError(t, err)

	_, err = storage.ReleaseBaseDiskSlot(ctx, slot.OverlayDisk)
	require.NoError(t, err)

	err = storage.CheckConsistency(ctx)
	require.NoError(t, err)

	err = storage.BaseDisksDeleted(ctx, baseDisks)
	require.NoError(t, err)

	err = storage.ClearDeletedBaseDisks(ctx, time.Now(), 1)
	require.NoError(t, err)

	// Check idempotency.
	_, err = storage.ReleaseBaseDiskSlot(ctx, slot.OverlayDisk)
	require.NoError(t, err)
}

func TestStorageYDBShouldSurviveBaseDisksInflightOverlimit(t *testing.T) {
	ctx, cancel := context.WithCancel(newContext())
	defer cancel()

	db, err := newYDB(ctx)
	require.NoError(t, err)
	defer db.Close(ctx)

	maxActiveSlots := uint32(1)
	maxBaseDisksInflight := uint32(2)
	maxBaseDiskUnits := uint32(1)
	config := &pools_config.PoolsConfig{
		MaxActiveSlots:       &maxActiveSlots,
		MaxBaseDisksInflight: &maxBaseDisksInflight,
		MaxBaseDiskUnits:     &maxBaseDiskUnits,
	}
	storage := newStorageWithConfig(t, ctx, db, config, metrics.NewEmptyRegistry())

	err = storage.ConfigurePool(ctx, "image", "zone", 2, 0)
	require.NoError(t, err)

	baseDisks, err := storage.TakeBaseDisksToSchedule(ctx)
	require.NoError(t, err)
	require.Equal(t, 2, len(baseDisks))

	for i := 0; i < len(baseDisks); i++ {
		baseDisks[i].CreateTaskID = "create"
	}
	err = storage.BaseDisksScheduled(ctx, baseDisks)
	require.NoError(t, err)

	*config.MaxBaseDisksInflight = uint32(1)
	storage, err = NewStorage(config, db, metrics.NewEmptyRegistry())
	require.NoError(t, err)

	err = storage.ConfigurePool(ctx, "image", "zone", 10, 0)
	require.NoError(t, err)

	baseDisks, err = storage.TakeBaseDisksToSchedule(ctx)
	require.NoError(t, err)
	require.Empty(t, baseDisks)

	err = storage.CheckConsistency(ctx)
	require.NoError(t, err)
}

func TestStorageYDBDeletePoolWhenRetiringIsInFlight(t *testing.T) {
	ctx, cancel := context.WithCancel(newContext())
	defer cancel()

	db, err := newYDB(ctx)
	require.NoError(t, err)
	defer db.Close(ctx)

	storage := newStorage(t, ctx, db)

	err = storage.ConfigurePool(
		ctx,
		"image",
		"zone",
		2*makeDefaultConfig().GetMaxActiveSlots(),
		0,
	)
	require.NoError(t, err)

	baseDisks, err := storage.TakeBaseDisksToSchedule(ctx)
	require.NoError(t, err)
	require.Equal(t, 2, len(baseDisks))

	baseDisks[0].CreateTaskID = "create"
	err = storage.BaseDisksScheduled(ctx, []BaseDisk{baseDisks[0]})
	require.NoError(t, err)
	err = storage.BaseDiskCreated(ctx, baseDisks[0])
	require.NoError(t, err)

	_, err = storage.AcquireBaseDiskSlot(
		ctx,
		"image",
		Slot{
			OverlayDisk: &types.Disk{
				ZoneId: "zone",
				DiskId: "disk",
			},
		},
	)
	require.NoError(t, err)

	rebaseInfos, err := storage.RetireBaseDisk(
		ctx,
		baseDisks[0].ID,
		&types.Disk{
			ZoneId: baseDisks[0].ZoneID,
			DiskId: baseDisks[0].ID,
		},
		0, // useImageSize
	)
	require.NoError(t, err)
	require.Equal(t, 1, len(rebaseInfos))

	targetBaseDiskID := rebaseInfos[0].TargetBaseDiskID
	// None of known base disks should be chosen as target for rebasing.
	require.NotEqual(t, baseDisks[0].ID, targetBaseDiskID)
	require.NotEqual(t, baseDisks[1].ID, targetBaseDiskID)

	err = storage.DeletePool(ctx, "image", "zone")
	require.NoError(t, err)

	// Check that target base disk is still has status 'scheduling'.
	actual, err := storage.TakeBaseDisksToSchedule(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, len(actual))
	require.Equal(t, targetBaseDiskID, actual[0].ID)

	err = storage.CheckConsistency(ctx)
	require.NoError(t, err)
}

func TestStorageYDBBaseDisksShouldHavePrefix(t *testing.T) {
	ctx, cancel := context.WithCancel(newContext())
	defer cancel()

	db, err := newYDB(ctx)
	require.NoError(t, err)
	defer db.Close(ctx)

	prefix := "prefix"
	config := &pools_config.PoolsConfig{
		BaseDiskIdPrefix: &prefix,
	}
	storage := newStorageWithConfig(t, ctx, db, config, metrics.NewEmptyRegistry())

	err = storage.ConfigurePool(ctx, "image", "zone", 1, 0)
	require.NoError(t, err)

	baseDisks, err := storage.TakeBaseDisksToSchedule(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, len(baseDisks))
	require.True(t, strings.HasPrefix(baseDisks[0].ID, prefix))
}
