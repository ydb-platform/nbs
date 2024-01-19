package storage

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/types"
)

////////////////////////////////////////////////////////////////////////////////

func TestCommonGenerateBaseDiskForPool(t *testing.T) {
	check := func(
		actual baseDisk,
		imageSize uint64,
		srcDisk *types.Disk,
	) {

		require.NotEmpty(t, actual.id)
		require.Equal(t, "image", actual.imageID)
		require.Equal(t, "zone", actual.zoneID)
		// Note: we use image id as checkpoint id.
		require.Equal(t, "image", actual.checkpointID)
		require.Empty(t, actual.createTaskID)
		require.Equal(t, imageSize, actual.imageSize)
		require.True(t, actual.fromPool)
		require.Equal(t, baseDiskStatusScheduling, actual.status)

		if srcDisk != nil {
			require.Equal(t, srcDisk.ZoneId, actual.srcDiskZoneID)
			require.Equal(t, srcDisk.DiskId, actual.srcDiskID)
			require.Equal(t, "image", actual.srcDiskCheckpointID)
		}
	}

	storage := &storageYDB{}

	imageSize := uint64(1024)

	baseDisk := storage.generateBaseDisk(
		"image",
		"zone",
		imageSize,
		nil,
	)
	check(baseDisk, imageSize, nil)

	srcDisk := &types.Disk{ZoneId: "src_disk_zone", DiskId: "src_disk"}
	baseDisk = storage.generateBaseDisk(
		"image",
		"zone",
		imageSize,
		srcDisk,
	)
	check(baseDisk, imageSize, srcDisk)
}

func TestCommonFreeSlots(t *testing.T) {
	baseDisk := &baseDisk{
		maxActiveSlots: 10,
		units:          5,
		status:         baseDiskStatusDeleting,
	}
	require.Equal(t, uint64(0), baseDisk.freeSlots())

	baseDisk.status = baseDiskStatusReady
	require.Equal(t, uint64(0), baseDisk.freeSlots())

	baseDisk.fromPool = true
	require.Equal(t, uint64(10), baseDisk.freeSlots())

	baseDisk.activeUnits = 4
	require.Equal(t, uint64(10), baseDisk.freeSlots())

	baseDisk.activeUnits = 5
	require.Equal(t, uint64(0), baseDisk.freeSlots())

	baseDisk.activeUnits = 6
	require.Equal(t, uint64(0), baseDisk.freeSlots())
	baseDisk.activeUnits = 1

	baseDisk.units = 0
	require.Equal(t, uint64(10), baseDisk.freeSlots())
	baseDisk.units = 5

	baseDisk.activeSlots = 1
	require.Equal(t, uint64(9), baseDisk.freeSlots())

	baseDisk.activeSlots = 11
	require.Equal(t, uint64(0), baseDisk.freeSlots())
}

func TestCommonAcquireAndReleaseUnitsAndSlots(t *testing.T) {
	ctx := newContext()

	baseDisk := &baseDisk{
		maxActiveSlots: 4,
		units:          6,
		fromPool:       true,
		status:         baseDiskStatusScheduling,
	}

	slot1 := &slot{}

	err := acquireUnitsAndSlots(ctx, nil, baseDisk, slot1)
	require.NoError(t, err)
	require.Equal(t, uint64(1), baseDisk.activeSlots)
	require.Equal(t, uint64(1), baseDisk.activeUnits)
	require.Equal(t, uint64(3), baseDisk.freeSlots())
	require.True(t, baseDisk.hasFreeSlots())
	require.Equal(t, uint64(1), slot1.allottedSlots)
	require.Equal(t, uint64(1), slot1.allottedUnits)

	hddUnit := overlayDiskOversubscription * baseDiskUnitSize
	ssdUnit := hddUnit / ssdUnitMultiplier

	slot2 := &slot{}
	slot2.overlayDiskKind = types.DiskKind_DISK_KIND_SSD
	slot2.overlayDiskSize = 3 * ssdUnit

	err = acquireUnitsAndSlots(ctx, nil, baseDisk, slot2)
	require.NoError(t, err)
	require.Equal(t, uint64(2), baseDisk.activeSlots)
	require.Equal(t, uint64(4), baseDisk.activeUnits)
	require.Equal(t, uint64(2), baseDisk.freeSlots())
	require.True(t, baseDisk.hasFreeSlots())
	require.Equal(t, uint64(1), slot2.allottedSlots)
	require.Equal(t, uint64(3), slot2.allottedUnits)

	slot3 := &slot{}
	slot3.overlayDiskKind = types.DiskKind_DISK_KIND_HDD
	slot3.overlayDiskSize = 3 * hddUnit

	err = acquireUnitsAndSlots(ctx, nil, baseDisk, slot3)
	require.NoError(t, err)
	require.Equal(t, uint64(3), baseDisk.activeSlots)
	require.Equal(t, uint64(7), baseDisk.activeUnits)
	require.Equal(t, uint64(0), baseDisk.freeSlots())
	require.False(t, baseDisk.hasFreeSlots())
	require.Equal(t, uint64(1), slot3.allottedSlots)
	require.Equal(t, uint64(3), slot3.allottedUnits)

	err = releaseUnitsAndSlots(ctx, nil, baseDisk, *slot1)
	require.NoError(t, err)
	require.Equal(t, uint64(2), baseDisk.activeSlots)
	require.Equal(t, uint64(6), baseDisk.activeUnits)
	require.Equal(t, uint64(0), baseDisk.freeSlots())
	require.False(t, baseDisk.hasFreeSlots())

	err = releaseUnitsAndSlots(ctx, nil, baseDisk, *slot2)
	require.NoError(t, err)
	require.Equal(t, uint64(1), baseDisk.activeSlots)
	require.Equal(t, uint64(3), baseDisk.activeUnits)
	require.Equal(t, uint64(3), baseDisk.freeSlots())
	require.True(t, baseDisk.hasFreeSlots())

	err = releaseUnitsAndSlots(ctx, nil, baseDisk, *slot3)
	require.NoError(t, err)
	require.Equal(t, uint64(0), baseDisk.activeSlots)
	require.Equal(t, uint64(0), baseDisk.activeUnits)
	require.Equal(t, uint64(4), baseDisk.freeSlots())
	require.True(t, baseDisk.hasFreeSlots())
}
