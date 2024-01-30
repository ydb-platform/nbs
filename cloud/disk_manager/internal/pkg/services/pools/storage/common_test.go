package storage

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/types"
)

////////////////////////////////////////////////////////////////////////////////

func TestCommonGenerateBaseDiskForPool(t *testing.T) {
	maxActiveSlots := uint64(640)
	maxBaseDiskUnits := uint64(640)

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

		if imageSize != 0 && srcDisk != nil {
			require.Equal(t, "src_disk_zone", actual.srcDiskZoneID)
			require.Equal(t, "src_disk", actual.srcDiskID)
			require.Equal(t, "image", actual.srcDiskCheckpointID)
		}
	}

	generate := func(requiredSize uint64) baseDisk {
		storage := &storageYDB{
			maxActiveSlots:   maxActiveSlots,
			maxBaseDiskUnits: maxBaseDiskUnits,
		}

		imageSize := requiredSize
		var srcDisk *types.Disk

		baseDisk := storage.generateBaseDisk(
			"image",
			"zone",
			imageSize,
			srcDisk,
		)
		check(baseDisk, imageSize, srcDisk)

		srcDisk = &types.Disk{ZoneId: "src_disk_zone", DiskId: "src_disk"}
		baseDisk = storage.generateBaseDisk(
			"image",
			"zone",
			imageSize,
			srcDisk,
		)
		check(baseDisk, imageSize, srcDisk)
		return baseDisk
	}

	baseDisk := generate(0)
	require.Equal(t, uint64(0), baseDisk.size)
	require.Equal(t, uint64(640), baseDisk.maxActiveSlots)
	require.Equal(t, uint64(640), baseDisk.units)

	baseDisk = generate(1)
	require.Equal(t, uint64(32<<30), baseDisk.size)
	require.Equal(t, uint64(30), baseDisk.maxActiveSlots)
	require.Equal(t, uint64(30), baseDisk.units)

	baseDisk = generate(32 << 30)
	require.Equal(t, uint64(32<<30), baseDisk.size)
	require.Equal(t, uint64(30), baseDisk.maxActiveSlots)
	require.Equal(t, uint64(30), baseDisk.units)

	baseDisk = generate((32 << 30) + 1)
	require.Equal(t, uint64(64<<30), baseDisk.size)
	require.Equal(t, uint64(30), baseDisk.maxActiveSlots)
	require.Equal(t, uint64(30), baseDisk.units)

	baseDisk = generate(192 << 30)
	require.Equal(t, uint64(192<<30), baseDisk.size)
	require.Equal(t, uint64(60), baseDisk.maxActiveSlots)
	require.Equal(t, uint64(60), baseDisk.units)

	baseDisk = generate((192 << 30) + 1)
	require.Equal(t, uint64(224<<30), baseDisk.size)
	require.Equal(t, uint64(70), baseDisk.maxActiveSlots)
	require.Equal(t, uint64(70), baseDisk.units)

	baseDisk = generate(1024 << 30)
	require.Equal(t, uint64(1024<<30), baseDisk.size)
	require.Equal(t, uint64(320), baseDisk.maxActiveSlots)
	require.Equal(t, uint64(320), baseDisk.units)

	baseDisk = generate(2048 << 30)
	require.Equal(t, uint64(2048<<30), baseDisk.size)
	require.Equal(t, uint64(640), baseDisk.maxActiveSlots)
	require.Equal(t, uint64(640), baseDisk.units)

	baseDisk = generate(4096 << 30)
	require.Equal(t, uint64(4096<<30), baseDisk.size)
	require.Equal(t, uint64(640), baseDisk.maxActiveSlots)
	require.Equal(t, uint64(640), baseDisk.units)

	maxActiveSlots = 1
	baseDisk = generate(4096 << 30)
	require.Equal(t, uint64(4096<<30), baseDisk.size)
	require.Equal(t, uint64(1), baseDisk.maxActiveSlots)
	require.Equal(t, uint64(640), baseDisk.units)

	maxBaseDiskUnits = 1
	baseDisk = generate(4096 << 30)
	require.Equal(t, uint64(4096<<30), baseDisk.size)
	require.Equal(t, uint64(1), baseDisk.maxActiveSlots)
	require.Equal(t, uint64(1), baseDisk.units)
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
