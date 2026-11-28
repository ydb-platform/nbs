package main

import (
	"context"
	"fmt"
	"log"
	"time"

	disk_manager "github.com/ydb-platform/nbs/cloud/disk_manager/api"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nbs"
)

////////////////////////////////////////////////////////////////////////////////

func (r *testRun) waitForImageBaseDisks(
	ctx context.Context,
	imageID string,
	excludedDisks map[string]bool,
	count int,
) (map[string]bool, error) {

	ctx, cancel := context.WithTimeout(ctx, 10*time.Minute)
	defer cancel()

	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		diskIDs, err := r.nbsClient.List(ctx)
		if err != nil {
			return nil, err
		}

		readyDisks := make(map[string]bool)
		for _, diskID := range diskIDs {
			if excludedDisks[diskID] {
				continue
			}

			checkpoints, err := r.nbsClient.GetCheckpoints(ctx, diskID)
			if nbs.IsNotFoundError(err) {
				// Another test may delete a disk after List returns it.
				continue
			}
			if err != nil {
				return nil, err
			}

			for _, checkpointID := range checkpoints {
				// Every base disk has a checkpoint named after its image.
				if checkpointID != imageID {
					continue
				}

				status, err := r.nbsClient.GetCheckpointStatus(
					ctx,
					diskID,
					imageID,
				)
				if nbs.IsNotFoundError(err) {
					break
				}
				if err != nil {
					return nil, err
				}
				if status == nbs.CheckpointStatusError {
					return nil, fmt.Errorf(
						"image %v checkpoint on base disk %v is broken",
						imageID,
						diskID,
					)
				}
				if status != nbs.CheckpointStatusReady {
					break
				}

				readyDisks[diskID] = true
				break
			}
		}

		if len(readyDisks) >= count {
			return readyDisks, nil
		}

		select {
		case <-ctx.Done():
			return nil, fmt.Errorf(
				"waiting for %v ready base disks for image %v, found %v: %w",
				count,
				imageID,
				len(readyDisks),
				ctx.Err(),
			)
		case <-ticker.C:
		}
	}
}

// Requires UseReadyBaseDiskAsSrc and HoldBaseDisksWithInflightDependents on
// the service. With those options, replenishment uses a ready base disk as its
// source. NBS exposes the resulting data and checkpoint, but not copy provenance.
func testCreateBaseDiskFromBaseDisk(
	ctx context.Context,
	r *testRun,
) (resources, error) {

	const (
		poolCapacity     = 31
		initialBaseCount = 2
		disksPerBase     = 2
		overlayDiskSize  = uint64(4 << 40)
	)

	zoneID := r.testConfig.GetZoneID()
	imageID := generateID()
	rs := resources{
		ZoneID: zoneID,
		Images: []string{imageID},
	}

	diskIDs, err := r.nbsClient.List(ctx)
	if err != nil {
		return rs, err
	}
	excludedDisks := make(map[string]bool)
	for _, diskID := range diskIDs {
		excludedDisks[diskID] = true
	}

	err = r.createImageFromURL(
		ctx,
		imageID,
		baseDisksTestsFolderID,
		r.testConfig.GetImageURL(),
	)
	if err != nil {
		return rs, err
	}

	err = r.configurePool(ctx, zoneID, imageID, poolCapacity, true)
	if err != nil {
		return rs, err
	}

	initialBases, err := r.waitForImageBaseDisks(
		ctx,
		imageID,
		excludedDisks,
		initialBaseCount,
	)
	if err != nil {
		return rs, err
	}
	if len(initialBases) != initialBaseCount {
		return rs, fmt.Errorf(
			"pool with capacity %v should start with %v base disks, got %v",
			poolCapacity,
			initialBaseCount,
			len(initialBases),
		)
	}
	for diskID := range initialBases {
		excludedDisks[diskID] = true
	}

	// 4 TiB is the largest SSD that can use a base disk. It consumes 22
	// pool units, so two such SSDs exhaust a 30-unit base. Stop as soon as
	// one base is exhausted, leaving another ready base with free capacity
	// for replenishment.
	disksByBase := make(map[string]int)
	maxDiskCount := len(initialBases)*(disksPerBase-1) + 1
	for i := 0; i < maxDiskCount; i++ {
		diskID := generateID()
		rs.Disks = append(rs.Disks, diskID)
		err = r.createDiskFromImage(
			ctx,
			zoneID,
			imageID,
			diskID,
			overlayDiskSize,
			defaultBlockSize,
			disk_manager.DiskKind_DISK_KIND_SSD,
			baseDisksTestsFolderID,
		)
		if err != nil {
			return rs, err
		}

		disk, err := r.nbsClient.Describe(ctx, diskID)
		if err != nil {
			return rs, err
		}
		if _, ok := initialBases[disk.BaseDiskID]; !ok {
			return rs, fmt.Errorf(
				"disk %v should use an initial base disk, got %q",
				diskID,
				disk.BaseDiskID,
			)
		}
		excludedDisks[diskID] = true
		disksByBase[disk.BaseDiskID]++
		if disksByBase[disk.BaseDiskID] == disksPerBase {
			log.Printf("Created %v large SSDs on base disk %v", disksPerBase, disk.BaseDiskID)
			break
		}
	}

	newBases, err := r.waitForImageBaseDisks(ctx, imageID, excludedDisks, 1)
	if err != nil {
		return rs, err
	}
	for diskID := range newBases {
		err = r.nbsClient.ValidateCrc32(ctx, diskID, nbs.DiskContentInfo{
			ContentSize: r.testConfig.GetImageSize(),
			Crc32:       r.testConfig.GetImageCrc32(),
		})
		if err != nil {
			return rs, err
		}
		log.Printf("Validated replenished base disk %v for image %v", diskID, imageID)
	}

	return rs, nil
}
