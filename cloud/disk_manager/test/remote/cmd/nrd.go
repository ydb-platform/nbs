package main

import (
	"context"
	"fmt"

	disk_manager "github.com/ydb-platform/nbs/cloud/disk_manager/api"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nbs"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/types"
)

////////////////////////////////////////////////////////////////////////////////

func (r *testRun) validateNrdDisk(
	ctx context.Context,
	diskID string,
) error {

	params, err := r.nbsClient.Describe(ctx, diskID)
	if err != nil {
		return err
	}

	if params.Kind != types.DiskKind_DISK_KIND_SSD_NONREPLICATED {
		return fmt.Errorf(
			"disk %v has kind %v, expected SSD_NONREPLICATED",
			diskID,
			params.Kind,
		)
	}
	if params.BaseDiskID != "" {
		return fmt.Errorf(
			"NRD disk %v is layered on base disk %v; expected data copied into a standalone NRD",
			diskID,
			params.BaseDiskID,
		)
	}

	if params.CloudID != cloudID || params.FolderID != baseDisksTestsFolderID {
		return fmt.Errorf(
			"disk %v has cloud/folder %v/%v, expected %v/%v",
			diskID,
			params.CloudID,
			params.FolderID,
			cloudID,
			baseDisksTestsFolderID,
		)
	}

	return r.nbsClient.ValidateCrc32(ctx, diskID, nbs.DiskContentInfo{
		ContentSize: r.testConfig.GetImageSize(),
		Crc32:       r.testConfig.GetImageCrc32(),
	})
}

func testCreateNrdFromImageWithInitializedPool(
	ctx context.Context,
	r *testRun,
) (resources, error) {

	imageSize := r.testConfig.GetImageSize()
	if imageSize == 0 || imageSize > nonreplicatedDiskSize {
		return resources{}, fmt.Errorf(
			"NRD test requires a non-empty image fitting in %v bytes, got %v bytes",
			nonreplicatedDiskSize,
			imageSize,
		)
	}

	zoneID := r.testConfig.GetZoneID()
	ssdDiskID := generateID()
	nrdDiskID := generateID()
	imageID := generateID()
	rs := resources{
		ZoneID: zoneID,
		Disks:  []string{ssdDiskID, nrdDiskID},
		Images: []string{imageID},
	}

	err := r.createImageFromURL(
		ctx,
		imageID,
		baseDisksTestsFolderID,
		r.testConfig.GetImageURL(),
	)
	if err != nil {
		return rs, err
	}

	ssdDiskSize := uint64(defaultDiskSize)
	if imageSize > ssdDiskSize {
		ssdDiskSize = imageSize
	}

	// An SSD overlay can only finish creation after its base disk is ready.
	// Keep it alive while creating the NRD to ensure the pool stays initialized.
	err = r.createDiskFromImage(
		ctx,
		zoneID,
		imageID,
		ssdDiskID,
		ssdDiskSize,
		defaultBlockSize,
		disk_manager.DiskKind_DISK_KIND_SSD,
		baseDisksTestsFolderID,
	)
	if err != nil {
		return rs, err
	}

	ssdDisk, err := r.nbsClient.Describe(ctx, ssdDiskID)
	if err != nil {
		return rs, err
	}
	if len(ssdDisk.BaseDiskID) == 0 {
		return rs, fmt.Errorf(
			"SSD disk %v has no base disk; the image pool was not used",
			ssdDiskID,
		)
	}

	err = r.createDiskFromImage(
		ctx,
		zoneID,
		imageID,
		nrdDiskID,
		nonreplicatedDiskSize,
		defaultBlockSize,
		disk_manager.DiskKind_DISK_KIND_SSD_NONREPLICATED,
		baseDisksTestsFolderID,
	)
	if err != nil {
		return rs, err
	}

	err = r.validateNrdDisk(ctx, nrdDiskID)
	if err != nil {
		return rs, err
	}

	return rs, nil
}
