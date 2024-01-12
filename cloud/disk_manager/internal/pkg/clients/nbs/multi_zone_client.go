package nbs

import (
	"context"

	"github.com/ydb-platform/nbs/cloud/blockstore/public/api/protos"
	nbs_client "github.com/ydb-platform/nbs/cloud/blockstore/public/sdk/go/client"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/tasks/logging"
)

////////////////////////////////////////////////////////////////////////////////

type multiZoneClient struct {
	srcZoneClient *client
	dstZoneClient *client
	metrics       *clientMetrics
}

////////////////////////////////////////////////////////////////////////////////

func (c *multiZoneClient) Clone(
	ctx context.Context,
	diskID string,
	dstPlacementGroupID string,
	dstPlacementPartitionIndex uint32,
	fillGeneration uint64,
	baseDiskID string,
) (err error) {

	defer c.metrics.StatRequest("Clone")(&err)

	err = c.clone(
		ctx,
		diskID,
		dstPlacementGroupID,
		dstPlacementPartitionIndex,
		fillGeneration,
		baseDiskID,
	)
	if err != nil {
		if isAbortedError(err) {
			logging.Error(
				ctx,
				"src disk cloning failed because there exists dst disk with outdated fill generation: %v",
				err,
			)

			err = c.deleteOutdatedDstDisk(ctx, diskID, fillGeneration)
			if err != nil {
				return err
			}

			return errors.NewRetriableErrorf(
				"retry src disk cloning after deleting dst disk with outdated fill generation",
			)
		}

		return err
	}

	return nil
}

////////////////////////////////////////////////////////////////////////////////

func (c *multiZoneClient) deleteOutdatedDstDisk(
	ctx context.Context,
	diskID string,
	fillGeneration uint64,
) error {

	if fillGeneration > 1 {
		volume, err := c.dstZoneClient.nbs.DescribeVolume(ctx, diskID)
		if IsNotFoundError(err) {
			return nil
		}

		if err != nil {
			return wrapError(err)
		}

		if volume.IsFillFinished {
			return errors.NewNonRetriableErrorf(
				"can't delete dst disk %v because filling is finished",
				diskID,
			)
		}

		return c.dstZoneClient.DeleteWithFillGeneration(
			ctx,
			diskID,
			fillGeneration-1,
		)
	}

	return nil
}

func (c *multiZoneClient) clone(
	ctx context.Context,
	diskID string,
	dstPlacementGroupID string,
	dstPlacementPartitionIndex uint32,
	fillGeneration uint64,
	baseDiskID string,
) (err error) {

	volume, err := c.srcZoneClient.nbs.DescribeVolume(ctx, diskID)
	if err != nil {
		return wrapError(err)
	}

	err = c.dstZoneClient.nbs.CreateVolume(
		ctx,
		volume.DiskId,
		volume.BlocksCount,
		&nbs_client.CreateVolumeOpts{
			BaseDiskId:              baseDiskID,
			BaseDiskCheckpointId:    volume.BaseDiskCheckpointId,
			BlockSize:               volume.BlockSize,
			StorageMediaKind:        volume.StorageMediaKind,
			CloudId:                 volume.CloudId,
			FolderId:                volume.FolderId,
			TabletVersion:           volume.TabletVersion,
			PlacementGroupId:        dstPlacementGroupID,
			PlacementPartitionIndex: dstPlacementPartitionIndex,
			PartitionsCount:         volume.PartitionsCount,
			IsSystem:                volume.IsSystem,
			ProjectId:               volume.ProjectId,
			ChannelsCount:           volume.ChannelsCount,
			EncryptionSpec: &protos.TEncryptionSpec{
				Mode: volume.EncryptionDesc.Mode,
				KeyParam: &protos.TEncryptionSpec_KeyHash{
					KeyHash: volume.EncryptionDesc.KeyHash,
				},
			},
			// TODO: NBS-3679: provide these parameters correctly
			StoragePoolName: "",
			AgentIds:        []string{},
			FillGeneration:  fillGeneration,
		},
	)
	if IsNotFoundError(err) {
		return errors.NewRetriableErrorf(
			"retry src disk cloning because dst disk is not found, it might be deleted because its fill generation is outdated",
		)
	}

	return wrapError(err)
}
