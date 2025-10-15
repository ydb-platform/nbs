package common

import (
	"context"

	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/cells"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/resources"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/types"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
)

////////////////////////////////////////////////////////////////////////////////

func GetDiskCell(
	ctx context.Context,
	storage resources.Storage,
	cellSelector cells.CellSelector,
	disk *types.Disk,
) (*types.Disk, error) {

	diskMeta, err := storage.GetDiskMeta(ctx, disk.DiskId)
	if err != nil {
		return nil, err
	}

	if !cellSelector.IsCellOfZone(diskMeta.ZoneID, disk.ZoneId) {
		return nil, errors.NewNonCancellableErrorf(
			"disk %s is not in zone %s",
			disk.DiskId,
			disk.ZoneId,
		)
	}

	return &types.Disk{
		DiskId: disk.DiskId,
		ZoneId: diskMeta.ZoneID,
	}, nil
}
