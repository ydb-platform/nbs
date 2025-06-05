package cells

import (
	"context"

	disk_manager "github.com/ydb-platform/nbs/cloud/disk_manager/api"
)

////////////////////////////////////////////////////////////////////////////////

type CellSelector interface {
	PickCell(
		ctx context.Context,
		disk *disk_manager.DiskId,
	) string
}
