package cells

import (
	"context"

	disk_manager "github.com/ydb-platform/nbs/cloud/disk_manager/api"
)

////////////////////////////////////////////////////////////////////////////////

type CellSelector interface {
	SelectCell(
		ctx context.Context,
		req *disk_manager.CreateDiskRequest,
	) string
}
