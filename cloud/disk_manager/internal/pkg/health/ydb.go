package health

import (
	"context"

	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/persistence"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/tasks/logging"
)

////////////////////////////////////////////////////////////////////////////////

type ydbCheck struct {
	db *persistence.YDBClient
}

func newYDBCheck(db *persistence.YDBClient) *ydbCheck {
	return &ydbCheck{
		db: db,
	}
}

func (c ydbCheck) Check(ctx context.Context) bool {
	res, err := c.db.ExecuteRO(ctx, "SELECT 1")
	if err != nil {
		logging.Warn(ctx, "YDB health check failed: %v", err)
		return false
	}
	defer res.Close()

	return true
}
