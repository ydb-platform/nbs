package health

import (
	"context"

	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/logging"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/persistence"
)

////////////////////////////////////////////////////////////////////////////////

type ydbCheck struct {
	client *persistence.YDBClient
}

func newYDBCheck(client *persistence.YDBClient) *ydbCheck {
	return &ydbCheck{
		client: client,
	}
}

func (y ydbCheck) Check(ctx context.Context) bool {
	res, err := y.client.ExecuteRO(ctx, "SELECT 1", nil)
	if err != nil {
		logging.Warn(ctx, "YDB health check failed: %v", err)
		return false
	}

	defer res.Close()
	return true
}
