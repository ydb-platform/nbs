package health

import (
	"context"

	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nbs"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/tasks/logging"
)

////////////////////////////////////////////////////////////////////////////////

type nbsCheck struct {
	nbsFactory nbs.Factory
	zone       string
}

func newNbsCheck(nbsFactory nbs.Factory, zone string) *nbsCheck {
	return &nbsCheck{nbsFactory: nbsFactory, zone: zone}
}

func (c *nbsCheck) Check(ctx context.Context) bool {
	client, err := c.nbsFactory.GetClient(ctx, c.zone)
	if err != nil {
		logging.Warn(ctx, "NBS health check failed to create client for zone %v: %v", c.zone, err)
		return false
	}

	err = client.Ping(ctx)
	if err != nil {
		logging.Warn(ctx, "NBS health check failed to ping nbs in zone %v: %v", c.zone, err)
		return false
	}

	return true
}
