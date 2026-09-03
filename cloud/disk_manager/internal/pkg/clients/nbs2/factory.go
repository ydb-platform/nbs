package nbs2

import (
	"context"
	"fmt"
	"time"

	nbs2_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nbs2/config"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	"golang.org/x/exp/maps"
)

////////////////////////////////////////////////////////////////////////////////

type factory struct {
	config  *nbs2_config.ClientConfig
	timeout time.Duration
}

func (f *factory) GetClient(
	ctx context.Context,
	zoneID string,
) (Client, error) {

	if f.config == nil {
		return nil, errors.NewNonRetriableErrorf(
			"nbs2 client is not configured, available zones: []",
		)
	}

	zone, ok := f.config.GetZones()[zoneID]
	if !ok {
		return nil, errors.NewNonRetriableErrorf(
			"unknown nbs2 zone %q, available zones: %q",
			zoneID,
			maps.Keys(f.config.GetZones()),
		)
	}
	if len(zone.GetEndpoints()) == 0 {
		return nil, errors.NewNonRetriableErrorf(
			"no nbs2 endpoints for zone %q",
			zoneID,
		)
	}

	return &client{
		endpoint: zone.GetEndpoints()[0],
		timeout:  f.timeout,
	}, nil
}

////////////////////////////////////////////////////////////////////////////////

func NewFactory(config *nbs2_config.ClientConfig) (Factory, error) {
	timeout := 20 * time.Second
	if config != nil && len(config.GetRequestTimeout()) > 0 {
		parsed, err := time.ParseDuration(config.GetRequestTimeout())
		if err != nil {
			return nil, fmt.Errorf("invalid nbs2 request timeout: %w", err)
		}
		timeout = parsed
	}

	return &factory{
		config:  config,
		timeout: timeout,
	}, nil
}
