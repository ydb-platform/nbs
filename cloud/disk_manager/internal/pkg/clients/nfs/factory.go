package nfs

import (
	"context"
	"time"

	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/auth"
	client_metrics "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/metrics"
	nfs_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nfs/config"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/monitoring/metrics"
	nfs_client "github.com/ydb-platform/nbs/cloud/filestore/public/sdk/go/client"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/logging"
	"golang.org/x/exp/maps"
)

////////////////////////////////////////////////////////////////////////////////

type factory struct {
	config                 *nfs_config.ClientConfig
	clientCredentials      *nfs_client.ClientCredentials
	metrics                client_metrics.Metrics
	sessionMetricsRegistry metrics.Registry
	// map from zone
	endpointPickers map[string]*endpointPicker
}

func (f *factory) NewClient(
	ctx context.Context,
	zoneID string,
) (Client, error) {

	zone, ok := f.config.GetZones()[zoneID]
	if !ok {
		return nil, errors.NewNonRetriableErrorf(
			"unknown zone %q, available zones: %q",
			zoneID,
			maps.Keys(f.config.GetZones()),
		)
	}

	if len(zone.Endpoints) == 0 {
		return nil, errors.NewNonRetriableErrorf(
			"no endpoints for zone %q",
			zoneID,
		)
	}

	durableClientTimeout, err := time.ParseDuration(
		f.config.GetDurableClientTimeout(),
	)
	if err != nil {
		return nil, err
	}
	grpcClientTimeout, err := time.ParseDuration(
		f.config.GetGrpcClientTimeout(),
	)
	if err != nil {
		return nil, err
	}

	endpoint, err := f.endpointPickers[zoneID].pickEndpoint(ctx)
	if err != nil {
		return nil, err
	}

	nfs, err := nfs_client.NewClient(
		&nfs_client.GrpcClientOpts{
			Endpoint:    endpoint,
			Credentials: f.clientCredentials,
			Timeout:     &grpcClientTimeout,
		},
		&nfs_client.DurableClientOpts{
			Timeout: &durableClientTimeout,
			OnError: func(err nfs_client.ClientError) {
				f.metrics.OnError(&err)
			},
		},
		logging.GetLogger(ctx),
	)
	if err != nil {
		return nil, errors.NewRetriableError(err)
	}

	return &client{
		nfs:                    nfs,
		metrics:                f.metrics,
		sessionMetricsRegistry: f.sessionMetricsRegistry,
		zoneID:                 zoneID,
	}, nil
}

func (f *factory) NewClientFromDefaultZone(
	ctx context.Context,
) (Client, error) {

	var zoneID string

	for id := range f.config.GetZones() {
		zoneID = id
		// It's OK to use first known zone.
		break
	}

	return f.NewClient(ctx, zoneID)
}

////////////////////////////////////////////////////////////////////////////////

func NewFactoryWithCreds(
	ctx context.Context,
	config *nfs_config.ClientConfig,
	credentials auth.Credentials,
	clientMetricsRegistry metrics.Registry,
	sessionMetricsRegistry metrics.Registry,
) Factory {

	clientMetrics := client_metrics.NewClientMetrics(clientMetricsRegistry)
	if config.GetDisableAuthentication() {
		credentials = nil
	}
	clientCredentials := &nfs_client.ClientCredentials{
		RootCertsFile: config.GetRootCertsFile(),
		IAMClient:     credentials,
	}
	if config.GetInsecure() {
		clientCredentials = nil
	}

	endpointPickers := make(map[string]*endpointPicker)
	for zoneID, zone := range config.GetZones() {
		endpointPickers[zoneID] = newEndpointPicker(
			ctx,
			clientCredentials,
			zone.GetEndpoints(),
		)
	}

	return &factory{
		config:                 config,
		clientCredentials:      clientCredentials,
		metrics:                clientMetrics,
		sessionMetricsRegistry: sessionMetricsRegistry,
		endpointPickers:        endpointPickers,
	}
}

func NewFactory(
	ctx context.Context,
	config *nfs_config.ClientConfig,
	clientMetricsRegistry metrics.Registry,
	sessionMetricsRegistry metrics.Registry,
) Factory {

	return NewFactoryWithCreds(ctx, config, nil, clientMetricsRegistry, sessionMetricsRegistry)
}
