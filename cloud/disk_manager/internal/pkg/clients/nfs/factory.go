package nfs

import (
	"context"
	"fmt"
	"time"

	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/auth"
	nfs_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nfs/config"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/monitoring/metrics"
	nfs_client "github.com/ydb-platform/nbs/cloud/filestore/public/sdk/go/client"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/logging"
	"golang.org/x/exp/maps"
)

////////////////////////////////////////////////////////////////////////////////

type errorLogger struct {
}

func (l *errorLogger) Print(ctx context.Context, v ...interface{}) {
	ctx = logging.AddCallerSkip(ctx, 1)
	logging.Error(ctx, fmt.Sprint(v...))
}

func (l *errorLogger) Printf(ctx context.Context, format string, v ...interface{}) {
	ctx = logging.AddCallerSkip(ctx, 1)
	logging.Error(ctx, fmt.Sprintf(format, v...))
}

////////////////////////////////////////////////////////////////////////////////

type warnLogger struct {
}

func (l *warnLogger) Print(ctx context.Context, v ...interface{}) {
	ctx = logging.AddCallerSkip(ctx, 1)
	logging.Warn(ctx, fmt.Sprint(v...))
}

func (l *warnLogger) Printf(ctx context.Context, format string, v ...interface{}) {
	ctx = logging.AddCallerSkip(ctx, 1)
	logging.Warn(ctx, fmt.Sprintf(format, v...))
}

////////////////////////////////////////////////////////////////////////////////

type infoLogger struct {
}

func (l *infoLogger) Print(ctx context.Context, v ...interface{}) {
	ctx = logging.AddCallerSkip(ctx, 1)
	logging.Info(ctx, fmt.Sprint(v...))
}

func (l *infoLogger) Printf(ctx context.Context, format string, v ...interface{}) {
	ctx = logging.AddCallerSkip(ctx, 1)
	logging.Info(ctx, fmt.Sprintf(format, v...))
}

////////////////////////////////////////////////////////////////////////////////

type debugLogger struct {
}

func (l *debugLogger) Print(ctx context.Context, v ...interface{}) {
	ctx = logging.AddCallerSkip(ctx, 1)
	logging.Debug(ctx, fmt.Sprint(v...))
}

func (l *debugLogger) Printf(ctx context.Context, format string, v ...interface{}) {
	ctx = logging.AddCallerSkip(ctx, 1)
	logging.Debug(ctx, fmt.Sprintf(format, v...))
}

////////////////////////////////////////////////////////////////////////////////

type nfsClientLogWrapper struct {
	level   nfs_client.LogLevel
	loggers []nfs_client.Logger
}

func (w *nfsClientLogWrapper) Logger(level nfs_client.LogLevel) nfs_client.Logger {
	if level <= w.level {
		return w.loggers[level]
	}
	return nil
}

func NewNfsClientLog(level nfs_client.LogLevel) nfs_client.Log {
	loggers := []nfs_client.Logger{
		&errorLogger{},
		&warnLogger{},
		&infoLogger{},
		&debugLogger{},
	}
	return &nfsClientLogWrapper{level, loggers}
}

////////////////////////////////////////////////////////////////////////////////

type factory struct {
	config            *nfs_config.ClientConfig
	clientCredentials *nfs_client.ClientCredentials
	metrics           clientMetrics
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
				f.metrics.OnError(err)
			},
		},
		NewNfsClientLog(nfs_client.LOG_DEBUG),
	)
	if err != nil {
		return nil, errors.NewRetriableError(err)
	}

	return &client{
		nfs:     nfs,
		metrics: f.metrics,
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
	metricsRegistry metrics.Registry,
) Factory {

	clientMetrics := clientMetrics{
		registry: metricsRegistry,
		errors:   metricsRegistry.Counter("errors"),
	}

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
		config:            config,
		clientCredentials: clientCredentials,
		metrics:           clientMetrics,
		endpointPickers:   endpointPickers,
	}
}

func NewFactory(
	ctx context.Context,
	config *nfs_config.ClientConfig,
	metricsRegistry metrics.Registry,
) Factory {

	return NewFactoryWithCreds(ctx, config, nil, metricsRegistry)
}
