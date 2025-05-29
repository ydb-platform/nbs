package nbs

import (
	"context"
	"fmt"
	"time"

	nbs_client "github.com/ydb-platform/nbs/cloud/blockstore/public/sdk/go/client"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/auth"
	nbs_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nbs/config"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/monitoring/metrics"
	coreprotos "github.com/ydb-platform/nbs/cloud/storage/core/protos"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/logging"
	"github.com/ydb-platform/nbs/cloud/tasks/tracing"
	"golang.org/x/exp/maps"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
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

type nbsClientLogWrapper struct {
	level   nbs_client.LogLevel
	loggers []nbs_client.Logger
}

func (w *nbsClientLogWrapper) Logger(level nbs_client.LogLevel) nbs_client.Logger {
	if level <= w.level {
		return w.loggers[level]
	}

	return nil
}

func NewNbsClientLog(level nbs_client.LogLevel) nbs_client.Log {
	loggers := []nbs_client.Logger{
		&errorLogger{},
		&warnLogger{},
		&infoLogger{},
		&debugLogger{},
	}
	return &nbsClientLogWrapper{level, loggers}
}

////////////////////////////////////////////////////////////////////////////////

type factory struct {
	config                 *nbs_config.ClientConfig
	credentials            auth.Credentials
	sessionMetricsRegistry metrics.Registry
	metrics                *clientMetrics
	clients                map[string]client
	multiZoneClients       map[[2]string]multiZoneClient
}

func (f *factory) initClients(
	ctx context.Context,
) error {

	f.clients = make(map[string]client)

	dialer := grpc.DialContext

	if f.config.GetGrpcKeepAlive() != nil {
		keepAliveTime, err := time.ParseDuration(
			f.config.GetGrpcKeepAlive().GetTime(),
		)
		if err != nil {
			return err
		}

		keepAliveTimeout, err := time.ParseDuration(
			f.config.GetGrpcKeepAlive().GetTimeout(),
		)
		if err != nil {
			return err
		}

		dialer = func(
			ctx context.Context,
			target string,
			opts ...grpc.DialOption,
		) (conn *grpc.ClientConn, err error) {
			ka := keepalive.ClientParameters{
				Time:                keepAliveTime,
				Timeout:             keepAliveTimeout,
				PermitWithoutStream: f.config.GetGrpcKeepAlive().GetPermitWithoutStream(),
			}
			opts = append(opts, grpc.WithKeepaliveParams(ka))
			return grpc.DialContext(ctx, target, opts...)
		}
	}

	durableClientTimeout, err := time.ParseDuration(
		f.config.GetDurableClientTimeout(),
	)
	if err != nil {
		return err
	}

	discoveryClientHardTimeout, err := time.ParseDuration(
		f.config.GetDiscoveryClientHardTimeout(),
	)
	if err != nil {
		return err
	}

	discoveryClientSoftTimeout, err := time.ParseDuration(
		f.config.GetDiscoveryClientSoftTimeout(),
	)
	if err != nil {
		return err
	}

	sessionRediscoverPeriodMin, err := time.ParseDuration(
		f.config.GetSessionRediscoverPeriodMin(),
	)
	if err != nil {
		return err
	}

	sessionRediscoverPeriodMax, err := time.ParseDuration(
		f.config.GetSessionRediscoverPeriodMax(),
	)
	if err != nil {
		return err
	}

	requestTimeout, err := time.ParseDuration(
		f.config.GetServerRequestTimeout(),
	)
	if err != nil {
		return err
	}

	for zoneID, zone := range f.config.GetZones() {
		clientCreds := &nbs_client.ClientCredentials{
			RootCertsFile: f.config.GetRootCertsFile(),
			IAMClient:     f.credentials,
		}

		if f.config.GetInsecure() {
			clientCreds = nil
		}

		nbs, err := nbs_client.NewDiscoveryClient(
			zone.Endpoints,
			&nbs_client.GrpcClientOpts{
				Credentials:        clientCreds,
				DialContext:        dialer,
				UseGZIPCompression: f.config.GetUseGZIPCompression(),
			},
			&nbs_client.DurableClientOpts{
				Timeout: &durableClientTimeout,
				OnError: func(ctx context.Context, err nbs_client.ClientError) {
					tracing.SpanFromContext(ctx).AddEvent(
						"Retriable error in blockstore SDK",
						tracing.WithAttributes(
							tracing.AttributeError(&err),
						),
					)
					f.metrics.OnError(err)
				},
			},
			&nbs_client.DiscoveryClientOpts{
				HardTimeout: discoveryClientHardTimeout,
				SoftTimeout: discoveryClientSoftTimeout,
			},
			NewNbsClientLog(nbs_client.LOG_DEBUG),
		)
		if err != nil {
			return err
		}

		var enableThrottlingForMediaKinds []coreprotos.EStorageMediaKind
		for _, mediaKindStr := range f.config.EnableThrottlingForMediaKinds {
			mediaKind, ok := coreprotos.EStorageMediaKind_value[mediaKindStr]
			if !ok {
				return errors.NewNonRetriableErrorf(
					"non existent media kind %q in EnableThrottlingForMediaKinds config",
					mediaKindStr,
				)
			}

			enableThrottlingForMediaKinds = append(
				enableThrottlingForMediaKinds,
				coreprotos.EStorageMediaKind(mediaKind),
			)
		}

		f.clients[zoneID] = client{
			nbs:                           nbs,
			sessionMetricsRegistry:        f.sessionMetricsRegistry,
			metrics:                       f.metrics,
			enableThrottlingForMediaKinds: enableThrottlingForMediaKinds,
			sessionRediscoverPeriodMin:    sessionRediscoverPeriodMin,
			sessionRediscoverPeriodMax:    sessionRediscoverPeriodMax,
			serverRequestTimeout:          requestTimeout,
		}
	}

	f.initMultiZoneClients()

	return nil
}

func (f *factory) initMultiZoneClients() {
	f.multiZoneClients = make(map[[2]string]multiZoneClient)

	for srcZoneID := range f.config.GetZones() {
		for dstZoneID := range f.config.GetZones() {
			if srcZoneID == dstZoneID {
				continue
			}

			srcClient := f.clients[srcZoneID]
			dstClient := f.clients[dstZoneID]

			f.multiZoneClients[[2]string{srcZoneID, dstZoneID}] = multiZoneClient{
				srcZoneClient: &srcClient,
				dstZoneClient: &dstClient,
				metrics:       f.metrics,
			}
		}
	}
}

func (f *factory) getClient(
	ctx context.Context,
	zoneID string,
) (*client, error) {

	client, ok := f.clients[zoneID]
	if !ok {
		return nil, errors.NewNonRetriableErrorf(
			"unknown zone %q, available zones: %q",
			zoneID,
			f.GetZones(),
		)
	}

	return &client, nil
}

func (f *factory) GetZones() []string {
	return maps.Keys(f.clients)
}

func (f *factory) HasClient(zoneID string) bool {
	_, ok := f.clients[zoneID]
	return ok
}

func (f *factory) GetClient(
	ctx context.Context,
	zoneID string,
) (Client, error) {

	return f.getClient(ctx, zoneID)
}

func (f *factory) GetClientFromDefaultZone(
	ctx context.Context,
) (Client, error) {

	var zoneID string

	for id := range f.config.GetZones() {
		zoneID = id
		// It's OK to use first known zone.
		break
	}

	return f.GetClient(ctx, zoneID)
}

func (f *factory) GetMultiZoneClient(
	srcZoneID string,
	dstZoneID string,
) (MultiZoneClient, error) {

	multiZoneClient, ok := f.multiZoneClients[[2]string{srcZoneID, dstZoneID}]
	if !ok {
		if _, ok := f.clients[srcZoneID]; !ok {
			return nil, errors.NewNonRetriableErrorf(
				"unknown zone %q, available zones: %q",
				srcZoneID,
				f.GetZones(),
			)
		}
		if _, ok := f.clients[dstZoneID]; !ok {
			return nil, errors.NewNonRetriableErrorf(
				"unknown zone %q, available zones: %q",
				dstZoneID,
				f.GetZones(),
			)
		}
		return nil, errors.NewNonRetriableErrorf("undefined error")
	}

	return &multiZoneClient, nil
}

////////////////////////////////////////////////////////////////////////////////

func newFactoryWithCreds(
	ctx context.Context,
	config *nbs_config.ClientConfig,
	creds auth.Credentials,
	clientMetricsRegistry metrics.Registry,
	sessionMetricsRegistry metrics.Registry,
) (*factory, error) {

	if config.GetDisableAuthentication() {
		creds = nil
	}

	f := &factory{
		config:                 config,
		credentials:            creds,
		sessionMetricsRegistry: sessionMetricsRegistry,
		metrics:                newClientMetrics(clientMetricsRegistry),
	}
	err := f.initClients(ctx)
	if err != nil {
		return nil, err
	}

	return f, nil
}

func NewFactoryWithCreds(
	ctx context.Context,
	config *nbs_config.ClientConfig,
	creds auth.Credentials,
	clientMetricsRegistry metrics.Registry,
	sessionMetricsRegistry metrics.Registry,
) (Factory, error) {

	return newFactoryWithCreds(
		ctx,
		config,
		creds,
		clientMetricsRegistry,
		sessionMetricsRegistry,
	)
}

func NewFactory(
	ctx context.Context,
	config *nbs_config.ClientConfig,
	clientMetricsRegistry metrics.Registry,
	sessionMetricsRegistry metrics.Registry,
) (Factory, error) {

	return NewFactoryWithCreds(
		ctx,
		config,
		nil,
		clientMetricsRegistry,
		sessionMetricsRegistry,
	)
}
