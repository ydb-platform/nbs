package monitoring

import (
	"fmt"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/siddontang/go-log/log"
	"github.com/ydb-platform/nbs/library/go/core/metrics"
	"github.com/ydb-platform/nbs/library/go/core/metrics/prometheus"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

////////////////////////////////////////////////////////////////////////////////

func requestDurationBuckets() metrics.DurationBuckets {
	return metrics.NewDurationBuckets(
		1*time.Millisecond, 10*time.Millisecond, 100*time.Millisecond,
		1*time.Second, 10*time.Second, 1*time.Minute, 10*time.Minute, 1*time.Hour,
	)
}

func isRetriableError(err error) bool {
	if err == nil {
		return false
	}

	status, ok := status.FromError(err)
	if !ok {
		return false
	}

	switch status.Code() {
	case codes.Aborted, codes.DeadlineExceeded, codes.Unavailable:
		return true
	}

	return false
}

////////////////////////////////////////////////////////////////////////////////

type MonitoringConfig struct {
	Port      uint
	Path      string
	Component string
}

type Monitoring struct {
	cfg      *MonitoringConfig
	registry metrics.Registry
	Handler  http.Handler
}

func (m *Monitoring) StartListening() {
	go func() {
		mux := http.NewServeMux()
		mux.Handle(m.cfg.Path, m.Handler)

		log.Info("Starting monitoring server on localhost:", m.cfg.Port)
		err := http.ListenAndServe(
			fmt.Sprintf(":%v", m.cfg.Port),
			mux,
		)
		if err != nil {
			log.Info(fmt.Sprintf(
				"Failed to set up monitoring on %v err=%v",
				m.cfg.Port,
				err,
			))
		}
	}()
}

func (m *Monitoring) ReportVersion(version string) {
	m.registry.WithTags(map[string]string{
		"revision": version,
	}).Gauge("version").Set(1)
}

func (m *Monitoring) ReportRequestReceived(method string) {
	subregistry := m.registry.WithTags(map[string]string{
		"method": method,
	})
	subregistry.Counter("Count").Inc()
	subregistry.IntGauge("InflightCount").Add(1)
}

func (m *Monitoring) ReportRequestCompleted(
	method string,
	err error,
	elapsedTime time.Duration,
) {
	subregistry := m.registry.WithTags(map[string]string{
		"method": method,
	})

	if elapsedTime > 0 {
		subregistry.DurationHistogram("Time", requestDurationBuckets()).RecordDuration(elapsedTime)
	}
	if err == nil {
		subregistry.Counter("Success").Inc()
	} else {
		if isRetriableError(err) {
			subregistry.Counter("RetriableErrors").Inc()
		} else {
			subregistry.Counter("Errors").Inc()
		}
	}
	subregistry.IntGauge("InflightCount").Add(-1)
}

////////////////////////////////////////////////////////////////////////////////

func NewMonitoring(
	cfg *MonitoringConfig,
) *Monitoring {
	registry := prometheus.NewRegistry(
		prometheus.NewRegistryOpts().AddTags(map[string]string{"component": cfg.Component}),
	)
	handler := promhttp.HandlerFor(
		registry.GetGatherer(),
		promhttp.HandlerOpts{},
	)

	return &Monitoring{
		cfg,
		registry,
		handler,
	}
}
