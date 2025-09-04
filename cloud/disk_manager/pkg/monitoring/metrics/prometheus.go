package metrics

import (
	"net/http"
	"regexp"
	"sync"

	prometheus2 "github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	clientmodel "github.com/prometheus/client_model/go"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/monitoring/metrics"
	"github.com/ydb-platform/nbs/library/go/core/metrics/prometheus"
)

////////////////////////////////////////////////////////////////////////////////

type gatherers struct {
	sync.Mutex
	gatherers prometheus2.Gatherers
}

func (g *gatherers) Add(gatherer prometheus2.Gatherer) {
	g.Lock()
	defer g.Unlock()
	g.gatherers = append(g.gatherers, gatherer)
}

func (g *gatherers) Gather() ([]*clientmodel.MetricFamily, error) {
	g.Lock()
	defer g.Unlock()
	return g.gatherers.Gather()
}

var allGatherers gatherers

////////////////////////////////////////////////////////////////////////////////

func sanitizeName(name string) string {
	// https://prometheus.io/docs/concepts/data_model/#metric-names-and-labels
	// Metric names should match the regexp: [a-zA-Z_:][a-zA-Z0-9_:]*
	// Metric labels should match the regexp [a-zA-Z_][a-zA-Z0-9_]*
	// For simplicity, we can ignore colons.
	validStartRegexp := regexp.MustCompile("^[a-zA-Z_]")
	if !validStartRegexp.MatchString(name) {
		name = "_" + name
	}

	invalidSymbolRegexp := regexp.MustCompile("[^a-zA-Z0-9_]")
	name = invalidSymbolRegexp.ReplaceAllString(name, "_")
	return name
}

////////////////////////////////////////////////////////////////////////////////

var newRegistryOnce sync.Once

func NewPrometheusRegistry(mux *http.ServeMux, path string) metrics.Registry {
	registry := prometheus.NewRegistry(
		prometheus.NewRegistryOpts().SetNameSanitizer(
			sanitizeName,
		).AddTags(map[string]string{"component": path}),
	)
	allGatherers.Add(registry)

	newRegistryOnce.Do(
		func() {
			mux.Handle(
				"/metrics/",
				promhttp.HandlerFor(
					prometheus2.GathererFunc(
						func() ([]*clientmodel.MetricFamily, error) {
							return allGatherers.Gather()
						},
					),
					promhttp.HandlerOpts{},
				),
			)
		},
	)
	return WrapRegistry(registry)
}
