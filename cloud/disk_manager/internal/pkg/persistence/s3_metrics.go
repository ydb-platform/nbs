package persistence

import (
	"time"

	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/monitoring/metrics"
)

////////////////////////////////////////////////////////////////////////////////

func s3requestDurationBuckets() metrics.DurationBuckets {
	return metrics.NewExponentialDurationBuckets(
		time.Millisecond,
		1.25,
		50,
	)
}

////////////////////////////////////////////////////////////////////////////////

type s3Metrics struct {
	registry metrics.Registry
}

func (m *s3Metrics) StatRequest(name string) func(err *error) {
	start := time.Now()

	return func(err *error) {
		subRegistry := m.registry.WithTags(map[string]string{
			"request": name,
		})

		// Should initialize all counters before using them, to avoid 'no data'.
		errorCounter := subRegistry.Counter("errors")
		successCounter := subRegistry.Counter("success")
		timeHistogram := subRegistry.DurationHistogram("time", s3requestDurationBuckets())

		if *err != nil {
			errorCounter.Inc()
			return
		}

		timeHistogram.RecordDuration(time.Since(start))
		successCounter.Inc()
	}
}

func newS3Metrics(registry metrics.Registry) *s3Metrics {
	return &s3Metrics{
		registry: registry,
	}
}
