package accounting

import (
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/monitoring/metrics"
)

////////////////////////////////////////////////////////////////////////////////

type accountingMetrics struct {
	// tasks accounting
	tasksCreateCount metrics.CounterVec

	// snapshots accounting
	snapshotsReadBytes  metrics.CounterVec
	snapshotsWriteBytes metrics.CounterVec

	// images usage
	imagesCreated        metrics.CounterVec
	imagesCreatedFolders metrics.CounterVec
}

////////////////////////////////////////////////////////////////////////////////

var metricsInstance *accountingMetrics

func getMetrics() *accountingMetrics {
	return metricsInstance
}

func initMetrics(registry metrics.Registry) {
	metricsInstance = &accountingMetrics{
		tasksCreateCount: registry.CounterVec("tasks/CreateCount", []string{
			"taskType",
			"cloudID",
			"folderID",
		}),

		snapshotsReadBytes: registry.CounterVec("snapshots/ReadBytes", []string{
			"snapshotID",
		}),
		snapshotsWriteBytes: registry.CounterVec("snapshots/WriteBytes", []string{
			"snapshotID",
		}),
		imagesCreated: registry.CounterVec("images/ImagesCreated", []string{
			"imageFormat",
		}),
		imagesCreatedFolders: registry.CounterVec("images/ImagesCreatedFolders", []string{
			"folderID",
			"imageFormat",
		}),
	}
}
