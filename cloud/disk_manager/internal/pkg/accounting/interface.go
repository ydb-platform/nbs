package accounting

import (
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/url"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/monitoring/metrics"
)

////////////////////////////////////////////////////////////////////////////////

func OnTaskCreated(taskType string, cloudID string, folderID string, taskCount int) {
	m := getMetrics()
	if m == nil {
		return
	}

	m.tasksCreateCount.With(map[string]string{
		"taskType": taskType,
		"cloudID":  cloudID,
		"folderID": folderID,
	}).Add(int64(taskCount))
}

////////////////////////////////////////////////////////////////////////////////

func OnSnapshotRead(snapshotID string, bytesCount int) {
	m := getMetrics()
	if m == nil {
		return
	}

	m.snapshotsReadBytes.With(map[string]string{
		"snapshotID": snapshotID,
	}).Add(int64(bytesCount))
}

func OnSnapshotWrite(snapshotID string, bytesCount int) {
	m := getMetrics()
	if m == nil {
		return
	}

	m.snapshotsWriteBytes.With(map[string]string{
		"snapshotID": snapshotID,
	}).Add(int64(bytesCount))
}

////////////////////////////////////////////////////////////////////////////////

func OnImageCreated(folderID string, format url.ImageFormat) {
	m := getMetrics()
	if m == nil {
		return
	}

	if !format.IsSupported() {
		m.imagesCreatedFolders.With(map[string]string{
			"folderID":    folderID,
			"imageFormat": string(format),
		}).Inc()
	}
	m.imagesCreated.With(map[string]string{
		"imageFormat": string(format),
	}).Inc()
}

////////////////////////////////////////////////////////////////////////////////

func Init(registry metrics.Registry) {
	initMetrics(registry)
}
