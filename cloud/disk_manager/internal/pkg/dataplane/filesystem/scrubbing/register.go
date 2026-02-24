package filesystem_scrubbing

import (
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nfs"
	scrubbing_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/filesystem_scrubbing/config"
	filesystem_snapshot_storage "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/filesystem_snapshot/storage"
	"github.com/ydb-platform/nbs/cloud/tasks"
)

////////////////////////////////////////////////////////////////////////////////

func Register(taskRegistry *tasks.Registry) error {
	return taskRegistry.Register(
		"dataplane.ScrubFilesystem",
		func() tasks.Task {
			return &scrubFilesystemTask{}
		},
	)
}

func RegisterForExecution(
	taskRegistry *tasks.Registry,
	config *scrubbing_config.FilesystemScrubbingConfig,
	factory nfs.Factory,
	storage filesystem_snapshot_storage.Storage,
) error {

	return taskRegistry.RegisterForExecution(
		"dataplane.ScrubFilesystem",
		func() tasks.Task {
			return &scrubFilesystemTask{
				config:  config,
				factory: factory,
				storage: storage,
			}
		},
	)
}
