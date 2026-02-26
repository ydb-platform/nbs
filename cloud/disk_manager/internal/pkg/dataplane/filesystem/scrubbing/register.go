package scrubbing

import (
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nfs"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/filesystem/scrubbing/config"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/filesystem/traversal/storage"
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
	config *config.FilesystemScrubbingConfig,
	factory nfs.Factory,
	storage storage.Storage,
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
