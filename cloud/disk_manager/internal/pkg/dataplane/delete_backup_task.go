package dataplane

import (
	"context"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/backup"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/protos"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage"
	"github.com/ydb-platform/nbs/cloud/tasks"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
)

////////////////////////////////////////////////////////////////////////////////

// Regular task: removes objects listed in backup_deleting from the slaves.
type deleteBackupTask struct {
	storage   storage.Storage
	slaves    backup.Slaves
	batchSize int
	state     *protos.DeleteBackupTaskState
}

func (t *deleteBackupTask) Save() ([]byte, error) {
	return proto.Marshal(t.state)
}

func (t *deleteBackupTask) Load(_, state []byte) error {
	t.state = &protos.DeleteBackupTaskState{}
	return proto.Unmarshal(state, t.state)
}

func (t *deleteBackupTask) Run(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	for {
		entries, err := t.storage.GetBackupDeleting(ctx, t.batchSize)
		if err != nil {
			return err
		}

		if len(entries) == 0 {
			// Nothing to delete.
			return errors.NewInterruptExecutionError()
		}

		for _, entry := range entries {
			slave, err := t.slaves.Get(entry.Slave)
			if err != nil {
				return err
			}

			err = slave.S3.DeleteObject(
				ctx,
				slave.Bucket,
				slave.Key(entry.Object),
			)
			if err != nil {
				return err
			}
		}

		err = t.storage.ClearBackupDeleting(ctx, entries)
		if err != nil {
			return err
		}
	}
}

func (t *deleteBackupTask) Cancel(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	return nil
}

func (t *deleteBackupTask) GetMetadata(
	ctx context.Context,
) (proto.Message, error) {

	return &empty.Empty{}, nil
}

func (t *deleteBackupTask) GetResponse() proto.Message {
	return &empty.Empty{}
}
