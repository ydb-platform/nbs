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

type deleteBackupObjectsTask struct {
	storage    storage.Storage
	followerS3 *backup.FollowerS3
	batchSize  int
	state      *protos.DeleteBackupObjectsTaskState
}

func (t *deleteBackupObjectsTask) Save() ([]byte, error) {
	return proto.Marshal(t.state)
}

func (t *deleteBackupObjectsTask) Load(_, state []byte) error {
	t.state = &protos.DeleteBackupObjectsTaskState{}
	return proto.Unmarshal(state, t.state)
}

func (t *deleteBackupObjectsTask) Run(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	for {
		objectKeys, err := t.storage.GetBackupDeleteQueue(ctx, t.batchSize)
		if err != nil {
			return err
		}

		if len(objectKeys) == 0 {
			return errors.NewInterruptExecutionError()
		}

		for _, objectKey := range objectKeys {
			err = t.followerS3.DeleteObject(ctx, objectKey)
			if err != nil {
				return err
			}
		}

		err = t.storage.BackupDeletionsCompleted(ctx, objectKeys)
		if err != nil {
			return err
		}
	}
}

func (t *deleteBackupObjectsTask) Cancel(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	return nil
}

func (t *deleteBackupObjectsTask) GetMetadata(
	ctx context.Context,
) (proto.Message, error) {

	return &empty.Empty{}, nil
}

func (t *deleteBackupObjectsTask) GetResponse() proto.Message {
	return &empty.Empty{}
}
