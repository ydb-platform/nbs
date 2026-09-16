package backup

import (
	"context"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/backup/protos"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/monitoring/metrics"
	"github.com/ydb-platform/nbs/cloud/tasks"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/logging"
	"github.com/ydb-platform/nbs/cloud/tasks/persistence"
	"golang.org/x/sync/errgroup"
)

////////////////////////////////////////////////////////////////////////////////

type backupChunksTask struct {
	storage      storage.Storage
	dstS3        *persistence.S3Client
	dstBucket    string
	dstKeyPrefix string
	batchSize    int
	workerCount  int
	registry     metrics.Registry
	state        *protos.BackupChunksTaskState
}

func (t *backupChunksTask) Save() ([]byte, error) {
	return proto.Marshal(t.state)
}

func (t *backupChunksTask) Load(_, state []byte) error {
	t.state = &protos.BackupChunksTaskState{}
	return proto.Unmarshal(state, t.state)
}

func (t *backupChunksTask) Run(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	for {
		entries, err := t.storage.GetBackupQueue(ctx, t.batchSize)
		if err != nil {
			return err
		}

		if len(entries) == 0 {
			return errors.NewInterruptExecutionError()
		}

		group, groupCtx := errgroup.WithContext(ctx)
		group.SetLimit(t.workerCount)

		for _, entry := range entries {
			entry := entry
			group.Go(func() error {
				return t.copyChunk(groupCtx, entry)
			})
		}

		err = group.Wait()
		if err != nil {
			return err
		}

		err = t.storage.ClearBackupQueue(ctx, entries)
		if err != nil {
			return err
		}
	}
}

func (t *backupChunksTask) Cancel(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	return nil
}

func (t *backupChunksTask) GetMetadata(
	ctx context.Context,
) (proto.Message, error) {

	return &empty.Empty{}, nil
}

func (t *backupChunksTask) GetResponse() proto.Message {
	return &empty.Empty{}
}

////////////////////////////////////////////////////////////////////////////////

func (t *backupChunksTask) copyChunk(
	ctx context.Context,
	entry storage.BackupQueueEntry,
) error {

	object, err := t.storage.ReadChunkBlob(ctx, entry.ChunkID)
	if err != nil {
		if errors.Is(err, errors.NewEmptyNonRetriableError()) &&
			errors.IsSilent(err) {

			logging.Warn(
				ctx,
				"chunk %v of snapshot %v is gone, skipping backup: %v",
				entry.ChunkID,
				entry.SnapshotID,
				err,
			)
			return nil
		}

		return err
	}

	err = t.dstS3.PutObject(
		ctx,
		t.dstBucket,
		ChunkKey(t.dstKeyPrefix, entry.ChunkID),
		persistence.S3Object{
			Data:     object.Data,
			Metadata: object.Metadata,
		},
	)
	if err != nil {
		return err
	}

	t.registry.Counter("backup/copiedChunks").Inc()
	t.registry.Counter("backup/copiedBytes").Add(int64(len(object.Data)))
	return nil
}
