package dataplane

import (
	"context"
	"fmt"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/backup"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/backup/layout"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/protos"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/monitoring/metrics"
	"github.com/ydb-platform/nbs/cloud/tasks"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/logging"
	"github.com/ydb-platform/nbs/cloud/tasks/persistence"
	"golang.org/x/sync/errgroup"
)

////////////////////////////////////////////////////////////////////////////////

// Regular task: takes batches from backup_queue and copies chunk objects from
// our S3 to the slave as is, with their metadata.
type backupChunksTask struct {
	storage      storage.Storage
	srcS3        *persistence.S3Client
	srcBucket    string
	srcKeyPrefix string
	slaves       backup.Slaves
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
			// Nothing to copy.
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

	slave, err := t.slaves.Get(entry.Slave)
	if err != nil {
		return err
	}

	object, err := t.srcS3.GetObject(
		ctx,
		t.srcBucket,
		fmt.Sprintf("%v/%v", t.srcKeyPrefix, entry.ChunkID),
	)
	if err != nil {
		// GetObject reports a missing key with a silent non retriable error.
		if errors.Is(err, errors.NewEmptyNonRetriableError()) &&
			errors.IsSilent(err) {

			// The snapshot has been deleted before its chunk was copied.
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

	err = slave.S3.PutObject(
		ctx,
		slave.Bucket,
		slave.Key(layout.ChunkObject(entry.ChunkID)),
		persistence.S3Object{
			Data:     object.Data,
			Metadata: object.Metadata,
		},
	)
	if err != nil {
		return err
	}

	registry := t.registry.WithTags(map[string]string{"slave": slave.ID})
	registry.Counter("backup/copiedChunks").Inc()
	registry.Counter("backup/copiedBytes").Add(int64(len(object.Data)))
	return nil
}
