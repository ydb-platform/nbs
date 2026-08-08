package dataplane

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sync/atomic"

	dataplane_protos "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/protos"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/monitoring/metrics"
	"github.com/ydb-platform/nbs/cloud/tasks"
	"github.com/ydb-platform/nbs/cloud/tasks/headers"
)

////////////////////////////////////////////////////////////////////////////////

func chunkBatchKey(chunkIDs []string) string {
	hasher := sha256.New()
	for _, chunkID := range chunkIDs {
		_, _ = hasher.Write([]byte(chunkID))
		_, _ = hasher.Write([]byte{0})
	}
	return hex.EncodeToString(hasher.Sum(nil))
}

func (t *relocateAllSnapshotsDataFromYDBToS3Task) relocateStillYdbChunksPass(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
	subregistry metrics.Registry,
) error {

	queueSize := int(t.config.GetRelocateSnapshotsScanQueueSize())
	if queueSize == 0 {
		queueSize = 500
	}
	batchSize := int(t.config.GetRelocateStillYdbChunksBatchSize())
	if batchSize == 0 {
		batchSize = 256
	}

	scanCtx, cancelScan := context.WithCancel(ctx)
	defer cancelScan()

	ids, scanErrors := t.storage.StreamStillYdbChunkIDs(scanCtx)
	candidates := make(chan string, queueSize)

	var queueDepth int64
	var scannedCount int64
	var scheduledCount int64

	scanDone := make(chan error, 1)
	go func() {
		defer close(candidates)

		for id := range ids {
			atomic.AddInt64(&scannedCount, 1)
			subregistry.Gauge("chunks/relocateStillYdbScanned").Set(
				float64(atomic.LoadInt64(&scannedCount)),
			)

			select {
			case candidates <- id:
				depth := atomic.AddInt64(&queueDepth, 1)
				subregistry.Gauge("chunks/relocateStillYdbQueue").Set(
					float64(depth),
				)
			case <-scanCtx.Done():
				scanDone <- scanCtx.Err()
				return
			}
		}

		var terminal error
		if err, ok := <-scanErrors; ok {
			terminal = err
		}
		scanDone <- terminal
	}()

	mapping := newSnapshotToTasksMapping()
	batchByKey := make(map[string][]string)
	for _, batch := range t.state.InflightChunkBatches {
		key := chunkBatchKey(batch.GetChunkIds())
		batchByKey[key] = append([]string(nil), batch.GetChunkIds()...)
	}
	scanExhausted := false

	for {
		err := t.updateInflightChunkBatchesFromScan(
			ctx,
			execCtx,
			candidates,
			&queueDepth,
			&scheduledCount,
			subregistry,
			&scanExhausted,
			batchByKey,
			batchSize,
		)
		if err != nil {
			cancelScan()
			<-scanDone
			return err
		}

		if len(t.state.InflightChunkBatches) == 0 {
			if scanExhausted {
				return <-scanDone
			}
			continue
		}

		err = t.scheduleInflightChunkBatches(
			ctx,
			execCtx,
			mapping,
			batchByKey,
		)
		if err != nil {
			cancelScan()
			<-scanDone
			return err
		}

		finishedTaskIDs, err := t.scheduler.WaitAnyTasks(
			ctx,
			mapping.taskIDs(),
		)
		if err != nil {
			cancelScan()
			<-scanDone
			return err
		}

		for _, key := range mapping.remove(finishedTaskIDs) {
			delete(batchByKey, key)
		}
		t.state.InflightChunkBatches = chunkBatchesFromMap(batchByKey)
		err = execCtx.SaveState(ctx)
		if err != nil {
			cancelScan()
			<-scanDone
			return err
		}
	}
}

func chunkBatchesFromMap(
	batchByKey map[string][]string,
) []*dataplane_protos.RelocateChunkIdBatch {

	batches := make(
		[]*dataplane_protos.RelocateChunkIdBatch,
		0,
		len(batchByKey),
	)
	for _, chunkIDs := range batchByKey {
		batches = append(batches, &dataplane_protos.RelocateChunkIdBatch{
			ChunkIds: append([]string(nil), chunkIDs...),
		})
	}
	return batches
}

func (t *relocateAllSnapshotsDataFromYDBToS3Task) updateInflightChunkBatchesFromScan(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
	candidates <-chan string,
	queueDepth *int64,
	scheduledCount *int64,
	subregistry metrics.Registry,
	scanExhausted *bool,
	batchByKey map[string][]string,
	batchSize int,
) error {

	inflightLimit := int(t.config.GetRelocatingSnapshotsToS3InflightLimit())
	if inflightLimit == 0 {
		inflightLimit = 1
	}

	for len(t.state.InflightChunkBatches) < inflightLimit {
		batch, exhausted := t.collectChunkBatch(
			candidates,
			queueDepth,
			subregistry,
			batchSize,
			len(t.state.InflightChunkBatches) == 0,
		)
		if exhausted {
			*scanExhausted = true
		}
		if len(batch) == 0 {
			return execCtx.SaveState(ctx)
		}

		key := chunkBatchKey(batch)
		if _, exists := batchByKey[key]; !exists {
			batchByKey[key] = append([]string(nil), batch...)
			t.state.InflightChunkBatches = chunkBatchesFromMap(batchByKey)
			atomic.AddInt64(scheduledCount, 1)
			subregistry.Gauge("chunks/relocateStillYdbScheduled").Set(
				float64(atomic.LoadInt64(scheduledCount)),
			)
		}

		if *scanExhausted {
			return execCtx.SaveState(ctx)
		}
	}

	return execCtx.SaveState(ctx)
}

func (t *relocateAllSnapshotsDataFromYDBToS3Task) collectChunkBatch(
	candidates <-chan string,
	queueDepth *int64,
	subregistry metrics.Registry,
	batchSize int,
	blockForFirst bool,
) ([]string, bool) {

	batch := make([]string, 0, batchSize)
	for len(batch) < batchSize {
		var chunkID string
		var ok bool

		if blockForFirst && len(batch) == 0 {
			chunkID, ok = <-candidates
			if !ok {
				return batch, true
			}
		} else {
			select {
			case chunkID, ok = <-candidates:
				if !ok {
					return batch, true
				}
			default:
				return batch, false
			}
		}

		depth := atomic.AddInt64(queueDepth, -1)
		if depth < 0 {
			depth = 0
			atomic.StoreInt64(queueDepth, 0)
		}
		subregistry.Gauge("chunks/relocateStillYdbQueue").Set(float64(depth))
		batch = append(batch, chunkID)
	}

	return batch, false
}

func (t *relocateAllSnapshotsDataFromYDBToS3Task) scheduleInflightChunkBatches(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
	mapping *snapshotToTasksMapping,
	batchByKey map[string][]string,
) error {

	for key, chunkIDs := range batchByKey {
		if mapping.hasSnapshots(key) {
			continue
		}

		taskID, err := t.scheduleRelocateChunkDataFromYDBToS3Task(
			ctx,
			execCtx,
			key,
			chunkIDs,
		)
		if err != nil {
			return err
		}
		mapping.add(key, taskID)
	}

	return nil
}

func (t *relocateAllSnapshotsDataFromYDBToS3Task) scheduleRelocateChunkDataFromYDBToS3Task(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
	batchKey string,
	chunkIDs []string,
) (string, error) {

	idempotencyKey := headers.SetIncomingIdempotencyKey(
		ctx,
		fmt.Sprintf(
			"relocate_chunk_data_from_ydb_to_s3_%s_%s",
			batchKey,
			execCtx.GetTaskID(),
		),
	)
	return t.scheduler.ScheduleTask(
		idempotencyKey,
		"dataplane.RelocateChunkDataFromYDBToS3Task",
		"",
		&dataplane_protos.RelocateChunkDataFromYDBToS3Request{
			ChunkIds:    chunkIDs,
			KeepYdbData: t.request.KeepYdbData,
		},
	)
}
