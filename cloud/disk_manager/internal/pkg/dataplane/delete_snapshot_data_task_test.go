package dataplane

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/protos"
	snapshot_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/config"
	snapshot_storage "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage/schema"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/test"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/monitoring/metrics"
	performance_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/performance/config"
	"github.com/ydb-platform/nbs/cloud/tasks/mocks"
	"github.com/ydb-platform/nbs/cloud/tasks/persistence"
	persistence_config "github.com/ydb-platform/nbs/cloud/tasks/persistence/config"
)

////////////////////////////////////////////////////////////////////////////////

func newYDB(ctx context.Context) (*persistence.YDBClient, error) {
	endpoint := fmt.Sprintf(
		"localhost:%v",
		os.Getenv("DISK_MANAGER_RECIPE_YDB_PORT"),
	)
	database := "/Root"
	connectionTimeout := "10s"

	return persistence.NewYDBClient(
		ctx,
		&persistence_config.PersistenceConfig{
			Endpoint:          &endpoint,
			Database:          &database,
			ConnectionTimeout: &connectionTimeout,
		},
		metrics.NewEmptyRegistry(),
	)
}

func newStorage(
	t *testing.T,
	ctx context.Context,
) (snapshot_storage.Storage, func()) {

	db, err := newYDB(ctx)
	require.NoError(t, err)

	closeFunc := func() {
		_ = db.Close(ctx)
	}

	storageFolder := fmt.Sprintf("dataplane_tasks_tests/%v", t.Name())
	deleteWorkerCount := uint32(10)
	shallowCopyWorkerCount := uint32(11)
	shallowCopyInflightLimit := uint32(22)
	shardCount := uint64(2)
	s3Bucket := "test"
	chunkBlobsS3KeyPrefix := t.Name()

	config := &snapshot_config.SnapshotConfig{
		StorageFolder:             &storageFolder,
		DeleteWorkerCount:         &deleteWorkerCount,
		ShallowCopyWorkerCount:    &shallowCopyWorkerCount,
		ShallowCopyInflightLimit:  &shallowCopyInflightLimit,
		ChunkBlobsTableShardCount: &shardCount,
		ChunkMapTableShardCount:   &shardCount,
		S3Bucket:                  &s3Bucket,
		ChunkBlobsS3KeyPrefix:     &chunkBlobsS3KeyPrefix,
	}

	s3, err := test.NewS3Client()
	require.NoError(t, err)

	err = schema.Create(ctx, config, db, s3, false /* dropUnusedColumns */)
	require.NoError(t, err)

	storage, err := snapshot_storage.NewStorage(
		config,
		metrics.NewEmptyRegistry(),
		db,
		s3,
	)
	require.NoError(t, err)

	return storage, closeFunc
}

////////////////////////////////////////////////////////////////////////////////

func TestDeleteSnapshotDataTask(t *testing.T) {
	ctx := test.NewContext()

	storage, closeFunc := newStorage(t, ctx)
	defer closeFunc()

	snapshotID := "snapshotID"
	deleteSnapshotDataBandwidthMiBs := uint64(42)
	storageSize := deleteSnapshotDataBandwidthMiBs * 1024 * 1024 // 42 MiB
	expectedEstimatedInflightDuration := 1 * time.Second

	_, err := storage.CreateSnapshot(
		ctx,
		snapshot_storage.SnapshotMeta{ID: snapshotID},
	)
	require.NoError(t, err)

	err = storage.SnapshotCreated(
		ctx,
		snapshotID,
		storageSize,                   // size
		storageSize,                   // storageSize
		uint32(storageSize/chunkSize), // chunkCount
		nil,                           // encryption
	)
	require.NoError(t, err)

	execCtx := mocks.NewExecutionContextMock()
	execCtx.On("SetEstimatedInflightDuration", expectedEstimatedInflightDuration).Once()

	task := &deleteSnapshotDataTask{
		performanceConfig: &performance_config.PerformanceConfig{
			DeleteSnapshotDataBandwidthMiBs: &deleteSnapshotDataBandwidthMiBs,
		},
		storage: storage,
		request: &protos.DeleteSnapshotDataRequest{SnapshotId: snapshotID},
		state:   &protos.DeleteSnapshotDataTaskState{},
	}

	err = task.Run(ctx, execCtx)
	require.NoError(t, err)

	mock.AssertExpectationsForObjects(t, execCtx)
}

func TestDeleteSnapshotDataTaskForNonExistingSnapshot(t *testing.T) {
	ctx := test.NewContext()

	storage, closeFunc := newStorage(t, ctx)
	defer closeFunc()

	execCtx := mocks.NewExecutionContextMock()

	snapshotID := "nonExistingSnapshotID"

	task := &deleteSnapshotDataTask{
		performanceConfig: &performance_config.PerformanceConfig{},
		storage:           storage,
		request:           &protos.DeleteSnapshotDataRequest{SnapshotId: snapshotID},
		state:             &protos.DeleteSnapshotDataTaskState{},
	}

	err := task.Run(ctx, execCtx)
	require.NoError(t, err)

	mock.AssertExpectationsForObjects(t, execCtx)
}
