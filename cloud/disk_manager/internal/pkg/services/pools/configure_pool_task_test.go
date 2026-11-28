package pools

import (
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/resources"
	resources_mocks "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/resources/mocks"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/pools/protos"
	pools_storage_mocks "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/pools/storage/mocks"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	tasks_mocks "github.com/ydb-platform/nbs/cloud/tasks/mocks"
)

////////////////////////////////////////////////////////////////////////////////

func TestConfigurePoolTaskImageNotReady(t *testing.T) {
	ctx := newContext()
	resource := resources_mocks.NewStorageMock()
	s := pools_storage_mocks.NewStorageMock()
	execCtx := tasks_mocks.NewExecutionContextMock()

	task := &configurePoolTask{
		storage:         s,
		resourceStorage: resource,
	}

	zone := "zone"
	imageID := t.Name()
	capacity := uint32(1)

	request := &protos.ConfigurePoolRequest{
		ZoneId:       zone,
		ImageId:      imageID,
		Capacity:     capacity,
		UseImageSize: false,
	}
	marshalledRequest, err := proto.Marshal(request)
	require.NoError(t, err)

	err = task.Load(marshalledRequest, nil)
	require.NoError(t, err)

	resource.On(
		"GetImageMeta",
		ctx,
		imageID,
	).Once().Return(&resources.ImageMeta{
		ID: imageID,
	}, nil)

	err = task.Run(ctx, execCtx)
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.NewEmptyNonRetriableError()))
	require.Contains(t, err.Error(), "is not ready")

	mock.AssertExpectationsForObjects(t, s, execCtx)
}

func newConfigurePoolTaskForTest(
	t *testing.T,
	resource *resources_mocks.StorageMock,
	s *pools_storage_mocks.StorageMock,
) (*configurePoolTask, string) {

	task := &configurePoolTask{
		storage:         s,
		resourceStorage: resource,
	}

	request := &protos.ConfigurePoolRequest{
		ZoneId:       "zone",
		ImageId:      t.Name(),
		Capacity:     1,
		UseImageSize: false,
	}
	marshalledRequest, err := proto.Marshal(request)
	require.NoError(t, err)

	err = task.Load(marshalledRequest, nil)
	require.NoError(t, err)

	return task, t.Name()
}

func TestConfigurePoolTaskImageAlive(t *testing.T) {
	ctx := newContext()
	resource := resources_mocks.NewStorageMock()
	s := pools_storage_mocks.NewStorageMock()
	execCtx := tasks_mocks.NewExecutionContextMock()

	task, imageID := newConfigurePoolTaskForTest(t, resource, s)

	resource.On("GetImageMeta", ctx, imageID).Twice().Return(&resources.ImageMeta{
		ID:    imageID,
		Ready: true,
	}, nil)
	s.On(
		"ConfigurePool",
		ctx,
		imageID,
		"zone",
		uint32(1),
		uint64(0),
	).Once().Return(nil)

	err := task.Run(ctx, execCtx)
	require.NoError(t, err)

	mock.AssertExpectationsForObjects(t, resource, s, execCtx)
}

// Image was deleted before the task has run (e.g. image creation was
// cancelled). Config might have been written by a previous attempt of this
// task, so the pool should be deleted.
func TestConfigurePoolTaskImageDeleted(t *testing.T) {
	ctx := newContext()
	resource := resources_mocks.NewStorageMock()
	s := pools_storage_mocks.NewStorageMock()
	execCtx := tasks_mocks.NewExecutionContextMock()

	task, imageID := newConfigurePoolTaskForTest(t, resource, s)

	var m *resources.ImageMeta
	resource.On("GetImageMeta", ctx, imageID).Once().Return(m, nil)
	s.On("DeletePool", ctx, imageID, "zone").Once().Return(nil)

	err := task.Run(ctx, execCtx)
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.NewEmptyNonRetriableError()))

	mock.AssertExpectationsForObjects(t, resource, s, execCtx)
}

// Image is deleted concurrently with pool configuration: image check passes,
// then image is marked as deleting and pools.ImageDeleting runs (and finds
// nothing), then config is written. Task must notice that and roll the config
// back.
func TestConfigurePoolTaskImageDeletedConcurrently(t *testing.T) {
	ctx := newContext()
	resource := resources_mocks.NewStorageMock()
	s := pools_storage_mocks.NewStorageMock()
	execCtx := tasks_mocks.NewExecutionContextMock()

	task, imageID := newConfigurePoolTaskForTest(t, resource, s)

	resource.On("GetImageMeta", ctx, imageID).Once().Return(&resources.ImageMeta{
		ID:    imageID,
		Ready: true,
	}, nil)
	s.On(
		"ConfigurePool",
		ctx,
		imageID,
		"zone",
		uint32(1),
		uint64(0),
	).Once().Return(nil)
	// Image has been marked as deleting in the meantime.
	resource.On("GetImageMeta", ctx, imageID).Once().Return(&resources.ImageMeta{
		ID:           imageID,
		Ready:        false,
		DeleteTaskID: "delete_image_task",
	}, nil)
	s.On("DeletePool", ctx, imageID, "zone").Once().Return(nil)

	err := task.Run(ctx, execCtx)
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.NewEmptyNonRetriableError()))

	mock.AssertExpectationsForObjects(t, resource, s, execCtx)
}

// Task was cancelled (e.g. interrupted right after writing the config).
func TestConfigurePoolTaskCancelRollsBackConfigOfDeletedImage(t *testing.T) {
	ctx := newContext()
	resource := resources_mocks.NewStorageMock()
	s := pools_storage_mocks.NewStorageMock()
	execCtx := tasks_mocks.NewExecutionContextMock()

	task, imageID := newConfigurePoolTaskForTest(t, resource, s)

	resource.On("GetImageMeta", ctx, imageID).Once().Return(&resources.ImageMeta{
		ID:           imageID,
		DeleteTaskID: "delete_image_task",
	}, nil)
	s.On("DeletePool", ctx, imageID, "zone").Once().Return(nil)

	err := task.Cancel(ctx, execCtx)
	require.NoError(t, err)

	// Nothing to roll back if image is alive.
	resource.On("GetImageMeta", ctx, imageID).Once().Return(&resources.ImageMeta{
		ID:    imageID,
		Ready: true,
	}, nil)

	err = task.Cancel(ctx, execCtx)
	require.NoError(t, err)

	mock.AssertExpectationsForObjects(t, resource, s, execCtx)
}
