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
		ID:                imageID,
		UseDataplaneTasks: true,
	}, nil)

	err = task.Run(ctx, execCtx)
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.NewEmptyNonRetriableError()))
	require.Contains(t, err.Error(), "is not ready")

	mock.AssertExpectationsForObjects(t, s, execCtx)
}
