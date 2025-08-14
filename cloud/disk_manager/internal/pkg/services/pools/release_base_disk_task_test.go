package pools

import (
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	nbs "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nbs"
	nbs_mocks "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nbs/mocks"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/pools/protos"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/pools/storage"
	pools_storage_mocks "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/pools/storage/mocks"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/types"
	tasks_mocks "github.com/ydb-platform/nbs/cloud/tasks/mocks"
)

type TestAction uint32

const (
	TestActionRunTask    TestAction = iota
	TestActionCancelTask TestAction = iota
)

func testReleaseBaseDiskTask(t *testing.T, action TestAction) {
	ctx := newContext()
	s := pools_storage_mocks.NewStorageMock()
	nbsFactory := nbs_mocks.NewFactoryMock()
	nbsClient := nbs_mocks.NewClientMock()
	execCtx := tasks_mocks.NewExecutionContextMock()

	disk := &types.Disk{ZoneId: "zone", DiskId: "disk"}
	request := &protos.ReleaseBaseDiskRequest{OverlayDisk: disk}

	task := &releaseBaseDiskTask{
		storage:    s,
		nbsFactory: nbsFactory,
		request:    request,
		state:      &protos.ReleaseBaseDiskTaskState{},
	}

	baseDisk := storage.BaseDisk{
		ID:      "baseDisk",
		ImageID: "image",
		ZoneID:  "zone",
	}
	s.On("ReleaseBaseDiskSlot", ctx, disk).Return(baseDisk, nil)

	nbsFactory.On("GetClient", ctx, "zone").Return(nbsClient, nil)
	nbsClient.On("Describe", ctx, "disk").Return(
		nbs.DiskParams{Kind: types.DiskKind_DISK_KIND_SSD}, nil,
	)

	var err error
	if action == TestActionRunTask {
		err = task.Run(ctx, execCtx)
	} else {
		err = task.Cancel(ctx, execCtx)
	}
	require.NoError(t, err)
	mock.AssertExpectationsForObjects(t, s, nbsFactory, nbsClient, execCtx)
}

func TestReleaseBaseDiskTaskRun(t *testing.T) {
	testReleaseBaseDiskTask(t, TestActionRunTask)
}

func TestReleaseBaseDiskTaskCancel(t *testing.T) {
	testReleaseBaseDiskTask(t, TestActionCancelTask)
}
