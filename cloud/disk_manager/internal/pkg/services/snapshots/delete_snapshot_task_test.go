package snapshots

import (
	"testing"

	"github.com/golang/protobuf/ptypes/empty"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/bucket"
	dataplane_protos "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/protos"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/test"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/resources"
	resources_mocks "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/resources/mocks"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/snapshots/protos"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/types"
	tasks_mocks "github.com/ydb-platform/nbs/cloud/tasks/mocks"
	"github.com/ydb-platform/nbs/cloud/tasks/persistence"
)

////////////////////////////////////////////////////////////////////////////////

func TestDeleteSnapshotTaskDeletesMeta(t *testing.T) {
	ctx := test.NewContext()

	snapshotS3 := newTestSnapshotS3(t, ctx)
	metaKey := bucket.SnapshotMetaKey("disk1", "snap1")
	err := snapshotS3.PutObject(
		ctx,
		metaKey,
		persistence.S3Object{Data: []byte("{}")},
	)
	require.NoError(t, err)

	storage := resources_mocks.NewStorageMock()
	scheduler := tasks_mocks.NewSchedulerMock()
	execCtx := tasks_mocks.NewExecutionContextMock()

	execCtx.On("GetTaskID").Return("delete1")
	storage.On(
		"DeleteSnapshot",
		mock.Anything,
		"snap1",
		"delete1",
		mock.Anything,
	).Return(&resources.SnapshotMeta{
		ID:           "snap1",
		Disk:         &types.Disk{ZoneId: "zone", DiskId: "disk1"},
		DeleteTaskID: "delete1",
	}, nil)
	scheduler.On(
		"ScheduleTask",
		mock.Anything,
		"dataplane.DeleteSnapshot",
		"",
		mock.MatchedBy(func(request *dataplane_protos.DeleteSnapshotRequest) bool {
			return request.SnapshotId == "snap1"
		}),
	).Return("dataplane1", nil)
	scheduler.On(
		"WaitTask",
		mock.Anything,
		execCtx,
		"dataplane1",
	).Return(&empty.Empty{}, nil)

	// The meta is gone by the time the snapshot is marked deleted.
	storage.On(
		"SnapshotDeleted",
		mock.Anything,
		"snap1",
		mock.Anything,
	).Run(func(mock.Arguments) {
		_, err := snapshotS3.GetObject(ctx, metaKey)
		require.Error(t, err)
	}).Return(nil)

	task := &deleteSnapshotTask{
		scheduler:  scheduler,
		storage:    storage,
		snapshotS3: snapshotS3,
		request:    &protos.DeleteSnapshotRequest{SnapshotId: "snap1"},
		state:      &protos.DeleteSnapshotTaskState{},
	}

	err = task.Run(ctx, execCtx)
	require.NoError(t, err)
	mock.AssertExpectationsForObjects(t, storage, scheduler, execCtx)

	// Deleting a missing meta is not an error.
	err = task.Run(ctx, execCtx)
	require.NoError(t, err)
}
