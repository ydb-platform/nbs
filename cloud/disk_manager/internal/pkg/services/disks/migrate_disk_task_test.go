package disks

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	disk_manager "github.com/ydb-platform/nbs/cloud/disk_manager/api"
	nbs_mocks "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nbs/mocks"
	dataplane_protos "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/protos"
	storage_mocks "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/resources/mocks"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/disks/protos"
	pools_mocks "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/pools/mocks"
	pools_storage_mocks "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/pools/storage/mocks"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/types"
	tasks_mocks "github.com/ydb-platform/nbs/cloud/tasks/mocks"
	"google.golang.org/protobuf/types/known/timestamppb"
)

////////////////////////////////////////////////////////////////////////////////

func TestMigrateDiskTaskProgress(t *testing.T) {
	ctx := context.Background()

	scheduler := tasks_mocks.NewSchedulerMock()
	poolService := pools_mocks.NewServiceMock()
	resourceStorage := storage_mocks.NewStorageMock()
	poolStorage := pools_storage_mocks.NewStorageMock()
	nbsFactory := nbs_mocks.NewFactoryMock()

	request := &protos.MigrateDiskRequest{
		Disk: &types.Disk{
			ZoneId: "zone",
			DiskId: t.Name(),
		},
		DstZoneId: "other",
	}

	task := &migrateDiskTask{
		scheduler:       scheduler,
		poolService:     poolService,
		resourceStorage: resourceStorage,
		poolStorage:     poolStorage,
		nbsFactory:      nbsFactory,
		request:         request,
		state:           &protos.MigrateDiskTaskState{},
	}

	metadataRaw, err := task.GetMetadata(ctx)
	require.NoError(t, err)

	metadata, ok := metadataRaw.(*disk_manager.MigrateDiskMetadata)
	require.True(t, ok)

	expectedMetadata := &disk_manager.MigrateDiskMetadata{
		Progress:         0,
		SecondsRemaining: math.MaxInt64,
	}

	require.Equal(t, expectedMetadata.Progress, metadata.Progress)
	require.Equal(t, expectedMetadata.SecondsRemaining, metadata.SecondsRemaining)
	require.Equal(t, expectedMetadata.UpdatedAt, metadata.UpdatedAt)

	task.state = &protos.MigrateDiskTaskState{
		ReplicateTaskID: "not_empty",
	}

	replicateDiskTaskProgress := 0.55
	replicateDiskTaskSecondsRemaining := int64(100)
	replicateDiskTaskUpdatedAt := timestamppb.New(time.Date(2023, 10, 5, 0, 0, 0, 0, time.UTC))

	scheduler.On(
		"GetTaskMetadata",
		mock.Anything,
		"not_empty",
	).Return(&dataplane_protos.ReplicateDiskTaskMetadata{
		Progress:         replicateDiskTaskProgress,
		SecondsRemaining: replicateDiskTaskSecondsRemaining,
		UpdatedAt:        replicateDiskTaskUpdatedAt,
	}, nil)

	metadataRaw, err = task.GetMetadata(ctx)
	require.NoError(t, err)

	metadata, ok = metadataRaw.(*disk_manager.MigrateDiskMetadata)
	require.True(t, ok)

	expectedMetadata = &disk_manager.MigrateDiskMetadata{
		Progress:         replicateDiskTaskProgress,
		SecondsRemaining: replicateDiskTaskSecondsRemaining,
		UpdatedAt:        replicateDiskTaskUpdatedAt,
	}

	require.Equal(t, expectedMetadata.Progress, metadata.Progress)
	require.Equal(t, expectedMetadata.SecondsRemaining, metadata.SecondsRemaining)
	require.Equal(t, expectedMetadata.UpdatedAt, metadata.UpdatedAt)

	mock.AssertExpectationsForObjects(t, scheduler)
}
