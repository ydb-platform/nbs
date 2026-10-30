package images

import (
	"context"
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	disk_manager "github.com/ydb-platform/nbs/cloud/disk_manager/api"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/images/config"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/types"
	tasks_mocks "github.com/ydb-platform/nbs/cloud/tasks/mocks"
)

////////////////////////////////////////////////////////////////////////////////

func TestCreateImageOverriddenDiskPoolConfigsByFolder(t *testing.T) {
	sources := []struct {
		name     string
		request  *disk_manager.CreateImageRequest
		taskType string
	}{
		{
			name: "snapshot",
			request: &disk_manager.CreateImageRequest{
				Src: &disk_manager.CreateImageRequest_SrcSnapshotId{
					SrcSnapshotId: "snapshot",
				},
			},
			taskType: "images.CreateImageFromSnapshot",
		},
		{
			name: "image",
			request: &disk_manager.CreateImageRequest{
				Src: &disk_manager.CreateImageRequest_SrcImageId{
					SrcImageId: "source-image",
				},
			},
			taskType: "images.CreateImageFromImage",
		},
		{
			name: "url",
			request: &disk_manager.CreateImageRequest{
				Src: &disk_manager.CreateImageRequest_SrcUrl{
					SrcUrl: &disk_manager.ImageUrl{Url: "https://example.com/image"},
				},
			},
			taskType: "images.CreateImageFromURL",
		},
		{
			name: "disk",
			request: &disk_manager.CreateImageRequest{
				Src: &disk_manager.CreateImageRequest_SrcDiskId{
					SrcDiskId: &disk_manager.DiskId{ZoneId: "zone-a", DiskId: "disk"},
				},
			},
			taskType: "images.CreateImageFromDisk",
		},
	}

	defaultPools := []*types.DiskPool{
		{ZoneId: "zone-a", Capacity: 10},
		{ZoneId: "zone-b", Capacity: 15},
	}
	mergedPools := []*types.DiskPool{
		{ZoneId: "zone-a", Capacity: 10},
		{ZoneId: "zone-b", Capacity: 20},
		{ZoneId: "zone-c", Capacity: 30},
	}
	testCases := []struct {
		name                    string
		folderID                string
		pooled                  bool
		configurePoolsByDefault bool
		expectedPools           []*types.DiskPool
	}{
		{
			name:          "folder overrides matching zones and adds new zones",
			folderID:      "folder",
			pooled:        true,
			expectedPools: mergedPools,
		},
		{
			name:          "default fallback",
			folderID:      "other-folder",
			pooled:        true,
			expectedPools: defaultPools,
		},
		{
			name:          "empty override retains defaults",
			folderID:      "empty-folder",
			pooled:        true,
			expectedPools: defaultPools,
		},
		{
			name:          "nil override retains defaults",
			folderID:      "nil-folder",
			pooled:        true,
			expectedPools: defaultPools,
		},
		{
			name:          "pooling disabled",
			folderID:      "folder",
			expectedPools: []*types.DiskPool{},
		},
		{
			name:                    "configure pools by default",
			folderID:                "folder",
			configurePoolsByDefault: true,
			expectedPools:           mergedPools,
		},
	}

	for _, source := range sources {
		for _, testCase := range testCases {
			t.Run(source.name+"/"+testCase.name, func(t *testing.T) {
				ctx := context.Background()
				scheduler := tasks_mocks.NewSchedulerMock()
				imageService := NewService(scheduler, &config.ImagesConfig{
					DefaultDiskPoolConfigs: []*config.DiskPoolConfig{
						{ZoneId: proto.String("zone-a"), Capacity: proto.Uint32(10)},
						{ZoneId: proto.String("zone-b"), Capacity: proto.Uint32(15)},
					},
					OverriddenDiskPoolConfigsByFolder: map[string]*config.DiskPoolConfigs{
						"folder": {
							Configs: []*config.DiskPoolConfig{
								{ZoneId: proto.String("zone-b"), Capacity: proto.Uint32(20)},
								{ZoneId: proto.String("zone-c"), Capacity: proto.Uint32(30)},
							},
						},
						"empty-folder": {},
						"nil-folder":   nil,
					},
					ConfigurePoolsByDefault: proto.Bool(testCase.configurePoolsByDefault),
				})
				scheduler.On(
					"ScheduleTask",
					ctx,
					source.taskType,
					"",
					mock.Anything,
				).Run(func(args mock.Arguments) {
					request := args.Get(3).(interface {
						GetDiskPools() []*types.DiskPool
					})
					actualPools := request.GetDiskPools()
					require.Len(t, actualPools, len(testCase.expectedPools))
					for i, expectedPool := range testCase.expectedPools {
						require.True(t, proto.Equal(expectedPool, actualPools[i]))
					}
				}).Return("create-image-task", nil).Once()

				request := proto.Clone(source.request).(*disk_manager.CreateImageRequest)
				request.DstImageId = "image"
				request.FolderId = testCase.folderID
				request.Pooled = testCase.pooled
				taskID, err := imageService.CreateImage(ctx, request)
				require.NoError(t, err)
				require.Equal(t, "create-image-task", taskID)
				scheduler.AssertExpectations(t)
			})
		}
	}
}
