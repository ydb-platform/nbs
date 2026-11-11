package snapshots

import (
	"context"
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	api "github.com/ydb-platform/nbs/cloud/disk_manager/api"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/snapshots/config"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/snapshots/protos"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/types"
	tasks_mocks "github.com/ydb-platform/nbs/cloud/tasks/mocks"
)

func TestCreateSnapshotUseProxyOverlayDiskZoneBlacklist(t *testing.T) {
	testCases := []struct {
		name                          string
		useProxyOverlayDisk           *bool
		proxyOverlayDiskZoneBlacklist []string
		requestZone                   string
		expectedUseProxyOverlayDisk   bool
	}{
		{
			name:        "disabled by default",
			requestZone: "zone-a",
		},
		{
			name:                          "globally disabled",
			useProxyOverlayDisk:           proto.Bool(false),
			proxyOverlayDiskZoneBlacklist: []string{"zone-b"},
			requestZone:                   "zone-a",
		},
		{
			name:                        "empty blacklist preserves global flag",
			useProxyOverlayDisk:         proto.Bool(true),
			requestZone:                 "zone-a",
			expectedUseProxyOverlayDisk: true,
		},
		{
			name:                          "request zone blacklisted",
			useProxyOverlayDisk:           proto.Bool(true),
			proxyOverlayDiskZoneBlacklist: []string{"zone-a"},
			requestZone:                   "zone-a",
		},
		{
			name:                          "cell entry does not match parent zone",
			useProxyOverlayDisk:           proto.Bool(true),
			proxyOverlayDiskZoneBlacklist: []string{"zone-a-shard1"},
			requestZone:                   "zone-a",
			expectedUseProxyOverlayDisk:   true,
		},
		{
			name:                          "another zone blacklisted",
			useProxyOverlayDisk:           proto.Bool(true),
			proxyOverlayDiskZoneBlacklist: []string{"zone-b"},
			requestZone:                   "zone-a",
			expectedUseProxyOverlayDisk:   true,
		},
		{
			name:                          "zone matching is exact",
			useProxyOverlayDisk:           proto.Bool(true),
			proxyOverlayDiskZoneBlacklist: []string{"zone-a"},
			requestZone:                   "zone-a-other",
			expectedUseProxyOverlayDisk:   true,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			ctx := context.Background()
			scheduler := tasks_mocks.NewSchedulerMock()

			expectedRequest := &protos.CreateSnapshotFromDiskRequest{
				SrcDisk: &types.Disk{
					ZoneId: testCase.requestZone,
					DiskId: "disk",
				},
				DstSnapshotId:                    "snapshot",
				FolderId:                         "folder",
				UseS3:                            true,
				UseProxyOverlayDisk:              testCase.expectedUseProxyOverlayDisk,
				RetryBrokenDRBasedDiskCheckpoint: true,
			}
			scheduler.On(
				"ScheduleTask",
				ctx,
				"snapshots.CreateSnapshotFromDisk",
				"",
				mock.MatchedBy(func(request *protos.CreateSnapshotFromDiskRequest) bool {
					return proto.Equal(expectedRequest, request)
				}),
			).Return("snapshot-task", nil).Once()

			service := NewService(scheduler, &config.SnapshotsConfig{
				UseProxyOverlayDisk:              testCase.useProxyOverlayDisk,
				UseProxyOverlayDiskZoneBlacklist: testCase.proxyOverlayDiskZoneBlacklist,
				UseS3ForFolder:                   []string{"folder"},
				RetryBrokenDRBasedDiskCheckpoint: proto.Bool(true),
			})
			taskID, err := service.CreateSnapshot(ctx, &api.CreateSnapshotRequest{
				Src: &api.DiskId{
					ZoneId: testCase.requestZone,
					DiskId: "disk",
				},
				SnapshotId: "snapshot",
				FolderId:   "folder",
			})
			require.NoError(t, err)
			require.Equal(t, "snapshot-task", taskID)
			scheduler.AssertExpectations(t)
		})
	}
}
