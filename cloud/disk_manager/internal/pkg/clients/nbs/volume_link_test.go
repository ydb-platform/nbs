package nbs

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/nbs/cloud/blockstore/public/api/protos"
	nbs_client "github.com/ydb-platform/nbs/cloud/blockstore/public/sdk/go/client"
	client_metrics "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/metrics"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/monitoring/metrics"
	core_protos "github.com/ydb-platform/nbs/cloud/storage/core/protos"
	task_errors "github.com/ydb-platform/nbs/cloud/tasks/errors"
)

type volumeLinkTestClient struct {
	nbs_client.ClientIface
	describe func(*protos.TDescribeVolumeRequest) (*protos.TDescribeVolumeResponse, error)
	create   func(*protos.TCreateVolumeRequest) (*protos.TCreateVolumeResponse, error)
	action   func(*protos.TExecuteActionRequest) (*protos.TExecuteActionResponse, error)
	destroy  func(*protos.TDestroyVolumeRequest) (*protos.TDestroyVolumeResponse, error)
}

func (c *volumeLinkTestClient) DescribeVolume(_ context.Context, request *protos.TDescribeVolumeRequest) (*protos.TDescribeVolumeResponse, error) {
	return c.describe(request)
}

func (c *volumeLinkTestClient) CreateVolume(_ context.Context, request *protos.TCreateVolumeRequest) (*protos.TCreateVolumeResponse, error) {
	return c.create(request)
}

func (c *volumeLinkTestClient) ExecuteAction(_ context.Context, request *protos.TExecuteActionRequest) (*protos.TExecuteActionResponse, error) {
	return c.action(request)
}

func (c *volumeLinkTestClient) DestroyVolume(_ context.Context, request *protos.TDestroyVolumeRequest) (*protos.TDestroyVolumeResponse, error) {
	return c.destroy(request)
}

func newVolumeLinkTestClient(cellID string, impl nbs_client.ClientIface) client {
	sdk := &nbs_client.DiscoveryClient{}
	sdk.Impl = impl
	return client{
		nbs:     sdk,
		zoneID:  cellID,
		metrics: client_metrics.NewClientMetrics(metrics.NewEmptyRegistry()),
	}
}

func TestParseVolumeLinkStatus(t *testing.T) {
	status, err := parseVolumeLinkStatus([]byte(`{"status":"LINK_STATUS_PREPARING","linkUUID":"uuid"}`))
	require.NoError(t, err)
	require.Equal(t, VolumeLinkStatePreparing, status.State)
	require.Equal(t, "uuid", status.LinkUUID)

	status, err = parseVolumeLinkStatus([]byte(`{"Status":"LINK_STATUS_COMPLETED","LinkUUID":"uuid"}`))
	require.NoError(t, err)
	require.Equal(t, VolumeLinkStateCompleted, status.State)
	require.Equal(t, "uuid", status.LinkUUID)

	for _, output := range []string{
		`{}`, `{"status":"new-status"}`,
		`{"status":`,
	} {
		_, err := parseVolumeLinkStatus([]byte(output))
		require.Error(t, err)
	}
}

func TestGetVolumeLinkStatusUsesExactActionFields(t *testing.T) {
	link := VolumeLink{"leader", "source-cell", "follower", "target-cell"}
	impl := &volumeLinkTestClient{action: func(request *protos.TExecuteActionRequest) (*protos.TExecuteActionResponse, error) {
		require.Equal(t, "getlinkstatus", request.Action)
		require.True(t, request.GetHeaders().GetExactDiskIdMatch())
		var input map[string]string
		require.NoError(t, json.Unmarshal(request.Input, &input))
		require.Equal(t, map[string]string{
			"LeaderDiskId": "leader", "LeaderCellId": "source-cell",
			"FollowerDiskId": "follower", "FollowerCellId": "target-cell",
		}, input)
		return &protos.TExecuteActionResponse{Output: []byte(`{"status":"LINK_STATUS_COMPLETED","linkUUID":"uuid"}`)}, nil
	}}
	client := newVolumeLinkTestClient("source-cell", impl)
	status, err := client.GetVolumeLinkStatus(context.Background(), link)
	require.NoError(t, err)
	require.Equal(t, VolumeLinkStateCompleted, status.State)

	client.zoneID = "another-cell"
	_, err = client.GetVolumeLinkStatus(context.Background(), link)
	require.Error(t, err)
}

func TestCreateVolumeLinkTargetRecoversLostReply(t *testing.T) {
	for _, sameCell := range []bool{false, true} {
		name := "cross-cell"
		if sameCell {
			name = "same-cell"
		}
		t.Run(name, func(t *testing.T) {
			link := VolumeLink{"leader", "source-cell", "follower", "target-cell"}
			if sameCell {
				link.FollowerCellID = link.LeaderCellID
				link.FollowerDiskID = "leader-copy"
			}
			source := &protos.TVolume{
				DiskId: "leader", BlockSize: 4096, BlocksCount: 1024,
				StorageMediaKind: core_protos.EStorageMediaKind_STORAGE_MEDIA_SSD,
				CloudId:          "cloud", FolderId: "folder", TabletVersion: 1,
				PartitionsCount: 4, IsSystem: true, ProjectId: "project", ChannelsCount: 3,
			}
			var target *protos.TVolume
			creates := 0
			impl := &volumeLinkTestClient{}
			impl.describe = func(request *protos.TDescribeVolumeRequest) (*protos.TDescribeVolumeResponse, error) {
				require.True(t, request.GetHeaders().GetExactDiskIdMatch())
				if request.DiskId == link.LeaderDiskID && sameCell {
					return &protos.TDescribeVolumeResponse{Volume: source}, nil
				}
				if target == nil {
					return nil, &nbs_client.ClientError{Code: nbs_client.E_NOT_FOUND}
				}
				return &protos.TDescribeVolumeResponse{Volume: target}, nil
			}
			impl.create = func(request *protos.TCreateVolumeRequest) (*protos.TCreateVolumeResponse, error) {
				creates++
				require.Equal(t, link.FollowerDiskID, request.DiskId)
				require.Zero(t, request.FillGeneration)
				require.Equal(t, source.BlockSize, request.BlockSize)
				require.Equal(t, source.BlocksCount, request.BlocksCount)
				require.Equal(t, source.PartitionsCount, request.PartitionsCount)
				require.Equal(t, source.TabletVersion, request.TabletVersion)
				require.Equal(t, source.IsSystem, request.IsSystem)
				require.Equal(t, source.ProjectId, request.ProjectId)
				require.Equal(t, source.ChannelsCount, request.ChannelsCount)
				target = &protos.TVolume{
					DiskId: request.DiskId, BlockSize: request.BlockSize, BlocksCount: request.BlocksCount,
					Tags: make(map[string]string),
				}
				for _, tag := range strings.Split(request.TagsStr, ",") {
					pair := strings.SplitN(tag, "=", 2)
					target.Tags[pair[0]] = pair[1]
				}
				// The volume was created, but the caller did not receive success.
				return nil, &nbs_client.ClientError{Code: nbs_client.E_REJECTED}
			}
			factory := &factory{clients: map[string]client{
				link.FollowerCellID: newVolumeLinkTestClient(link.FollowerCellID, impl),
			}}
			if !sameCell {
				factory.clients[link.LeaderCellID] = newVolumeLinkTestClient(link.LeaderCellID, &volumeLinkTestClient{
					describe: func(request *protos.TDescribeVolumeRequest) (*protos.TDescribeVolumeResponse, error) {
						require.Equal(t, link.LeaderDiskID, request.DiskId)
						require.True(t, request.GetHeaders().GetExactDiskIdMatch())
						return &protos.TDescribeVolumeResponse{Volume: source}, nil
					},
				})
			}
			require.Error(t, factory.CreateVolumeLinkTarget(context.Background(), link, "task-id"))
			require.NoError(t, factory.CreateVolumeLinkTarget(context.Background(), link, "task-id"))
			require.Equal(t, 1, creates)
			require.Equal(t, volumeLinkSourceID(link), target.Tags["source-disk-id"])
			require.Error(t, factory.CreateVolumeLinkTarget(context.Background(), link, "another-task"))
			require.Equal(t, 1, creates)
		})
	}
}

func TestVolumeLinkTargetRejectsChangedGeometry(t *testing.T) {
	link := VolumeLink{"leader", "cell", "follower", "cell"}
	source := &protos.TVolume{BlockSize: 4096, BlocksCount: 1024}
	target := &protos.TVolume{BlockSize: 4096, BlocksCount: 512, Tags: map[string]string{
		"source-disk-id": "leader", volumeLinkTaskIDTag: "task",
	}}
	require.ErrorContains(t, validateVolumeLinkTarget(target, source, link, "task"), "geometry differs")
}

func TestResolveVolumeLinkSourcePreservesPhysicalName(t *testing.T) {
	for _, physicalID := range []string{"leader", "leader-copy", "unrelated"} {
		t.Run(physicalID, func(t *testing.T) {
			impl := &volumeLinkTestClient{describe: func(request *protos.TDescribeVolumeRequest) (*protos.TDescribeVolumeResponse, error) {
				require.Equal(t, "leader", request.DiskId)
				require.False(t, request.GetHeaders().GetExactDiskIdMatch())
				return &protos.TDescribeVolumeResponse{Volume: &protos.TVolume{DiskId: physicalID}}, nil
			}}
			client := newVolumeLinkTestClient("cell", impl)
			actual, err := client.ResolveVolumeLinkSource(context.Background(), "leader")
			if physicalID == "unrelated" {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, physicalID, actual)
			}
		})
	}
}

func TestDeleteVolumeLinkTargetChecksOwnershipAndExactName(t *testing.T) {
	for _, owner := range []string{"task", "another-task"} {
		t.Run(owner, func(t *testing.T) {
			link := VolumeLink{"leader", "source-cell", "leader", "target-cell"}
			deletes := 0
			impl := &volumeLinkTestClient{
				describe: func(request *protos.TDescribeVolumeRequest) (*protos.TDescribeVolumeResponse, error) {
					require.True(t, request.GetHeaders().GetExactDiskIdMatch())
					return &protos.TDescribeVolumeResponse{Volume: &protos.TVolume{DiskId: "leader", Tags: map[string]string{
						volumeLinkTaskIDTag: owner, "source-disk-id": "source-cell/leader",
					}}}, nil
				},
				destroy: func(request *protos.TDestroyVolumeRequest) (*protos.TDestroyVolumeResponse, error) {
					deletes++
					require.Equal(t, "leader", request.DiskId)
					require.True(t, request.GetHeaders().GetExactDiskIdMatch())
					require.True(t, request.Sync)
					return &protos.TDestroyVolumeResponse{}, nil
				},
			}
			factory := &factory{clients: map[string]client{"target-cell": newVolumeLinkTestClient("target-cell", impl)}}
			require.NoError(t, factory.DeleteVolumeLinkTarget(context.Background(), link, "task"))
			if owner == "task" {
				require.Equal(t, 1, deletes)
			} else {
				require.Zero(t, deletes)
			}
		})
	}
}

func TestFinalizeVolumeLinkTargetRemovesMarkersIdempotently(t *testing.T) {
	link := VolumeLink{"leader", "source-cell", "leader", "target-cell"}
	volume := &protos.TVolume{DiskId: "leader", ConfigVersion: 7, Tags: map[string]string{
		volumeLinkTaskIDTag: "task", "source-disk-id": "source-cell/leader",
	}}
	actions := 0
	describes := 0
	impl := &volumeLinkTestClient{
		describe: func(request *protos.TDescribeVolumeRequest) (*protos.TDescribeVolumeResponse, error) {
			describes++
			require.Equal(t, "leader", request.DiskId)
			require.True(t, request.GetHeaders().GetExactDiskIdMatch())
			return &protos.TDescribeVolumeResponse{Volume: volume}, nil
		},
		action: func(request *protos.TExecuteActionRequest) (*protos.TExecuteActionResponse, error) {
			actions++
			require.Equal(t, "ModifyTags", request.Action)
			require.True(t, request.GetHeaders().GetExactDiskIdMatch())
			var input struct {
				DiskID        string   `json:"DiskId"`
				ConfigVersion uint32   `json:"ConfigVersion"`
				TagsToRemove  []string `json:"TagsToRemove"`
			}
			require.NoError(t, json.Unmarshal(request.Input, &input))
			require.Equal(t, "leader", input.DiskID)
			require.Equal(t, uint32(7), input.ConfigVersion)
			require.ElementsMatch(t, []string{"source-disk-id=source-cell/leader", "volume-link-task-id=task"}, input.TagsToRemove)
			for _, tag := range input.TagsToRemove {
				delete(volume.Tags, strings.SplitN(tag, "=", 2)[0])
			}
			return &protos.TExecuteActionResponse{Output: []byte(`{}`)}, nil
		},
	}
	factory := &factory{clients: map[string]client{"target-cell": newVolumeLinkTestClient("target-cell", impl)}}
	require.NoError(t, factory.FinalizeVolumeLinkTarget(context.Background(), link, "task"))
	require.Empty(t, volume.Tags)
	require.Equal(t, 1, describes)
	require.NoError(t, factory.FinalizeVolumeLinkTarget(context.Background(), link, "task"))
	require.Equal(t, 1, actions)
	require.Equal(t, 2, describes)
}

func TestVolumeLinkAdapterClassifiesPermanentClientErrors(t *testing.T) {
	for _, code := range []uint32{nbs_client.E_ARGUMENT, nbs_client.E_NOT_IMPLEMENTED} {
		impl := &volumeLinkTestClient{action: func(request *protos.TExecuteActionRequest) (*protos.TExecuteActionResponse, error) {
			require.True(t, request.GetHeaders().GetExactDiskIdMatch())
			return nil, &nbs_client.ClientError{Code: code}
		}}
		client := newVolumeLinkTestClient("source-cell", impl)
		_, err := client.GetVolumeLinkStatus(context.Background(), VolumeLink{"leader", "source-cell", "follower", "target-cell"})
		require.ErrorIs(t, err, task_errors.NewEmptyNonRetriableError())
	}
	require.False(t, IsVolumeLinkCreateRejected(&nbs_client.ClientError{Code: nbs_client.E_TIMEOUT}))
	require.False(t, IsVolumeLinkCreateRejected(&nbs_client.ClientError{Code: nbs_client.E_FAIL}))
	require.True(t, IsVolumeLinkCreateRejected(&nbs_client.ClientError{Code: nbs_client.E_ARGUMENT}))
	require.True(t, IsVolumeLinkCreateRejected(&nbs_client.ClientError{Code: nbs_client.E_TRY_AGAIN}))
}

func TestFinalizeVolumeLinkTargetRechecksOwnershipAfterConflict(t *testing.T) {
	for _, ownerChanged := range []bool{false, true} {
		name := "same-owner"
		if ownerChanged {
			name = "owner-changed"
		}
		t.Run(name, func(t *testing.T) {
			link := VolumeLink{"leader", "source-cell", "leader", "target-cell"}
			volume := &protos.TVolume{DiskId: "leader", ConfigVersion: 7, Tags: map[string]string{
				volumeLinkTaskIDTag: "task", "source-disk-id": "source-cell/leader",
			}}
			describes, actions := 0, 0
			impl := &volumeLinkTestClient{
				describe: func(request *protos.TDescribeVolumeRequest) (*protos.TDescribeVolumeResponse, error) {
					describes++
					require.Equal(t, "leader", request.DiskId)
					require.True(t, request.GetHeaders().GetExactDiskIdMatch())
					return &protos.TDescribeVolumeResponse{Volume: volume}, nil
				},
				action: func(request *protos.TExecuteActionRequest) (*protos.TExecuteActionResponse, error) {
					actions++
					require.Equal(t, "ModifyTags", request.Action)
					require.True(t, request.GetHeaders().GetExactDiskIdMatch())
					var input struct {
						ConfigVersion uint32 `json:"ConfigVersion"`
					}
					require.NoError(t, json.Unmarshal(request.Input, &input))
					require.Equal(t, volume.ConfigVersion, input.ConfigVersion)
					if actions == 1 {
						volume.ConfigVersion++
						if ownerChanged {
							volume.Tags[volumeLinkTaskIDTag] = "another-task"
						}
						return nil, &nbs_client.ClientError{Code: nbs_client.E_ABORTED}
					}
					delete(volume.Tags, volumeLinkTaskIDTag)
					delete(volume.Tags, "source-disk-id")
					return &protos.TExecuteActionResponse{Output: []byte(`{}`)}, nil
				},
			}
			factory := &factory{clients: map[string]client{"target-cell": newVolumeLinkTestClient("target-cell", impl)}}
			err := factory.FinalizeVolumeLinkTarget(context.Background(), link, "task")
			require.Equal(t, 2, describes)
			if ownerChanged {
				require.ErrorContains(t, err, "ownership changed")
				require.Equal(t, 1, actions)
				require.Equal(t, "another-task", volume.Tags[volumeLinkTaskIDTag])
			} else {
				require.NoError(t, err)
				require.Equal(t, 2, actions)
				require.Empty(t, volume.Tags)
			}
		})
	}
}

func TestResolveVolumeLinkSourceDistinguishesLiteralCopySuffix(t *testing.T) {
	for _, test := range []struct {
		physicalID string
		valid      bool
	}{
		{"volume-copy", true},
		{"volume-copy-copy", true},
		{"volume", false},
		{"volume-copy-copy-copy", false},
	} {
		t.Run(test.physicalID, func(t *testing.T) {
			impl := &volumeLinkTestClient{describe: func(request *protos.TDescribeVolumeRequest) (*protos.TDescribeVolumeResponse, error) {
				require.Equal(t, "volume-copy", request.DiskId)
				require.False(t, request.GetHeaders().GetExactDiskIdMatch())
				return &protos.TDescribeVolumeResponse{Volume: &protos.TVolume{DiskId: test.physicalID}}, nil
			}}
			client := newVolumeLinkTestClient("cell", impl)
			physicalID, err := client.ResolveVolumeLinkSource(context.Background(), "volume-copy")
			if test.valid {
				require.NoError(t, err)
				require.Equal(t, test.physicalID, physicalID)
			} else {
				require.ErrorContains(t, err, "unrelated volume")
			}
		})
	}
}

func TestParseVolumeLinkStatusIgnoresUnusedProgress(t *testing.T) {
	status, err := parseVolumeLinkStatus([]byte(`{"status":"LINK_STATUS_PREPARING","linkUUID":"uuid","migratedBytes":"not-a-number"}`))
	require.NoError(t, err)
	require.Equal(t, VolumeLinkStatus{State: VolumeLinkStatePreparing, LinkUUID: "uuid"}, status)
}

func TestCreateVolumeLinkTargetRejectsOverlay(t *testing.T) {
	link := VolumeLink{"leader", "source-cell", "leader", "target-cell"}
	describes := 0
	creates := 0
	impl := &volumeLinkTestClient{
		describe: func(request *protos.TDescribeVolumeRequest) (*protos.TDescribeVolumeResponse, error) {
			describes++
			require.Equal(t, link.LeaderDiskID, request.DiskId)
			require.True(t, request.GetHeaders().GetExactDiskIdMatch())
			return &protos.TDescribeVolumeResponse{Volume: &protos.TVolume{
				DiskId: link.LeaderDiskID, BaseDiskId: "base-disk",
			}}, nil
		},
		create: func(*protos.TCreateVolumeRequest) (*protos.TCreateVolumeResponse, error) {
			creates++
			return &protos.TCreateVolumeResponse{}, nil
		},
	}
	factory := &factory{clients: map[string]client{
		link.LeaderCellID:   newVolumeLinkTestClient(link.LeaderCellID, impl),
		link.FollowerCellID: newVolumeLinkTestClient(link.FollowerCellID, impl),
	}}

	err := factory.CreateVolumeLinkTarget(context.Background(), link, "task-id")
	require.ErrorContains(t, err, "overlay volumes")
	require.ErrorIs(t, err, task_errors.NewEmptyNonRetriableError())
	require.Equal(t, 1, describes)
	require.Zero(t, creates)
}
