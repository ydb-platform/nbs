package main

import (
	"context"
	"fmt"
	"io/ioutil"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
	nbsgrpc "github.com/ydb-platform/nbs/cloud/blockstore/public/api/grpc"
	"github.com/ydb-platform/nbs/cloud/blockstore/public/api/protos"
	"github.com/ydb-platform/nbs/library/go/test/canon"
	"github.com/ydb-platform/nbs/library/go/test/portmanager"
	"github.com/ydb-platform/nbs/library/go/test/yatest"
	"google.golang.org/grpc"
)

////////////////////////////////////////////////////////////////////////////////

type nbsService struct {
	drStateJSON             []byte
	diskID2FreshDeviceCount map[string]int
}

func (n nbsService) Ping(
	ctx context.Context,
	request *protos.TPingRequest,
) (*protos.TPingResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (n nbsService) UploadClientMetrics(
	ctx context.Context,
	request *protos.TUploadClientMetricsRequest,
) (*protos.TUploadClientMetricsResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (n nbsService) CreateVolume(
	ctx context.Context,
	request *protos.TCreateVolumeRequest,
) (*protos.TCreateVolumeResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (n nbsService) DestroyVolume(
	ctx context.Context,
	request *protos.TDestroyVolumeRequest,
) (*protos.TDestroyVolumeResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (n nbsService) ResizeVolume(
	ctx context.Context,
	request *protos.TResizeVolumeRequest,
) (*protos.TResizeVolumeResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (n nbsService) AlterVolume(
	ctx context.Context,
	request *protos.TAlterVolumeRequest,
) (*protos.TAlterVolumeResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (n nbsService) AssignVolume(
	ctx context.Context,
	request *protos.TAssignVolumeRequest,
) (*protos.TAssignVolumeResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (n nbsService) ListVolumes(
	ctx context.Context,
	request *protos.TListVolumesRequest,
) (*protos.TListVolumesResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (n nbsService) StatVolume(
	ctx context.Context,
	request *protos.TStatVolumeRequest,
) (*protos.TStatVolumeResponse, error) {
	cnt, found := n.diskID2FreshDeviceCount[request.DiskId]
	if found {
		if cnt > 0 {
			cnt -= 1
			n.diskID2FreshDeviceCount[request.DiskId] = cnt
		}
	} else {
		n.diskID2FreshDeviceCount[request.DiskId] = 5
		cnt = 5
	}

	response := protos.TStatVolumeResponse{}
	response.Volume = &protos.TVolume{
		FreshDeviceIds: make([]string, 0),
	}
	for i := 0; i < cnt; i++ {
		response.Volume.FreshDeviceIds = append(
			response.Volume.FreshDeviceIds,
			fmt.Sprintf("fake-fresh-device-%v", i),
		)
	}

	return &response, nil
}

func (n nbsService) DescribeVolume(
	ctx context.Context,
	request *protos.TDescribeVolumeRequest,
) (*protos.TDescribeVolumeResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (n nbsService) DescribeVolumeModel(
	ctx context.Context,
	request *protos.TDescribeVolumeModelRequest,
) (*protos.TDescribeVolumeModelResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (n nbsService) MountVolume(
	ctx context.Context,
	request *protos.TMountVolumeRequest,
) (*protos.TMountVolumeResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (n nbsService) UnmountVolume(
	ctx context.Context,
	request *protos.TUnmountVolumeRequest,
) (*protos.TUnmountVolumeResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (n nbsService) ReadBlocks(
	ctx context.Context,
	request *protos.TReadBlocksRequest,
) (*protos.TReadBlocksResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (n nbsService) WriteBlocks(
	ctx context.Context,
	request *protos.TWriteBlocksRequest,
) (*protos.TWriteBlocksResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (n nbsService) ZeroBlocks(
	ctx context.Context,
	request *protos.TZeroBlocksRequest,
) (*protos.TZeroBlocksResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (n nbsService) CreateCheckpoint(
	ctx context.Context,
	request *protos.TCreateCheckpointRequest,
) (*protos.TCreateCheckpointResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (n nbsService) DeleteCheckpoint(
	ctx context.Context,
	request *protos.TDeleteCheckpointRequest,
) (*protos.TDeleteCheckpointResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (n nbsService) GetCheckpointStatus(
	ctx context.Context,
	request *protos.TGetCheckpointStatusRequest,
) (*protos.TGetCheckpointStatusResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (n nbsService) GetChangedBlocks(
	ctx context.Context,
	request *protos.TGetChangedBlocksRequest,
) (*protos.TGetChangedBlocksResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (n nbsService) DiscoverInstances(
	ctx context.Context,
	request *protos.TDiscoverInstancesRequest,
) (*protos.TDiscoverInstancesResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (n nbsService) StartEndpoint(
	ctx context.Context,
	request *protos.TStartEndpointRequest,
) (*protos.TStartEndpointResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (n nbsService) StopEndpoint(
	ctx context.Context,
	request *protos.TStopEndpointRequest,
) (*protos.TStopEndpointResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (n nbsService) ListEndpoints(
	ctx context.Context,
	request *protos.TListEndpointsRequest,
) (*protos.TListEndpointsResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (n nbsService) KickEndpoint(
	ctx context.Context,
	request *protos.TKickEndpointRequest,
) (*protos.TKickEndpointResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (n nbsService) ListKeyrings(
	ctx context.Context,
	request *protos.TListKeyringsRequest,
) (*protos.TListKeyringsResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (n nbsService) DescribeEndpoint(
	ctx context.Context,
	request *protos.TDescribeEndpointRequest,
) (*protos.TDescribeEndpointResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (n nbsService) RefreshEndpoint(
	ctx context.Context,
	request *protos.TRefreshEndpointRequest,
) (*protos.TRefreshEndpointResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (n nbsService) CancelEndpointInFlightRequests(
	ctx context.Context,
	request *protos.TCancelEndpointInFlightRequestsRequest,
) (*protos.TCancelEndpointInFlightRequestsResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (n nbsService) UpdateDiskRegistryConfig(
	ctx context.Context,
	request *protos.TUpdateDiskRegistryConfigRequest,
) (*protos.TUpdateDiskRegistryConfigResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (n nbsService) DescribeDiskRegistryConfig(
	ctx context.Context,
	request *protos.TDescribeDiskRegistryConfigRequest,
) (*protos.TDescribeDiskRegistryConfigResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (n nbsService) QueryAvailableStorage(
	ctx context.Context,
	request *protos.TQueryAvailableStorageRequest,
) (*protos.TQueryAvailableStorageResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (n nbsService) CreateVolumeFromDevice(
	ctx context.Context,
	request *protos.TCreateVolumeFromDeviceRequest,
) (*protos.TCreateVolumeFromDeviceResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (n nbsService) ResumeDevice(
	ctx context.Context,
	request *protos.TResumeDeviceRequest,
) (*protos.TResumeDeviceResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (n nbsService) QueryAgentsInfo(
	ctx context.Context,
	request *protos.TQueryAgentsInfoRequest,
) (*protos.TQueryAgentsInfoResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (n nbsService) ExecuteAction(
	ctx context.Context,
	request *protos.TExecuteActionRequest,
) (*protos.TExecuteActionResponse, error) {
	if request.Action == "backupdiskregistrystate" {
		var response protos.TExecuteActionResponse
		response.Output = n.drStateJSON
		return &response, nil
	} else if request.Action == "diskregistrychangestate" {
		var response protos.TExecuteActionResponse
		response.Output = []byte(fmt.Sprintf(
			"received request: %v",
			string(request.Input),
		))
		return &response, nil
	}

	return nil, fmt.Errorf("unsupported action: %v", request.Action)
}

func (n nbsService) CreatePlacementGroup(
	ctx context.Context,
	request *protos.TCreatePlacementGroupRequest,
) (*protos.TCreatePlacementGroupResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (n nbsService) DestroyPlacementGroup(
	ctx context.Context,
	request *protos.TDestroyPlacementGroupRequest,
) (*protos.TDestroyPlacementGroupResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (n nbsService) AlterPlacementGroupMembership(
	ctx context.Context,
	request *protos.TAlterPlacementGroupMembershipRequest,
) (*protos.TAlterPlacementGroupMembershipResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (n nbsService) ListPlacementGroups(
	ctx context.Context,
	request *protos.TListPlacementGroupsRequest,
) (*protos.TListPlacementGroupsResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (n nbsService) DescribePlacementGroup(
	ctx context.Context,
	request *protos.TDescribePlacementGroupRequest,
) (*protos.TDescribePlacementGroupResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (n nbsService) CmsAction(
	ctx context.Context,
	request *protos.TCmsActionRequest,
) (*protos.TCmsActionResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (n nbsService) CreateVolumeLink(
	ctx context.Context,
	request *protos.TCreateVolumeLinkRequest,
) (*protos.TCreateVolumeLinkResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (n nbsService) DestroyVolumeLink(
	ctx context.Context,
	request *protos.TDestroyVolumeLinkRequest,
) (*protos.TDestroyVolumeLinkResponse, error) {
	//TODO implement me
	panic("implement me")
}

////////////////////////////////////////////////////////////////////////////////

type TestContext struct {
	Binary    string
	Pm        *portmanager.PortManager
	Port      int
	Srv       *grpc.Server
	NbsServer nbsService
	TmpDir    string
}

func (tc *TestContext) init(t *testing.T, stateFile string) {
	var err error
	tc.Binary, err = yatest.BinaryPath(
		"cloud/blockstore/tools/testing/chaos-monkey/chaos-monkey")
	require.NoError(t, err)

	drStatePath := yatest.SourcePath(
		fmt.Sprintf(
			"cloud/blockstore/tools/testing/chaos-monkey/tests/data/%v.json",
			stateFile,
		),
	)
	drStateJSON, err := ioutil.ReadFile(drStatePath)
	require.NoError(t, err)

	err = os.MkdirAll(os.Getenv("PORT_SYNC_PATH"), os.ModePerm)
	require.NoError(t, err)

	tc.Pm, err = portmanager.New()
	require.NoError(t, err)
	tc.Port, err = tc.Pm.GetPort()
	require.NoError(t, err)
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", tc.Port))
	require.NoError(t, err)
	tc.Srv = grpc.NewServer()
	tc.NbsServer = nbsService{
		drStateJSON:             drStateJSON,
		diskID2FreshDeviceCount: make(map[string]int),
	}
	nbsgrpc.RegisterTBlockStoreServiceServer(tc.Srv, &tc.NbsServer)
	go func() {
		err = tc.Srv.Serve(lis)
		require.NoError(t, err)
	}()

	tc.TmpDir, err = ioutil.TempDir(os.TempDir(), "canon_")
	require.NoError(t, err)
}

func (tc *TestContext) cleanup(t *testing.T) {
	err := os.RemoveAll(tc.TmpDir)
	require.NoError(t, err)

	tc.Srv.GracefulStop()
}

func (tc *TestContext) saveResult(t *testing.T, output []byte) {
	canonFile := filepath.Join(tc.TmpDir, "test.golden")
	err := ioutil.WriteFile(canonFile, output, 0644)
	require.NoError(t, err)

	canon.SaveFile(t, canonFile, canon.WithLocal(true))
}

func doStateAnalysis(t *testing.T, args ...string) {
	binary, err := yatest.BinaryPath(
		"cloud/blockstore/tools/testing/chaos-monkey/chaos-monkey")
	require.NoError(t, err)

	drStatePath := yatest.SourcePath(
		"cloud/blockstore/tools/testing/chaos-monkey/tests/data/dr3.json")

	tmpDir, err := ioutil.TempDir(os.TempDir(), "canon_")
	require.NoError(t, err)
	defer func() { _ = os.RemoveAll(tmpDir) }()

	appendedArgs := append([]string{"--dr-state"}, drStatePath)
	appendedArgs = append(appendedArgs, args...)
	cmd := exec.Command(
		binary,
		appendedArgs...,
	)
	cmd.Stderr = os.Stderr
	output, err := cmd.Output()
	if err != nil {
		println(err)
	}
	require.NoError(t, err)

	canonFile := filepath.Join(tmpDir, "test.golden")
	err = ioutil.WriteFile(canonFile, output, 0644)
	require.NoError(t, err)

	canon.SaveFile(t, canonFile, canon.WithLocal(true))
}

////////////////////////////////////////////////////////////////////////////////

func TestStateAnalysis(t *testing.T) {
	doStateAnalysis(
		t,
		"--disk-id", "fsa0uap9g03n6o91mjnl",
		"--max-targets", "5")
}

func TestStateAnalysisPreferFresh(t *testing.T) {
	doStateAnalysis(
		t,
		"--disk-id", "fsa0uap9g03n6o91mjnl",
		"--max-targets", "5",
		"--prefer-fresh")
}

func TestStateAnalysisPreferMinusOne(t *testing.T) {
	doStateAnalysis(
		t,
		"--disk-id", "fsa0uap9g03n6o91mjnl",
		"--max-targets", "5",
		"--prefer-minus-one")
}

func TestStateAnalysisBreakTwo(t *testing.T) {
	doStateAnalysis(
		t,
		"--disk-id", "fsa0uap9g03n6o91mjnl",
		"--max-targets", "2",
		"--can-break-two")
}

func TestStateAnalysisBreakDevice(t *testing.T) {
	doStateAnalysis(
		t,
		"--disk-id", "fsa0uap9g03n6o91mjnl",
		"--device-id", "bb0fbae0ae5c1285b29ad657069a8abb")
}

func TestStateAnalysisHealDevice(t *testing.T) {
	doStateAnalysis(
		t,
		"--disk-id", "fsa0uap9g03n6o91mjnl",
		"--device-id", "0750cecf67723c52d8b6501e882f9673")
}

////////////////////////////////////////////////////////////////////////////////

func TestFetchAnalyzeApply(t *testing.T) {
	tc := TestContext{}
	tc.init(t, "dr")
	defer tc.cleanup(t)

	cmd := exec.Command(
		tc.Binary,
		"--nbs-host",
		"localhost",
		"--nbs-port",
		strconv.Itoa(tc.Port),
		"--disk-id", "cga7mpius6uopt9beskc",
		"--max-targets", "5",
		"--apply",
		"--wait-for-replication-start",
		"--wait-for-replication-finish",
	)
	cmd.Stderr = os.Stderr
	output, err := cmd.Output()
	if err != nil {
		println(err)
	}
	require.NoError(t, err)

	tc.saveResult(t, output)
}

func TestCleanup(t *testing.T) {
	tc := TestContext{}
	tc.init(t, "dr2")
	defer tc.cleanup(t)

	cmd := exec.Command(
		tc.Binary,
		"--nbs-host",
		"localhost",
		"--nbs-port",
		strconv.Itoa(tc.Port),
		"--cleanup",
	)
	cmd.Stderr = os.Stderr
	output, err := cmd.Output()
	if err != nil {
		println(err)
	}
	require.NoError(t, err)

	tc.saveResult(t, output)
}
