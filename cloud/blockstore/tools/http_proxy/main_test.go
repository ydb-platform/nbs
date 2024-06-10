package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	blockstore_config "github.com/ydb-platform/nbs/cloud/blockstore/config"
	blockstore_grpc "github.com/ydb-platform/nbs/cloud/blockstore/public/api/grpc"
	blockstore_protos "github.com/ydb-platform/nbs/cloud/blockstore/public/api/protos"
	storage_protos "github.com/ydb-platform/nbs/cloud/storage/core/protos"
	"github.com/ydb-platform/nbs/library/go/test/yatest"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	grpc_metadata "google.golang.org/grpc/metadata"
)

////////////////////////////////////////////////////////////////////////////////

const (
	maxRetryCount   = 5
	backoffDuration = time.Second
)

////////////////////////////////////////////////////////////////////////////////

func runRequestWithRetries(
	c *http.Client,
	req *http.Request,
) (*http.Response, error) {

	runRequest := func() (*http.Response, error) {
		resp, err := c.Do(req)
		if err != nil {
			return nil, err
		}

		if resp.StatusCode != 200 {
			err := resp.Body.Close()
			if err != nil {
				log.Fatalf("failed to close response body: %v", err)
			}
			return nil, fmt.Errorf("%v", resp.Status)
		}

		return resp, nil
	}

	for i := 1; i < maxRetryCount; i++ {
		resp, err := runRequest()
		if err == nil {
			return resp, nil
		}

		<-time.After(backoffDuration * time.Duration(i))
	}

	return runRequest()
}

////////////////////////////////////////////////////////////////////////////////

func getCertFilePath() string {
	return yatest.SourcePath("cloud/blockstore/tests/certs/server.crt")
}

func getCertPrivateKeyFilePath() string {
	return yatest.SourcePath("cloud/blockstore/tests/certs/server.key")
}

////////////////////////////////////////////////////////////////////////////////

func getPort(addr string) (uint32, error) {
	_, portStr, err := net.SplitHostPort(addr)
	if err != nil {
		return 0, fmt.Errorf(
			"failed to parse port out of addr %v: %w",
			addr,
			err,
		)
	}

	port, err := strconv.Atoi(portStr)
	if err != nil {
		return 0, fmt.Errorf(
			"failed to parse port '%v' as an int: %w",
			portStr,
			err,
		)
	}

	return uint32(port), nil
}

////////////////////////////////////////////////////////////////////////////////

type mockBlockstoreServer struct {
	mock.Mock
}

func (s *mockBlockstoreServer) Ping(
	ctx context.Context,
	req *blockstore_protos.TPingRequest,
) (*blockstore_protos.TPingResponse, error) {

	args := s.Called(ctx, req)
	res, _ := args.Get(0).(*blockstore_protos.TPingResponse)
	return res, args.Error(1)
}

func (s *mockBlockstoreServer) UploadClientMetrics(
	ctx context.Context,
	req *blockstore_protos.TUploadClientMetricsRequest,
) (*blockstore_protos.TUploadClientMetricsResponse, error) {

	args := s.Called(ctx, req)
	res, _ := args.Get(0).(*blockstore_protos.TUploadClientMetricsResponse)
	return res, args.Error(1)
}

func (s *mockBlockstoreServer) CreateVolume(
	ctx context.Context,
	req *blockstore_protos.TCreateVolumeRequest,
) (*blockstore_protos.TCreateVolumeResponse, error) {

	args := s.Called(ctx, req)
	res, _ := args.Get(0).(*blockstore_protos.TCreateVolumeResponse)
	return res, args.Error(1)
}

func (s *mockBlockstoreServer) DestroyVolume(
	ctx context.Context,
	req *blockstore_protos.TDestroyVolumeRequest,
) (*blockstore_protos.TDestroyVolumeResponse, error) {

	args := s.Called(ctx, req)
	res, _ := args.Get(0).(*blockstore_protos.TDestroyVolumeResponse)
	return res, args.Error(1)
}

func (s *mockBlockstoreServer) ResizeVolume(
	ctx context.Context,
	req *blockstore_protos.TResizeVolumeRequest,
) (*blockstore_protos.TResizeVolumeResponse, error) {

	args := s.Called(ctx, req)
	res, _ := args.Get(0).(*blockstore_protos.TResizeVolumeResponse)
	return res, args.Error(1)
}

func (s *mockBlockstoreServer) AlterVolume(
	ctx context.Context,
	req *blockstore_protos.TAlterVolumeRequest,
) (*blockstore_protos.TAlterVolumeResponse, error) {

	args := s.Called(ctx, req)
	res, _ := args.Get(0).(*blockstore_protos.TAlterVolumeResponse)
	return res, args.Error(1)
}

func (s *mockBlockstoreServer) AssignVolume(
	ctx context.Context,
	req *blockstore_protos.TAssignVolumeRequest,
) (*blockstore_protos.TAssignVolumeResponse, error) {

	args := s.Called(ctx, req)
	res, _ := args.Get(0).(*blockstore_protos.TAssignVolumeResponse)
	return res, args.Error(1)
}

func (s *mockBlockstoreServer) StatVolume(
	ctx context.Context,
	req *blockstore_protos.TStatVolumeRequest,
) (*blockstore_protos.TStatVolumeResponse, error) {

	args := s.Called(ctx, req)
	res, _ := args.Get(0).(*blockstore_protos.TStatVolumeResponse)
	return res, args.Error(1)
}

func (s *mockBlockstoreServer) QueryAvailableStorage(
	ctx context.Context,
	req *blockstore_protos.TQueryAvailableStorageRequest,
) (*blockstore_protos.TQueryAvailableStorageResponse, error) {

	args := s.Called(ctx, req)
	res, _ := args.Get(0).(*blockstore_protos.TQueryAvailableStorageResponse)
	return res, args.Error(1)
}

func (s *mockBlockstoreServer) ResumeDevice(
	ctx context.Context,
	req *blockstore_protos.TResumeDeviceRequest,
) (*blockstore_protos.TResumeDeviceResponse, error) {

	args := s.Called(ctx, req)
	res, _ := args.Get(0).(*blockstore_protos.TResumeDeviceResponse)
	return res, args.Error(1)
}

func (s *mockBlockstoreServer) CreateVolumeFromDevice(
	ctx context.Context,
	req *blockstore_protos.TCreateVolumeFromDeviceRequest,
) (*blockstore_protos.TCreateVolumeFromDeviceResponse, error) {

	args := s.Called(ctx, req)
	res, _ := args.Get(0).(*blockstore_protos.TCreateVolumeFromDeviceResponse)
	return res, args.Error(1)
}

func (s *mockBlockstoreServer) MountVolume(
	ctx context.Context,
	req *blockstore_protos.TMountVolumeRequest,
) (*blockstore_protos.TMountVolumeResponse, error) {

	args := s.Called(ctx, req)
	res, _ := args.Get(0).(*blockstore_protos.TMountVolumeResponse)
	return res, args.Error(1)
}

func (s *mockBlockstoreServer) UnmountVolume(
	ctx context.Context,
	req *blockstore_protos.TUnmountVolumeRequest,
) (*blockstore_protos.TUnmountVolumeResponse, error) {

	args := s.Called(ctx, req)
	res, _ := args.Get(0).(*blockstore_protos.TUnmountVolumeResponse)
	return res, args.Error(1)
}

func (s *mockBlockstoreServer) ReadBlocks(
	ctx context.Context,
	req *blockstore_protos.TReadBlocksRequest,
) (*blockstore_protos.TReadBlocksResponse, error) {

	args := s.Called(ctx, req)
	res, _ := args.Get(0).(*blockstore_protos.TReadBlocksResponse)
	return res, args.Error(1)
}

func (s *mockBlockstoreServer) WriteBlocks(
	ctx context.Context,
	req *blockstore_protos.TWriteBlocksRequest,
) (*blockstore_protos.TWriteBlocksResponse, error) {

	args := s.Called(ctx, req)
	res, _ := args.Get(0).(*blockstore_protos.TWriteBlocksResponse)
	return res, args.Error(1)
}

func (s *mockBlockstoreServer) ZeroBlocks(
	ctx context.Context,
	req *blockstore_protos.TZeroBlocksRequest,
) (*blockstore_protos.TZeroBlocksResponse, error) {

	args := s.Called(ctx, req)
	res, _ := args.Get(0).(*blockstore_protos.TZeroBlocksResponse)
	return res, args.Error(1)
}

func (s *mockBlockstoreServer) CreateCheckpoint(
	ctx context.Context,
	req *blockstore_protos.TCreateCheckpointRequest,
) (*blockstore_protos.TCreateCheckpointResponse, error) {

	args := s.Called(ctx, req)
	res, _ := args.Get(0).(*blockstore_protos.TCreateCheckpointResponse)
	return res, args.Error(1)
}

func (s *mockBlockstoreServer) DeleteCheckpoint(
	ctx context.Context,
	req *blockstore_protos.TDeleteCheckpointRequest,
) (*blockstore_protos.TDeleteCheckpointResponse, error) {

	args := s.Called(ctx, req)
	res, _ := args.Get(0).(*blockstore_protos.TDeleteCheckpointResponse)
	return res, args.Error(1)
}

func (s *mockBlockstoreServer) GetCheckpointStatus(
	ctx context.Context,
	req *blockstore_protos.TGetCheckpointStatusRequest,
) (*blockstore_protos.TGetCheckpointStatusResponse, error) {

	args := s.Called(ctx, req)
	res, _ := args.Get(0).(*blockstore_protos.TGetCheckpointStatusResponse)
	return res, args.Error(1)
}

func (s *mockBlockstoreServer) GetChangedBlocks(
	ctx context.Context,
	req *blockstore_protos.TGetChangedBlocksRequest,
) (*blockstore_protos.TGetChangedBlocksResponse, error) {

	args := s.Called(ctx, req)
	res, _ := args.Get(0).(*blockstore_protos.TGetChangedBlocksResponse)
	return res, args.Error(1)
}

func (s *mockBlockstoreServer) DescribeVolume(
	ctx context.Context,
	req *blockstore_protos.TDescribeVolumeRequest,
) (*blockstore_protos.TDescribeVolumeResponse, error) {

	args := s.Called(ctx, req)
	res, _ := args.Get(0).(*blockstore_protos.TDescribeVolumeResponse)
	return res, args.Error(1)
}

func (s *mockBlockstoreServer) ListVolumes(
	ctx context.Context,
	req *blockstore_protos.TListVolumesRequest,
) (*blockstore_protos.TListVolumesResponse, error) {

	args := s.Called(ctx, req)
	res, _ := args.Get(0).(*blockstore_protos.TListVolumesResponse)
	return res, args.Error(1)
}

func (s *mockBlockstoreServer) DescribeVolumeModel(
	ctx context.Context,
	req *blockstore_protos.TDescribeVolumeModelRequest,
) (*blockstore_protos.TDescribeVolumeModelResponse, error) {

	args := s.Called(ctx, req)
	res, _ := args.Get(0).(*blockstore_protos.TDescribeVolumeModelResponse)
	return res, args.Error(1)
}

func (s *mockBlockstoreServer) DiscoverInstances(
	ctx context.Context,
	req *blockstore_protos.TDiscoverInstancesRequest,
) (*blockstore_protos.TDiscoverInstancesResponse, error) {

	args := s.Called(ctx, req)
	res, _ := args.Get(0).(*blockstore_protos.TDiscoverInstancesResponse)
	return res, args.Error(1)
}

func (s *mockBlockstoreServer) StartEndpoint(
	ctx context.Context,
	req *blockstore_protos.TStartEndpointRequest,
) (*blockstore_protos.TStartEndpointResponse, error) {

	args := s.Called(ctx, req)
	res, _ := args.Get(0).(*blockstore_protos.TStartEndpointResponse)
	return res, args.Error(1)
}

func (s *mockBlockstoreServer) StopEndpoint(
	ctx context.Context,
	req *blockstore_protos.TStopEndpointRequest,
) (*blockstore_protos.TStopEndpointResponse, error) {

	args := s.Called(ctx, req)
	res, _ := args.Get(0).(*blockstore_protos.TStopEndpointResponse)
	return res, args.Error(1)
}

func (s *mockBlockstoreServer) ListEndpoints(
	ctx context.Context,
	req *blockstore_protos.TListEndpointsRequest,
) (*blockstore_protos.TListEndpointsResponse, error) {

	args := s.Called(ctx, req)
	res, _ := args.Get(0).(*blockstore_protos.TListEndpointsResponse)
	return res, args.Error(1)
}

func (s *mockBlockstoreServer) KickEndpoint(
	ctx context.Context,
	req *blockstore_protos.TKickEndpointRequest,
) (*blockstore_protos.TKickEndpointResponse, error) {

	args := s.Called(ctx, req)
	res, _ := args.Get(0).(*blockstore_protos.TKickEndpointResponse)
	return res, args.Error(1)
}

func (s *mockBlockstoreServer) ListKeyrings(
	ctx context.Context,
	req *blockstore_protos.TListKeyringsRequest,
) (*blockstore_protos.TListKeyringsResponse, error) {

	args := s.Called(ctx, req)
	res, _ := args.Get(0).(*blockstore_protos.TListKeyringsResponse)
	return res, args.Error(1)
}

func (s *mockBlockstoreServer) DescribeEndpoint(
	ctx context.Context,
	req *blockstore_protos.TDescribeEndpointRequest,
) (*blockstore_protos.TDescribeEndpointResponse, error) {

	args := s.Called(ctx, req)
	res, _ := args.Get(0).(*blockstore_protos.TDescribeEndpointResponse)
	return res, args.Error(1)
}

func (s *mockBlockstoreServer) RefreshEndpoint(
	ctx context.Context,
	req *blockstore_protos.TRefreshEndpointRequest,
) (*blockstore_protos.TRefreshEndpointResponse, error) {

	args := s.Called(ctx, req)
	res, _ := args.Get(0).(*blockstore_protos.TRefreshEndpointResponse)
	return res, args.Error(1)
}

func (s *mockBlockstoreServer) UpdateDiskRegistryConfig(
	ctx context.Context,
	req *blockstore_protos.TUpdateDiskRegistryConfigRequest,
) (*blockstore_protos.TUpdateDiskRegistryConfigResponse, error) {

	args := s.Called(ctx, req)
	res, _ := args.Get(0).(*blockstore_protos.TUpdateDiskRegistryConfigResponse)
	return res, args.Error(1)
}

func (s *mockBlockstoreServer) DescribeDiskRegistryConfig(
	ctx context.Context,
	req *blockstore_protos.TDescribeDiskRegistryConfigRequest,
) (*blockstore_protos.TDescribeDiskRegistryConfigResponse, error) {

	args := s.Called(ctx, req)
	res, _ := args.Get(0).(*blockstore_protos.TDescribeDiskRegistryConfigResponse)
	return res, args.Error(1)
}

func (s *mockBlockstoreServer) ExecuteAction(
	ctx context.Context,
	req *blockstore_protos.TExecuteActionRequest,
) (*blockstore_protos.TExecuteActionResponse, error) {

	args := s.Called(ctx, req)
	res, _ := args.Get(0).(*blockstore_protos.TExecuteActionResponse)
	return res, args.Error(1)
}

func (s *mockBlockstoreServer) CreatePlacementGroup(
	ctx context.Context,
	req *blockstore_protos.TCreatePlacementGroupRequest,
) (*blockstore_protos.TCreatePlacementGroupResponse, error) {

	args := s.Called(ctx, req)
	res, _ := args.Get(0).(*blockstore_protos.TCreatePlacementGroupResponse)
	return res, args.Error(1)
}

func (s *mockBlockstoreServer) DestroyPlacementGroup(
	ctx context.Context,
	req *blockstore_protos.TDestroyPlacementGroupRequest,
) (*blockstore_protos.TDestroyPlacementGroupResponse, error) {

	args := s.Called(ctx, req)
	res, _ := args.Get(0).(*blockstore_protos.TDestroyPlacementGroupResponse)
	return res, args.Error(1)
}

func (s *mockBlockstoreServer) AlterPlacementGroupMembership(
	ctx context.Context,
	req *blockstore_protos.TAlterPlacementGroupMembershipRequest,
) (*blockstore_protos.TAlterPlacementGroupMembershipResponse, error) {

	args := s.Called(ctx, req)
	res, _ := args.Get(0).(*blockstore_protos.TAlterPlacementGroupMembershipResponse)
	return res, args.Error(1)
}

func (s *mockBlockstoreServer) ListPlacementGroups(
	ctx context.Context,
	req *blockstore_protos.TListPlacementGroupsRequest,
) (*blockstore_protos.TListPlacementGroupsResponse, error) {

	args := s.Called(ctx, req)
	res, _ := args.Get(0).(*blockstore_protos.TListPlacementGroupsResponse)
	return res, args.Error(1)
}

func (s *mockBlockstoreServer) DescribePlacementGroup(
	ctx context.Context,
	req *blockstore_protos.TDescribePlacementGroupRequest,
) (*blockstore_protos.TDescribePlacementGroupResponse, error) {

	args := s.Called(ctx, req)
	res, _ := args.Get(0).(*blockstore_protos.TDescribePlacementGroupResponse)
	return res, args.Error(1)
}

func (s *mockBlockstoreServer) CmsAction(
	ctx context.Context,
	req *blockstore_protos.TCmsActionRequest,
) (*blockstore_protos.TCmsActionResponse, error) {

	args := s.Called(ctx, req)
	res, _ := args.Get(0).(*blockstore_protos.TCmsActionResponse)
	return res, args.Error(1)
}

func (s *mockBlockstoreServer) QueryAgentsInfo(
	ctx context.Context,
	req *blockstore_protos.TQueryAgentsInfoRequest,
) (*blockstore_protos.TQueryAgentsInfoResponse, error) {

	args := s.Called(ctx, req)
	res, _ := args.Get(0).(*blockstore_protos.TQueryAgentsInfoResponse)
	return res, args.Error(1)
}

////////////////////////////////////////////////////////////////////////////////

func runMockBlockstoreServer(
	nbsInsecure bool,
) (*mockBlockstoreServer, uint32, func(), error) {

	service := &mockBlockstoreServer{}

	var creds credentials.TransportCredentials
	var err error
	if !nbsInsecure {
		creds, err = credentials.NewServerTLSFromFile(
			getCertFilePath(),
			getCertPrivateKeyFilePath(),
		)
		if err != nil {
			return nil, 0, nil, fmt.Errorf("failed to create creds: %w", err)
		}
	}

	server := grpc.NewServer(
		grpc.Creds(creds),
	)
	blockstore_grpc.RegisterTBlockStoreServiceServer(server, service)

	lis, err := net.Listen("tcp", ":0")
	if err != nil {
		return nil, 0, nil, fmt.Errorf("failed to listen: %w", err)
	}

	port, err := getPort(lis.Addr().String())
	if err != nil {
		return nil, 0, nil, err
	}

	log.Printf("Running mockBlockstoreServer on port %v", port)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()

		err := server.Serve(lis)
		if err != nil {
			log.Fatalf("Error serving grpc: %v", err)
		}
	}()

	return service, port, func() {
		server.Stop()
		wg.Wait()
	}, nil
}

////////////////////////////////////////////////////////////////////////////////

type httpProxyClient struct {
	client *http.Client
	addr   string
}

func createSecureHTTPProxyClient(port uint32) (*httpProxyClient, error) {
	b, err := ioutil.ReadFile(getCertFilePath())
	if err != nil {
		return nil, err
	}
	cp := x509.NewCertPool()
	if !cp.AppendCertsFromPEM(b) {
		return nil, fmt.Errorf("failed to append credentials")
	}

	return &httpProxyClient{
		client: &http.Client{
			Transport: &http.Transport{
				TLSClientConfig: &tls.Config{
					RootCAs: cp,
				},
			},
		},
		addr: fmt.Sprintf("https://localhost:%v", port),
	}, nil
}

func createInsecureHTTPProxyClient(port uint32) (*httpProxyClient, error) {
	return &httpProxyClient{
		client: &http.Client{},
		addr:   fmt.Sprintf("http://localhost:%v", port),
	}, nil
}

func (c *httpProxyClient) run(
	path string,
	body string,
	headers map[string]string,
) (string, error) {

	req, err := http.NewRequest(
		"POST",
		fmt.Sprintf("%v/%v", c.addr, path),
		strings.NewReader(body),
	)
	if err != nil {
		return "", fmt.Errorf("failed to create request: %w", err)
	}

	for k, v := range headers {
		req.Header.Add(k, v)
	}

	resp, err := runRequestWithRetries(c.client, req)
	if err != nil {
		return "", fmt.Errorf("failed to run request: %v", err)
	}
	defer resp.Body.Close()

	respBody, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("failed to read response body: %w", err)
	}

	return string(respBody), nil
}

////////////////////////////////////////////////////////////////////////////////

func runHTTPProxyServer(
	nbsPort uint32,
	nbsInsecure bool,
) (*httpProxyClient, *httpProxyClient, func(), error) {

	certFile := new(string)
	*certFile = getCertFilePath()
	certPrivateKeyFile := new(string)
	*certPrivateKeyFile = getCertPrivateKeyFilePath()
	host := new(string)
	*host = "localhost"
	config := &blockstore_config.THttpProxyConfig{
		SecurePort: new(uint32),
		Port:       new(uint32),
		Certs: []*storage_protos.TCertificate{
			&storage_protos.TCertificate{
				CertFile:           certFile,
				CertPrivateKeyFile: certPrivateKeyFile,
			},
		},
		NbsServerHost:     host,
		NbsServerPort:     &nbsPort,
		NbsServerCertFile: certFile,
		NbsServerInsecure: &nbsInsecure,
	}
	mux, err := createProxyMux(context.Background(), config)
	if err != nil {
		return nil, nil, nil, err
	}

	insecureServer, err := runInsecureServer(mux, config)
	if err != nil {
		return nil, nil, nil, err
	}

	insecurePort, err := getPort(insecureServer.addr)
	if err != nil {
		return nil, nil, nil, err
	}

	insecureClient, err := createInsecureHTTPProxyClient(insecurePort)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to create insecure HTTP client: %w", err)
	}

	secureServer, err := runSecureServer(mux, config)
	if err != nil {
		return nil, nil, nil, err
	}

	securePort, err := getPort(secureServer.addr)
	if err != nil {
		return nil, nil, nil, err
	}

	secureClient, err := createSecureHTTPProxyClient(securePort)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to create secure HTTP client: %w", err)
	}

	return insecureClient, secureClient, func() {
		secureServer.kill()
		insecureServer.kill()
		secureServer.wait()
		insecureServer.wait()
	}, nil
}

////////////////////////////////////////////////////////////////////////////////

func runServers(
	nbsInsecure bool,
) (*mockBlockstoreServer, *httpProxyClient, *httpProxyClient, func(), error) {

	srv, nbsPort, closeBlockstoreServer, err := runMockBlockstoreServer(nbsInsecure)
	if err != nil {
		return nil, nil, nil, nil, err
	}

	insecureClient, secureClient, closeHTTPProxyServer, err := runHTTPProxyServer(nbsPort, nbsInsecure)
	if err != nil {
		closeBlockstoreServer()
		return nil, nil, nil, nil, err
	}

	return srv, insecureClient, secureClient, func() {
		closeHTTPProxyServer()
		closeBlockstoreServer()
	}, nil
}

type runServersType func() (*mockBlockstoreServer, *httpProxyClient, func(), error)

////////////////////////////////////////////////////////////////////////////////

func containsHeaders(
	t *testing.T,
	headers map[string]string,
) func(context.Context) bool {

	return func(ctx context.Context) bool {
		md, ok := grpc_metadata.FromIncomingContext(ctx)
		if !ok {
			return assert.ElementsMatch(t, map[string]string{}, headers)
		}
		for k, v := range headers {
			ok = assert.Equal(t, []string{v}, md[k]) && ok
		}

		return ok
	}
}

////////////////////////////////////////////////////////////////////////////////

func testPing(t *testing.T, run runServersType) {
	srv, client, closeServers, err := run()
	require.NoError(t, err)
	defer closeServers()

	srv.On(
		"Ping",
		mock.MatchedBy(containsHeaders(t, map[string]string{})),
		&blockstore_protos.TPingRequest{},
	).Times(1).Return(&blockstore_protos.TPingResponse{}, nil)

	body, err := client.run("ping", "", map[string]string{})
	mock.AssertExpectationsForObjects(t, srv)
	assert.NoError(t, err)
	assert.Equal(t, body, "{}")
}

func testMountVolumeWithBody(t *testing.T, run runServersType) {
	srv, client, closeServers, err := run()
	require.NoError(t, err)
	defer closeServers()

	srv.On(
		"MountVolume",
		mock.MatchedBy(containsHeaders(t, map[string]string{})),
		&blockstore_protos.TMountVolumeRequest{
			DiskId: "vol0",
		}).Times(1).Return(&blockstore_protos.TMountVolumeResponse{
		SessionId: "session_id",
	}, nil)

	body, err := client.run(
		"mount_volume",
		"{ \"DiskId\": \"vol0\" }",
		map[string]string{},
	)
	mock.AssertExpectationsForObjects(t, srv)
	assert.NoError(t, err)
	assert.Equal(t, body, "{\"SessionId\":\"session_id\"}")
}

func testHeaders(t *testing.T, run runServersType) {
	srv, client, closeServers, err := run()
	require.NoError(t, err)
	defer closeServers()

	srv.On(
		"Ping",
		mock.MatchedBy(containsHeaders(t, map[string]string{
			"authorization":   "Bearer AUTH_TOKEN",
			"x-nbs-client-id": "SOME_CLIENT",
		})),
		&blockstore_protos.TPingRequest{},
	).Times(1).Return(&blockstore_protos.TPingResponse{}, nil)

	body, err := client.run("ping", "", map[string]string{
		"authorization":   "Bearer AUTH_TOKEN",
		"x-nbs-client-id": "SOME_CLIENT",
	})
	mock.AssertExpectationsForObjects(t, srv)
	assert.NoError(t, err)
	assert.Equal(t, body, "{}")
}

////////////////////////////////////////////////////////////////////////////////

func runSecureNbsInsecureProxy() (*mockBlockstoreServer, *httpProxyClient, func(), error) {
	srv, client, _, closeServers, err := runServers(false)
	return srv, client, closeServers, err
}

func TestPingSecureNbsInsecureProxy(t *testing.T) {
	testPing(t, runSecureNbsInsecureProxy)
}

func TestMountVolumeWithBodySecureNbsInsecureProxy(t *testing.T) {
	testMountVolumeWithBody(t, runSecureNbsInsecureProxy)
}

func TestHeadersSecureNbsInsecureProxy(t *testing.T) {
	testHeaders(t, runSecureNbsInsecureProxy)
}

////////////////////////////////////////////////////////////////////////////////

func runInsecureNbsInsecureProxy() (*mockBlockstoreServer, *httpProxyClient, func(), error) {
	srv, client, _, closeServers, err := runServers(true)
	return srv, client, closeServers, err
}

func TestPingInsecureNbsInsecureProxy(t *testing.T) {
	testPing(t, runInsecureNbsInsecureProxy)
}

func TestMountVolumeWithBodyInsecureNbsInsecureProxy(t *testing.T) {
	testMountVolumeWithBody(t, runInsecureNbsInsecureProxy)
}

func TestHeadersInsecureNbsInsecureProxy(t *testing.T) {
	testHeaders(t, runInsecureNbsInsecureProxy)
}

////////////////////////////////////////////////////////////////////////////////

func runSecureNbsSecureProxy() (*mockBlockstoreServer, *httpProxyClient, func(), error) {
	srv, _, client, closeServers, err := runServers(false)
	return srv, client, closeServers, err
}

func TestPingSecureNbsSecureProxy(t *testing.T) {
	testPing(t, runSecureNbsSecureProxy)
}

func TestMountVolumeWithBodySecureNbsSecureProxy(t *testing.T) {
	testMountVolumeWithBody(t, runSecureNbsSecureProxy)
}

func TestHeadersSecureNbsSecureProxy(t *testing.T) {
	testHeaders(t, runSecureNbsSecureProxy)
}

////////////////////////////////////////////////////////////////////////////////

func runInsecureNbsSecureProxy() (*mockBlockstoreServer, *httpProxyClient, func(), error) {
	srv, _, client, closeServers, err := runServers(true)
	return srv, client, closeServers, err
}

func TestPingInsecureNbsSecureProxy(t *testing.T) {
	testPing(t, runInsecureNbsSecureProxy)
}

func TestMountVolumeWithBodyInsecureNbsSecureProxy(t *testing.T) {
	testMountVolumeWithBody(t, runInsecureNbsSecureProxy)
}

func TestHeadersInsecureNbsSecureProxy(t *testing.T) {
	testHeaders(t, runInsecureNbsSecureProxy)
}
