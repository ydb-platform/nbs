package client

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	protos "github.com/ydb-platform/nbs/cloud/blockstore/public/api/protos"
)

////////////////////////////////////////////////////////////////////////////////

const (
	DefaultUnixSocket = "./test.sock"
)

func createTestClient(port string) (*Client, error) {
	return NewClient(
		&GrpcClientOpts{
			Endpoint: fmt.Sprintf("localhost:%v", port),
		},
		&DurableClientOpts{},
		NewStderrLog(LOG_DEBUG),
	)
}

func checkEndpointsCount(client *Client, expectedCount int, t *testing.T) {
	endpoints, err := client.ListEndpoints(context.Background())

	if err != nil {
		t.Fatal(err)
	}

	if len(endpoints) != expectedCount {
		err = fmt.Errorf("Size mismatch: (expected: %v, actual: %v)",
			expectedCount, len(endpoints))
		t.Fatal(err)
	}
}

////////////////////////////////////////////////////////////////////////////////

func TestEndpointRequests(t *testing.T) {
	ctx := context.Background()
	port := os.Getenv("LOCAL_NULL_INSECURE_NBS_SERVER_PORT")

	client, err := createTestClient(port)
	require.NoError(t, err)

	checkEndpointsCount(client, 0, t)

	_, err = client.StartEndpoint(
		ctx,
		DefaultUnixSocket,
		"diskId",
		protos.EClientIpcType_IPC_GRPC,
		"clientId",
		"instanceId",
		protos.EVolumeAccessMode_VOLUME_ACCESS_READ_WRITE,
		protos.EVolumeMountMode_VOLUME_MOUNT_LOCAL,
		3,    // mountSeqNumber
		1,    // vhostQueuesCount
		true) // unalignedRequestsDisabled
	require.NoError(t, err)

	checkEndpointsCount(client, 1, t)

	err = client.StopEndpoint(ctx, DefaultUnixSocket)
	require.NoError(t, err)

	checkEndpointsCount(client, 0, t)
}

func TestQueryAvailableStorage(t *testing.T) {
	ctx := context.Background()
	port := os.Getenv("LOCAL_NULL_INSECURE_NBS_SERVER_PORT")

	client, err := createTestClient(port)
	require.NoError(t, err)

	_, err = client.QueryAvailableStorage(
		ctx,
		[]string{"node"},
	)
	require.NoError(t, err)
}

func TestCreateVolumeFromDevice(t *testing.T) {
	ctx := context.Background()
	port := os.Getenv("LOCAL_NULL_INSECURE_NBS_SERVER_PORT")

	client, err := createTestClient(port)
	require.NoError(t, err)

	err = client.CreateVolumeFromDevice(
		ctx,
		"diskId",
		"agentId",
		"path",
		&CreateVolumeOpts{
			FolderId: "folder",
			CloudId:  "cloud",
		},
	)
	require.NoError(t, err)
}

func TestResumeDevice(t *testing.T) {
	ctx := context.Background()
	port := os.Getenv("LOCAL_NULL_INSECURE_NBS_SERVER_PORT")

	client, err := createTestClient(port)
	require.NoError(t, err)

	err = client.ResumeDevice(
		ctx,
		"agentId",
		"path",
		false, // DryRun
	)
	require.NoError(t, err)
}
