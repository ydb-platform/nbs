package client

import (
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	protos "github.com/ydb-platform/nbs/cloud/filestore/public/api/protos"
	"golang.org/x/net/context"
)

////////////////////////////////////////////////////////////////////////////////

func createTestGrpcClient(port string, creds *ClientCredentials) (ClientIface, error) {
	return NewGrpcClient(
		&GrpcClientOpts{
			Endpoint:    fmt.Sprintf("localhost:%s", port),
			Credentials: creds,
		},
		NewStderrLog(LOG_DEBUG),
	)
}

////////////////////////////////////////////////////////////////////////////////

func TestPing(t *testing.T) {
	port := os.Getenv("NFS_SERVER_PORT")
	grpcClient, err := createTestGrpcClient(port, nil)
	assert.NoError(t, err)

	_, err = grpcClient.Ping(context.TODO(), &protos.TPingRequest{})
	assert.NoError(t, err)
}

////////////////////////////////////////////////////////////////////////////////

func createTestGrpcEndpointClient(port string, creds *ClientCredentials) (EndpointClientIface, error) {
	return NewGrpcEndpointClient(
		&GrpcClientOpts{
			Endpoint:    fmt.Sprintf("localhost:%s", port),
			Credentials: creds,
		},
		NewStderrLog(LOG_DEBUG),
	)
}

////////////////////////////////////////////////////////////////////////////////

func TestListEndpoints(t *testing.T) {
	port := os.Getenv("NFS_VHOST_PORT")
	grpcClient, err := createTestGrpcEndpointClient(port, nil)
	assert.NoError(t, err)

	_, err = grpcClient.ListEndpoints(context.TODO(), &protos.TListEndpointsRequest{})
	assert.NoError(t, err)
}
