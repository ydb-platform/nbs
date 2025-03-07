package client

import (
	"fmt"
	"os"
	"testing"

	"github.com/ydb-platform/nbs/cloud/blockstore/public/api/protos"
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

func TestPingWithInsecureChannel(t *testing.T) {
	port := os.Getenv("LOCAL_NULL_INSECURE_NBS_SERVER_PORT")
	grpcClient, err := createTestGrpcClient(port, nil)
	if err != nil {
		t.Error(err)
		return
	}

	_, err = grpcClient.Ping(context.TODO(), &protos.TPingRequest{})
	if err != nil {
		t.Error(err)
	}
}

func TestPingWithSecureChannel(t *testing.T) {
	port := os.Getenv("LOCAL_NULL_SECURE_NBS_SERVER_PORT")
	certFilesDir := os.Getenv("TEST_CERT_FILES_DIR")
	creds := &ClientCredentials{
		RootCertsFile: fmt.Sprintf("%s/server.crt", certFilesDir),
		AuthToken:     "test",
	}

	grpcClient, err := createTestGrpcClient(port, creds)
	if err != nil {
		t.Error(err)
		return
	}

	_, err = grpcClient.Ping(context.TODO(), &protos.TPingRequest{})
	if err != nil {
		t.Error(err)
	}
}

func TestPingWithSecureChannelAndCustomCertificate(t *testing.T) {
	port := os.Getenv("LOCAL_NULL_SECURE_NBS_SERVER_PORT")
	certFilesDir := os.Getenv("TEST_CERT_FILES_DIR")
	creds := &ClientCredentials{
		RootCertsFile:      fmt.Sprintf("%s/server.crt", certFilesDir),
		CertFile:           fmt.Sprintf("%s/server.crt", certFilesDir),
		CertPrivateKeyFile: fmt.Sprintf("%s/server.key", certFilesDir),
		AuthToken:          "test",
	}

	grpcClient, err := createTestGrpcClient(port, creds)
	if err != nil {
		t.Error(err)
		return
	}

	_, err = grpcClient.Ping(context.TODO(), &protos.TPingRequest{})
	if err != nil {
		t.Error(err)
	}
}
