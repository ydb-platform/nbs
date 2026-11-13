package disk_service

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/logging"
	compute "github.com/ydb-platform/nbs/contrib/ydb/public/api/client/yc_private/compute"
	api "github.com/ydb-platform/nbs/contrib/ydb/public/api/client/yc_private/compute/inner"
)

type exportCredentialsService struct {
	api.UnimplementedDiskServiceServer
	requests chan *api.GetExportCredentialsRequest
}

func (s *exportCredentialsService) GetExportCredentials(
	ctx context.Context,
	request *api.GetExportCredentialsRequest,
) (*api.GetExportCredentialsResponse, error) {

	s.requests <- request
	return &api.GetExportCredentialsResponse{
		Credentials: &compute.ExportS3Credentials{
			AccessKeyId: "id", SecretAccessKey: "secret",
		},
	}, nil
}

func TestExportCredentialsIdentifyResourceKind(t *testing.T) {
	listener := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer()
	service := &exportCredentialsService{
		requests: make(chan *api.GetExportCredentialsRequest, 1),
	}
	api.RegisterDiskServiceServer(server, service)
	go server.Serve(listener)
	t.Cleanup(server.Stop)
	t.Cleanup(func() { _ = listener.Close() })
	connection, err := grpc.DialContext(
		context.Background(), "passthrough:///compute-test",
		grpc.WithContextDialer(func(
			ctx context.Context,
			address string,
		) (net.Conn, error) {

			return listener.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = connection.Close() })
	client := &client{
		service: api.NewDiskServiceClient(connection), timeout: time.Second,
	}
	for _, image := range []bool{false, true} {
		snapshotID, imageID := "same-id", ""
		if image {
			snapshotID, imageID = "", "same-id"
		}

		_, err := client.GetExportCredentials(
			context.Background(), "compute-task", snapshotID, imageID,
		)
		require.NoError(t, err)
		request := <-service.requests
		require.Equal(t, "compute-task", request.TaskId)
		require.Equal(t, snapshotID, request.SnapshotId)
		require.Equal(t, imageID, request.ImageId)
	}
}

func TestExportCredentialsRejectAmbiguousResource(t *testing.T) {
	client := &client{}
	for _, id := range []string{"", "both"} {
		_, err := client.GetExportCredentials(
			context.Background(), "compute-task", id, id,
		)
		require.ErrorIs(t, err, errors.NewEmptyNonRetriableError())
	}
}

type exportCredentialsClient struct {
	api.DiskServiceClient
	response *api.GetExportCredentialsResponse
	err      error
}

func (c *exportCredentialsClient) GetExportCredentials(
	ctx context.Context,
	request *api.GetExportCredentialsRequest,
	options ...grpc.CallOption,
) (*api.GetExportCredentialsResponse, error) {

	return c.response, c.err
}

func TestExportCredentialsAllowAnonymousAccess(t *testing.T) {
	for _, test := range []struct {
		name        string
		credentials *compute.ExportS3Credentials
		valid       bool
	}{
		{name: "anonymous", valid: true},
		{name: "empty", credentials: &compute.ExportS3Credentials{}},
		{
			name:        "missing secret",
			credentials: &compute.ExportS3Credentials{AccessKeyId: "id"},
		},
		{
			name: "missing id",
			credentials: &compute.ExportS3Credentials{
				SecretAccessKey: "secret",
			},
		},
		{
			name: "signed",
			credentials: &compute.ExportS3Credentials{
				AccessKeyId: "id", SecretAccessKey: "secret",
			},
			valid: true,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := logging.SetLogger(
				context.Background(),
				logging.NewStderrLogger(logging.ErrorLevel),
			)
			service := &exportCredentialsClient{
				response: &api.GetExportCredentialsResponse{
					Credentials: test.credentials,
				},
			}
			client := &client{service: service, timeout: time.Second}
			response, err := client.GetExportCredentials(
				ctx, "compute-task", "snapshot", "",
			)
			if test.valid {
				require.NoError(t, err)
				require.Same(t, service.response, response)
				return
			}

			require.ErrorIs(t, err, errors.NewEmptyNonRetriableError())
		})
	}
}

func TestExportCredentialsRPCErrorClassification(t *testing.T) {
	for _, code := range []codes.Code{codes.Internal, codes.PermissionDenied} {
		t.Run(code.String(), func(t *testing.T) {
			ctx := logging.SetLogger(
				context.Background(),
				logging.NewStderrLogger(logging.ErrorLevel),
			)
			rpcError := status.Error(code, "service error")
			client := &client{
				service:  &exportCredentialsClient{err: rpcError},
				endpoint: "compute.example:9051", timeout: time.Second,
			}
			_, err := client.GetExportCredentials(
				ctx, "compute-task", "snapshot", "",
			)
			require.ErrorIs(t, err, rpcError)
			require.Equal(t, code == codes.Internal, errors.CanRetry(err))
		})
	}
}
