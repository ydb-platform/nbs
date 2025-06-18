package client

import (
	"context"
	"crypto/x509"
	"fmt"
	"os"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes"
	"github.com/golang/protobuf/ptypes/empty"
	grpc_retry "github.com/grpc-ecosystem/go-grpc-middleware/retry"
	disk_manager "github.com/ydb-platform/nbs/cloud/disk_manager/api"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/api"
	client_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/configs/client/config"
	sdk_client "github.com/ydb-platform/nbs/cloud/disk_manager/pkg/client"
	sdk_client_config "github.com/ydb-platform/nbs/cloud/disk_manager/pkg/client/config"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/logging"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	grpc_status "google.golang.org/grpc/status"
)

////////////////////////////////////////////////////////////////////////////////

type WaitableClient interface {
	WaitOperation(
		ctx context.Context,
		operationID string,
		callback func(context.Context, *disk_manager.Operation) error,
	) (*disk_manager.Operation, error)
}

func WaitOperation(
	ctx context.Context,
	client WaitableClient,
	operationID string,
) error {

	return WaitResponse(ctx, client, operationID, nil)
}

func WaitResponse(
	ctx context.Context,
	client WaitableClient,
	operationID string,
	response proto.Message,
) error {

	logging.Info(ctx, "Waiting for operation %v", operationID)

	o, err := client.WaitOperation(
		ctx,
		operationID,
		func(context.Context, *disk_manager.Operation) error { return nil },
	)
	if err != nil {
		return err
	}

	if !o.Done {
		return errors.NewNonRetriableErrorf(
			"operation %v should be finished",
			o,
		)
	}

	err = parseOperationResponse(ctx, o, response)
	if err != nil {
		return err
	}

	logging.Info(ctx, "Operation %v finished", operationID)
	return nil
}

////////////////////////////////////////////////////////////////////////////////

func parseOperationResponse(
	ctx context.Context,
	o *disk_manager.Operation,
	response proto.Message,
) error {

	switch result := o.Result.(type) {
	case *disk_manager.Operation_Error:
		return grpc_status.ErrorProto(result.Error)
	case *disk_manager.Operation_Response:
		if response != nil {
			return ptypes.UnmarshalAny(result.Response, response)
		}

		return nil
	default:
		return errors.NewNonRetriableErrorf(
			"unknown operation result type %v",
			result,
		)
	}
}

// If |done && err != nil| then operation is ended with error which is
// represented by |err|.
func GetOperationResponse(
	ctx context.Context,
	client sdk_client.Client,
	operationID string,
	response proto.Message,
) (done bool, err error) {

	o, err := client.GetOperation(
		ctx,
		&disk_manager.GetOperationRequest{
			OperationId: operationID,
		},
	)
	if err != nil {
		return false, err
	}

	if !o.Done {
		return false, nil
	}

	return true, parseOperationResponse(ctx, o, response)
}

func GetOperationMetadata(
	ctx context.Context,
	client sdk_client.Client,
	operationID string,
	metadata proto.Message,
) error {

	o, err := client.GetOperation(
		ctx,
		&disk_manager.GetOperationRequest{
			OperationId: operationID,
		},
	)
	if err != nil {
		return err
	}

	if o.Metadata != nil {
		return ptypes.UnmarshalAny(o.Metadata, metadata)
	}

	return nil
}

////////////////////////////////////////////////////////////////////////////////

type PrivateClient interface {
	WaitOperation(
		ctx context.Context,
		operationID string,
		callback func(context.Context, *disk_manager.Operation) error,
	) (*disk_manager.Operation, error)

	// Used for testing.
	ScheduleBlankOperation(ctx context.Context) (*disk_manager.Operation, error)

	ReleaseBaseDisk(
		ctx context.Context,
		req *api.ReleaseBaseDiskRequest,
	) (*disk_manager.Operation, error)

	RebaseOverlayDisk(
		ctx context.Context,
		req *api.RebaseOverlayDiskRequest,
	) (*disk_manager.Operation, error)

	RetireBaseDisk(
		ctx context.Context,
		req *api.RetireBaseDiskRequest,
	) (*disk_manager.Operation, error)

	RetireBaseDisks(
		ctx context.Context,
		req *api.RetireBaseDisksRequest,
	) (*disk_manager.Operation, error)

	OptimizeBaseDisks(ctx context.Context) (*disk_manager.Operation, error)

	ConfigurePool(
		ctx context.Context,
		req *api.ConfigurePoolRequest,
	) (*disk_manager.Operation, error)

	DeletePool(
		ctx context.Context,
		req *api.DeletePoolRequest,
	) (*disk_manager.Operation, error)

	ListDisks(
		ctx context.Context,
		req *api.ListDisksRequest,
	) (*api.ListDisksResponse, error)

	ListImages(
		ctx context.Context,
		req *api.ListImagesRequest,
	) (*api.ListImagesResponse, error)

	ListSnapshots(
		ctx context.Context,
		req *api.ListSnapshotsRequest,
	) (*api.ListSnapshotsResponse, error)

	ListFilesystems(
		ctx context.Context,
		req *api.ListFilesystemsRequest,
	) (*api.ListFilesystemsResponse, error)

	ListPlacementGroups(
		ctx context.Context,
		req *api.ListPlacementGroupsRequest,
	) (*api.ListPlacementGroupsResponse, error)

	GetAliveNodes(
		ctx context.Context,
	) (*api.GetAliveNodesResponse, error)

	FinishExternalFilesystemCreation(
		ctx context.Context,
		req *api.FinishExternalFilesystemCreationRequest,
	) error

	FinishExternalFilesystemDeletion(
		ctx context.Context,
		req *api.FinishExternalFilesystemDeletionRequest,
	) error

	Close() error
}

type privateClient struct {
	operationPollPeriod    time.Duration
	operationServiceClient disk_manager.OperationServiceClient
	privateServiceClient   api.PrivateServiceClient
	conn                   *grpc.ClientConn
}

func (c *privateClient) WaitOperation(
	ctx context.Context,
	operationID string,
	callback func(context.Context, *disk_manager.Operation) error,
) (*disk_manager.Operation, error) {

	for {
		o, err := c.operationServiceClient.Get(ctx, &disk_manager.GetOperationRequest{
			OperationId: operationID,
		})
		if err != nil {
			return nil, err
		}

		if callback != nil {
			err = callback(ctx, o)
			if err != nil {
				return nil, err
			}
		}

		if o.Done {
			return o, nil
		}

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(c.operationPollPeriod):
		}
	}
}

func (c *privateClient) ScheduleBlankOperation(
	ctx context.Context,
) (*disk_manager.Operation, error) {

	return c.privateServiceClient.ScheduleBlankOperation(ctx, &empty.Empty{})
}

func (c *privateClient) ReleaseBaseDisk(
	ctx context.Context,
	req *api.ReleaseBaseDiskRequest,
) (*disk_manager.Operation, error) {

	return c.privateServiceClient.ReleaseBaseDisk(ctx, req)
}

func (c *privateClient) RebaseOverlayDisk(
	ctx context.Context,
	req *api.RebaseOverlayDiskRequest,
) (*disk_manager.Operation, error) {

	return c.privateServiceClient.RebaseOverlayDisk(ctx, req)
}

func (c *privateClient) RetireBaseDisk(
	ctx context.Context,
	req *api.RetireBaseDiskRequest,
) (*disk_manager.Operation, error) {

	return c.privateServiceClient.RetireBaseDisk(ctx, req)
}

func (c *privateClient) RetireBaseDisks(
	ctx context.Context,
	req *api.RetireBaseDisksRequest,
) (*disk_manager.Operation, error) {

	return c.privateServiceClient.RetireBaseDisks(ctx, req)
}

func (c *privateClient) OptimizeBaseDisks(
	ctx context.Context,
) (*disk_manager.Operation, error) {

	return c.privateServiceClient.OptimizeBaseDisks(ctx, &empty.Empty{})
}

func (c *privateClient) ConfigurePool(
	ctx context.Context,
	req *api.ConfigurePoolRequest,
) (*disk_manager.Operation, error) {

	return c.privateServiceClient.ConfigurePool(ctx, req)
}

func (c *privateClient) DeletePool(
	ctx context.Context,
	req *api.DeletePoolRequest,
) (*disk_manager.Operation, error) {

	return c.privateServiceClient.DeletePool(ctx, req)
}

func (c *privateClient) ListDisks(
	ctx context.Context,
	req *api.ListDisksRequest,
) (*api.ListDisksResponse, error) {

	return c.privateServiceClient.ListDisks(ctx, req)
}

func (c *privateClient) ListImages(
	ctx context.Context,
	req *api.ListImagesRequest,
) (*api.ListImagesResponse, error) {

	return c.privateServiceClient.ListImages(ctx, req)
}

func (c *privateClient) ListSnapshots(
	ctx context.Context,
	req *api.ListSnapshotsRequest,
) (*api.ListSnapshotsResponse, error) {

	return c.privateServiceClient.ListSnapshots(ctx, req)
}

func (c *privateClient) ListFilesystems(
	ctx context.Context,
	req *api.ListFilesystemsRequest,
) (*api.ListFilesystemsResponse, error) {

	return c.privateServiceClient.ListFilesystems(ctx, req)
}

func (c *privateClient) ListPlacementGroups(
	ctx context.Context,
	req *api.ListPlacementGroupsRequest,
) (*api.ListPlacementGroupsResponse, error) {

	return c.privateServiceClient.ListPlacementGroups(ctx, req)
}

func (c *privateClient) GetAliveNodes(
	ctx context.Context,
) (*api.GetAliveNodesResponse, error) {

	return c.privateServiceClient.GetAliveNodes(ctx, new(empty.Empty))
}

func (c *privateClient) FinishExternalFilesystemCreation(
	ctx context.Context,
	req *api.FinishExternalFilesystemCreationRequest,
) error {

	_, err := c.privateServiceClient.FinishExternalFilesystemCreation(ctx, req)
	return err
}

func (c *privateClient) FinishExternalFilesystemDeletion(
	ctx context.Context,
	req *api.FinishExternalFilesystemDeletionRequest,
) error {

	_, err := c.privateServiceClient.FinishExternalFilesystemDeletion(ctx, req)
	return err
}

////////////////////////////////////////////////////////////////////////////////

func (c *privateClient) Close() error {
	return c.conn.Close()
}

////////////////////////////////////////////////////////////////////////////////

func getDialOptions(config *client_config.ClientConfig) ([]grpc.DialOption, error) {
	options := make([]grpc.DialOption, 0)

	if config.GetInsecure() {
		return append(options, grpc.WithInsecure()), nil
	}

	cp, err := x509.SystemCertPool()
	if err != nil {
		return nil, errors.NewNonRetriableError(err)
	}

	if len(config.GetServerCertFile()) != 0 {
		bytes, err := os.ReadFile(config.GetServerCertFile())
		if err != nil {
			return nil, errors.NewNonRetriableError(err)
		}

		if !cp.AppendCertsFromPEM(bytes) {
			return nil, errors.NewNonRetriableErrorf(
				"failed to append certificate %v",
				config.GetServerCertFile(),
			)
		}
	}

	return append(
		options,
		grpc.WithTransportCredentials(
			credentials.NewClientTLSFromCert(cp, ""),
		),
	), nil
}

func getEndpoint(config *client_config.ClientConfig) (string, error) {
	host := config.GetHost()
	if len(host) == 0 {
		var err error
		host, err = os.Hostname()
		if err != nil {
			return "", errors.NewNonRetriableError(err)
		}
	}

	return fmt.Sprintf("%v:%v", host, config.GetPort()), nil
}

////////////////////////////////////////////////////////////////////////////////

func NewClient(
	ctx context.Context,
	config *client_config.ClientConfig,
) (sdk_client.Client, error) {

	options, err := getDialOptions(config)
	if err != nil {
		return nil, err
	}

	endpoint, err := getEndpoint(config)
	if err != nil {
		return nil, err
	}

	interceptor := func(
		ctx context.Context,
		method string,
		req interface{},
		reply interface{},
		cc *grpc.ClientConn,
		invoker grpc.UnaryInvoker,
		opts ...grpc.CallOption,
	) error {

		err := invoker(ctx, method, req, reply, cc, opts...)
		if err != nil {
			logging.Warn(ctx, "%v", err)
		}

		return err
	}

	options = append(options, grpc.WithChainUnaryInterceptor(interceptor))

	maxRetryAttempts := config.GetMaxRetryAttempts()
	perRetryTimeout := config.GetPerRetryTimeout()
	backoffTimeout := config.GetBackoffTimeout()
	operationPollPeriod := config.GetOperationPollPeriod()

	client, err := sdk_client.NewClient(
		ctx,
		&sdk_client_config.Config{
			Endpoint:            &endpoint,
			MaxRetryAttempts:    &maxRetryAttempts,
			PerRetryTimeout:     &perRetryTimeout,
			BackoffTimeout:      &backoffTimeout,
			OperationPollPeriod: &operationPollPeriod,
		},
		options...,
	)
	if err != nil {
		return nil, errors.NewNonRetriableError(err)
	}

	return client, nil
}

func NewPrivateClient(
	ctx context.Context,
	config *sdk_client_config.Config,
	options ...grpc.DialOption,
) (PrivateClient, error) {

	perRetryTimeout, err := time.ParseDuration(config.GetPerRetryTimeout())
	if err != nil {
		return nil, errors.NewNonRetriableError(err)
	}

	backoffTimeout, err := time.ParseDuration(config.GetBackoffTimeout())
	if err != nil {
		return nil, errors.NewNonRetriableError(err)
	}

	operationPollPeriod, err := time.ParseDuration(config.GetOperationPollPeriod())
	if err != nil {
		return nil, errors.NewNonRetriableError(err)
	}

	interceptor := grpc_retry.UnaryClientInterceptor(
		grpc_retry.WithMax(uint(config.GetMaxRetryAttempts())),
		grpc_retry.WithPerRetryTimeout(perRetryTimeout),
		grpc_retry.WithBackoff(func(attempt uint) time.Duration { return backoffTimeout }),
	)
	options = append(options, grpc.WithChainUnaryInterceptor(interceptor))

	conn, err := grpc.DialContext(ctx, config.GetEndpoint(), options...)
	if err != nil {
		return nil, err
	}

	return &privateClient{
		operationPollPeriod:    operationPollPeriod,
		operationServiceClient: disk_manager.NewOperationServiceClient(conn),
		privateServiceClient:   api.NewPrivateServiceClient(conn),
		conn:                   conn,
	}, nil
}

// TODO: naming should be better, because this function is used not only in CLI.
func NewPrivateClientForCLI(
	ctx context.Context,
	config *client_config.ClientConfig,
) (PrivateClient, error) {

	options, err := getDialOptions(config)
	if err != nil {
		return nil, err
	}

	endpoint, err := getEndpoint(config)
	if err != nil {
		return nil, err
	}

	maxRetryAttempts := config.GetMaxRetryAttempts()
	perRetryTimeout := config.GetPerRetryTimeout()
	backoffTimeout := config.GetBackoffTimeout()
	operationPollPeriod := config.GetOperationPollPeriod()

	return NewPrivateClient(
		ctx,
		&sdk_client_config.Config{
			Endpoint:            &endpoint,
			MaxRetryAttempts:    &maxRetryAttempts,
			PerRetryTimeout:     &perRetryTimeout,
			BackoffTimeout:      &backoffTimeout,
			OperationPollPeriod: &operationPollPeriod,
		},
		options...,
	)
}
