package disk_service

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"os"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/disk_service/config"
	"github.com/ydb-platform/nbs/cloud/disk_manager/pkg/auth"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/logging"
	api "github.com/ydb-platform/nbs/contrib/ydb/public/api/client/yc_private/compute/inner"
)

// Client obtains destination credentials separately from the Export request.
// A missing credentials message requests anonymous destination access.
type Client interface {
	GetExportCredentials(
		ctx context.Context,
		computeTaskID string,
		snapshotID string,
		imageID string,
	) (*api.GetExportCredentialsResponse, error)
}

type client struct {
	service     api.DiskServiceClient
	credentials auth.Credentials
	endpoint    string
	timeout     time.Duration
}

func NewClient(
	config *config.ClientConfig,
	tokenProvider auth.Credentials,
) (Client, func() error, error) {

	timeout, err := time.ParseDuration(config.GetCallTimeout())
	if err != nil || timeout <= 0 || config.GetEndpoint() == "" {
		return nil, nil, errors.NewNonRetriableErrorf(
			"invalid DiskService endpoint or call timeout",
		)
	}

	var transport credentials.TransportCredentials
	if config.GetInsecure() {
		transport = insecure.NewCredentials()
	} else {
		tlsConfig := &tls.Config{MinVersion: tls.VersionTLS12}
		if config.GetRootCertsFile() != "" {
			certs, err := os.ReadFile(config.GetRootCertsFile())
			if err != nil {
				return nil, nil, err
			}

			tlsConfig.RootCAs = x509.NewCertPool()
			if !tlsConfig.RootCAs.AppendCertsFromPEM(certs) {
				return nil, nil, errors.NewNonRetriableErrorf(
					"invalid DiskService root certificates",
				)
			}
		}

		transport = credentials.NewTLS(tlsConfig)
	}

	connection, err := grpc.Dial(
		config.GetEndpoint(),
		grpc.WithTransportCredentials(transport),
	)
	if err != nil {
		return nil, nil, err
	}

	return &client{
		service:     api.NewDiskServiceClient(connection),
		credentials: tokenProvider,
		endpoint:    config.GetEndpoint(),
		timeout:     timeout,
	}, connection.Close, nil
}

func (c *client) GetExportCredentials(
	ctx context.Context,
	computeTaskID string,
	snapshotID string,
	imageID string,
) (*api.GetExportCredentialsResponse, error) {

	if computeTaskID == "" {
		return nil, errors.NewNonRetriableErrorf(
			"Compute export task ID is required",
		)
	}

	if (snapshotID == "") == (imageID == "") {
		return nil, errors.NewNonRetriableErrorf(
			"exactly one snapshot ID or image ID is required",
		)
	}

	ctx = logging.WithFields(
		ctx,
		logging.String("DISK_SERVICE_ENDPOINT", c.endpoint),
		logging.String("COMPUTE_TASK_ID", computeTaskID),
		logging.String("SNAPSHOT_ID", snapshotID),
		logging.String("IMAGE_ID", imageID),
	)
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	if c.credentials != nil {
		token, err := c.credentials.Token(ctx)
		if err != nil {
			logging.Error(
				ctx,
				"getting IAM token for DiskService.GetExportCredentials "+
					"failed: %v",
				err,
			)
			return nil, err
		}

		headers, _ := metadata.FromOutgoingContext(ctx)
		headers = headers.Copy()
		headers.Set("authorization", "Bearer "+token)
		ctx = metadata.NewOutgoingContext(ctx, headers)
	}

	started := time.Now()
	response, err := c.service.GetExportCredentials(
		ctx,
		&api.GetExportCredentialsRequest{
			TaskId:     computeTaskID,
			SnapshotId: snapshotID,
			ImageId:    imageID,
		},
	)
	if err != nil {
		logging.Error(
			ctx,
			"DiskService.GetExportCredentials failed, "+
				"code %v, duration %v, timeout %v: %v",
			status.Code(err), time.Since(started), c.timeout, err,
		)
		switch status.Code(err) {
		case codes.InvalidArgument, codes.NotFound,
			codes.PermissionDenied, codes.Unimplemented:
			return nil, errors.NewNonRetriableError(err)
		default:
			return nil, errors.NewRetriableError(err)
		}
	}

	creds := response.GetCredentials()
	if creds != nil &&
		(creds.GetAccessKeyId() == "" || creds.GetSecretAccessKey() == "") {

		logging.Error(
			ctx,
			"DiskService.GetExportCredentials returned "+
				"incomplete S3 credentials",
		)
		return nil, errors.NewNonRetriableErrorf(
			"DiskService.GetExportCredentials returned " +
				"incomplete S3 credentials",
		)
	}

	return response, nil
}
