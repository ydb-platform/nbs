package nbs2

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/golang/protobuf/proto"
	nbs2_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nbs2/config"
	nbs2_protos "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nbs2/protos"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	"golang.org/x/exp/maps"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

////////////////////////////////////////////////////////////////////////////////

// Ydb.StatusIds.StatusCode.SUCCESS
const ydbStatusSuccess int32 = 400000

type client struct {
	zoneID  string
	timeout time.Duration
	dial    func(ctx context.Context) (*grpc.ClientConn, error)
}

func (c *client) ZoneID() string {
	return c.zoneID
}

func (c *client) CreatePartition(
	ctx context.Context,
	params CreatePartitionParams,
) (string, error) {

	if len(params.DiskID) == 0 {
		return "", errors.NewNonRetriableErrorf("disk id is required")
	}
	if len(params.StoragePoolName) == 0 {
		return "", errors.NewNonRetriableErrorf(
			"storage pool name is required for ssd-nbs2 disk %v",
			params.DiskID,
		)
	}

	req := &nbs2_protos.CreatePartitionRequest{
		OperationParams: &nbs2_protos.OperationParams{
			OperationMode: nbs2_protos.OperationParams_SYNC,
		},
		DiskId:          params.DiskID,
		BlockSize:       params.BlockSize,
		BlocksCount:     params.BlocksCount,
		StoragePoolName: params.StoragePoolName,
		StorageMedia:    nbs2_protos.StorageMediaKind_STORAGE_MEDIA_SSD,
	}

	resp := &nbs2_protos.CreatePartitionResponse{}
	err := c.invoke(ctx, "CreatePartition", req, resp)
	if err != nil {
		return "", err
	}

	op, err := checkOperation(resp.GetOperation())
	if err != nil {
		return "", err
	}

	result := &nbs2_protos.CreatePartitionResult{}
	err = unpackOperationResult(op, result)
	if err != nil {
		return "", err
	}
	if len(result.GetTabletId()) == 0 {
		return "", errors.NewNonRetriableErrorf(
			"CreatePartition for disk %v returned empty tablet id",
			params.DiskID,
		)
	}

	return result.GetTabletId(), nil
}

func (c *client) DeletePartition(ctx context.Context, tabletID string) error {
	if len(tabletID) == 0 {
		return errors.NewNonRetriableErrorf("tablet id is required")
	}

	req := &nbs2_protos.DeletePartitionRequest{
		OperationParams: &nbs2_protos.OperationParams{
			OperationMode: nbs2_protos.OperationParams_SYNC,
		},
		TabletId: tabletID,
	}

	resp := &nbs2_protos.DeletePartitionResponse{}
	err := c.invoke(ctx, "DeletePartition", req, resp)
	if err != nil {
		return err
	}

	_, err = checkOperation(resp.GetOperation())
	return err
}

func (c *client) invoke(
	ctx context.Context,
	method string,
	req proto.Message,
	resp proto.Message,
) error {

	timeout := c.timeout
	if timeout <= 0 {
		timeout = 20 * time.Second
	}

	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	conn, err := c.dial(ctx)
	if err != nil {
		return errors.NewRetriableError(err)
	}
	defer conn.Close()

	stub := nbs2_protos.NewNbsServiceClient(conn)
	switch method {
	case "CreatePartition":
		out, err := stub.CreatePartition(ctx, req.(*nbs2_protos.CreatePartitionRequest))
		if err != nil {
			return errors.NewRetriableError(err)
		}
		proto.Merge(resp, out)
	case "DeletePartition":
		out, err := stub.DeletePartition(ctx, req.(*nbs2_protos.DeletePartitionRequest))
		if err != nil {
			return errors.NewRetriableError(err)
		}
		proto.Merge(resp, out)
	default:
		return errors.NewNonRetriableErrorf("unknown nbs method %v", method)
	}

	return nil
}

func checkOperation(op *nbs2_protos.Operation) (*nbs2_protos.Operation, error) {
	if op == nil {
		return nil, errors.NewRetriableErrorf("empty operation in nbs response")
	}
	if !op.GetReady() {
		return nil, errors.NewRetriableErrorf(
			"nbs operation %v is not ready",
			op.GetId(),
		)
	}
	if op.GetStatus() != ydbStatusSuccess {
		return nil, errors.NewRetriableErrorf(
			"nbs operation %v failed with status %v",
			op.GetId(),
			op.GetStatus(),
		)
	}
	return op, nil
}

func unpackOperationResult(op *nbs2_protos.Operation, msg proto.Message) error {
	if op.GetResult() == nil {
		return errors.NewNonRetriableErrorf("nbs operation %v has empty result", op.GetId())
	}

	err := proto.Unmarshal(op.GetResult().GetValue(), msg)
	if err != nil {
		return errors.NewNonRetriableErrorf(
			"failed to unpack nbs operation %v result: %w",
			op.GetId(),
			err,
		)
	}
	return nil
}

////////////////////////////////////////////////////////////////////////////////

type factory struct {
	config  *nbs2_config.ClientConfig
	timeout time.Duration
}

func (f *factory) GetClient(ctx context.Context, zoneID string) (Client, error) {
	if f.config == nil {
		return nil, errors.NewNonRetriableErrorf(
			"nbs2 client is not configured, available zones: []",
		)
	}

	zone, ok := f.config.GetZones()[zoneID]
	if !ok {
		return nil, errors.NewNonRetriableErrorf(
			"unknown nbs2 zone %q, available zones: %q",
			zoneID,
			maps.Keys(f.config.GetZones()),
		)
	}
	if len(zone.GetEndpoints()) == 0 {
		return nil, errors.NewNonRetriableErrorf(
			"no nbs2 endpoints for zone %q",
			zoneID,
		)
	}

	endpoint := normalizeEndpoint(zone.GetEndpoints()[0])
	creds, err := f.transportCredentials()
	if err != nil {
		return nil, err
	}

	return &client{
		zoneID:  zoneID,
		timeout: f.timeout,
		dial: func(ctx context.Context) (*grpc.ClientConn, error) {
			return grpc.DialContext(ctx, endpoint, grpc.WithTransportCredentials(creds))
		},
	}, nil
}

func (f *factory) transportCredentials() (credentials.TransportCredentials, error) {
	if f.config.GetInsecure() || len(f.config.GetRootCertsFile()) == 0 {
		return insecure.NewCredentials(), nil
	}

	creds, err := credentials.NewClientTLSFromFile(f.config.GetRootCertsFile(), "")
	if err != nil {
		return nil, errors.NewNonRetriableErrorf(
			"failed to load nbs2 root certs from %v: %w",
			f.config.GetRootCertsFile(),
			err,
		)
	}
	return creds, nil
}

func normalizeEndpoint(endpoint string) string {
	endpoint = strings.TrimPrefix(endpoint, "grpc://")
	endpoint = strings.TrimPrefix(endpoint, "grpcs://")
	return endpoint
}

func NewFactory(config *nbs2_config.ClientConfig) (Factory, error) {
	timeout := 20 * time.Second
	if config != nil && len(config.GetRequestTimeout()) > 0 {
		parsed, err := time.ParseDuration(config.GetRequestTimeout())
		if err != nil {
			return nil, fmt.Errorf("invalid nbs2 request timeout: %w", err)
		}
		timeout = parsed
	}

	return &factory{
		config:  config,
		timeout: timeout,
	}, nil
}
