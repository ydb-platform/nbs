package nbs2

import (
	"context"
	"fmt"
	"time"

	nbs2_protos "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nbs2/protos"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

////////////////////////////////////////////////////////////////////////////////

// Ydb.StatusIds.StatusCode values used by Ydb.Nbs.V1.NbsService.
const (
	ydbStatusSuccess        int32 = 400000
	ydbStatusInternalError  int32 = 400030
	ydbStatusAborted        int32 = 400040
	ydbStatusUnavailable    int32 = 400050
	ydbStatusOverloaded     int32 = 400060
	ydbStatusGenericError   int32 = 400080
	ydbStatusTimeout        int32 = 400090
	ydbStatusBadSession     int32 = 400100
	ydbStatusAlreadyExists  int32 = 400130
	ydbStatusNotFound       int32 = 400140
	ydbStatusSessionExpired int32 = 400150
	ydbStatusCancelled      int32 = 400160
	ydbStatusUndetermined   int32 = 400170
	ydbStatusSessionBusy    int32 = 400190
)

type client struct {
	endpoint string
	timeout  time.Duration
}

func (c *client) Create(ctx context.Context, params CreateDiskParams) error {
	if len(params.ID) == 0 {
		return errors.NewNonRetriableErrorf("disk id is required")
	}
	if len(params.StoragePoolName) == 0 {
		return errors.NewNonRetriableErrorf(
			"storage pool name is required for ssd-nbs2 disk %v",
			params.ID,
		)
	}

	req := &nbs2_protos.CreatePartitionRequest{
		OperationParams: &nbs2_protos.OperationParams{
			OperationMode: nbs2_protos.OperationParams_SYNC,
		},
		DiskId:          params.ID,
		BlockSize:       params.BlockSize,
		BlocksCount:     params.BlocksCount,
		StoragePoolName: params.StoragePoolName,
		StorageMedia:    nbs2_protos.StorageMediaKind_STORAGE_MEDIA_SSD,
	}

	var resp *nbs2_protos.CreatePartitionResponse
	err := c.withClient(ctx, func(ctx context.Context, client nbs2_protos.NbsServiceClient) error {
		var err error
		resp, err = client.CreatePartition(ctx, req)
		return err
	})
	if err != nil {
		return err
	}

	return checkOperation(
		resp.GetOperation(),
		ydbStatusSuccess,
		ydbStatusAlreadyExists,
	)
}

func (c *client) Delete(ctx context.Context, diskID string) error {
	if len(diskID) == 0 {
		return errors.NewNonRetriableErrorf("disk id is required")
	}

	req := &nbs2_protos.DeletePartitionRequest{
		OperationParams: &nbs2_protos.OperationParams{
			OperationMode: nbs2_protos.OperationParams_SYNC,
		},
		DiskId: diskID,
	}

	var resp *nbs2_protos.DeletePartitionResponse
	err := c.withClient(ctx, func(ctx context.Context, client nbs2_protos.NbsServiceClient) error {
		var err error
		resp, err = client.DeletePartition(ctx, req)
		return err
	})
	if err != nil {
		return err
	}

	return checkOperation(
		resp.GetOperation(),
		ydbStatusSuccess,
		ydbStatusNotFound,
	)
}

func (c *client) withClient(
	ctx context.Context,
	call func(context.Context, nbs2_protos.NbsServiceClient) error,
) error {

	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()

	conn, err := grpc.DialContext(
		ctx,
		c.endpoint,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return errors.NewRetriableError(err)
	}
	defer conn.Close()

	err = call(ctx, nbs2_protos.NewNbsServiceClient(conn))
	if err != nil {
		return errors.NewRetriableError(err)
	}

	return nil
}

func checkOperation(op *nbs2_protos.Operation, okStatuses ...int32) error {
	if op == nil {
		return errors.NewRetriableErrorf("empty operation in nbs response")
	}
	if !op.GetReady() {
		return errors.NewRetriableErrorf(
			"nbs operation %v is not ready",
			op.GetId(),
		)
	}
	if len(okStatuses) == 0 {
		okStatuses = []int32{ydbStatusSuccess}
	}
	for _, status := range okStatuses {
		if op.GetStatus() == status {
			return nil
		}
	}
	return operationStatusError(op)
}

func operationStatusError(op *nbs2_protos.Operation) error {
	msg := fmt.Sprintf(
		"nbs operation %v failed with status %v",
		op.GetId(),
		op.GetStatus(),
	)
	if isRetriableYdbStatus(op.GetStatus()) {
		return errors.NewRetriableErrorf("%s", msg)
	}
	return errors.NewNonRetriableErrorf("%s", msg)
}

func isRetriableYdbStatus(status int32) bool {
	switch status {
	case ydbStatusInternalError,
		ydbStatusAborted,
		ydbStatusUnavailable,
		ydbStatusOverloaded,
		ydbStatusTimeout,
		ydbStatusBadSession,
		ydbStatusSessionExpired,
		ydbStatusCancelled,
		ydbStatusUndetermined,
		ydbStatusSessionBusy:
		return true
	default:
		return false
	}
}
