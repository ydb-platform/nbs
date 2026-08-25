package nbs2

import (
	"testing"

	"github.com/stretchr/testify/require"
	nbs2_protos "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nbs2/protos"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
)

func TestCheckOperationNotReady(t *testing.T) {
	err := checkOperation(&nbs2_protos.Operation{Id: "op", Ready: false})
	require.Error(t, err)
}

func TestCheckOperationFailedStatus(t *testing.T) {
	err := checkOperation(&nbs2_protos.Operation{
		Id:     "op",
		Ready:  true,
		Status: ydbStatusInternalError,
	})
	require.Error(t, err)
	require.True(t, errors.CanRetry(err))
}

func TestCheckOperationCreateAlreadyExists(t *testing.T) {
	err := checkOperation(
		&nbs2_protos.Operation{
			Id:     "op",
			Ready:  true,
			Status: ydbStatusAlreadyExists,
		},
		ydbStatusSuccess,
		ydbStatusAlreadyExists,
	)
	require.NoError(t, err)
}

func TestCheckOperationDeleteNotFound(t *testing.T) {
	err := checkOperation(
		&nbs2_protos.Operation{
			Id:     "op",
			Ready:  true,
			Status: ydbStatusNotFound,
		},
		ydbStatusSuccess,
		ydbStatusNotFound,
	)
	require.NoError(t, err)
}

func TestCheckOperationPermanentStatus(t *testing.T) {
	err := checkOperation(&nbs2_protos.Operation{
		Id:     "op",
		Ready:  true,
		Status: ydbStatusGenericError,
	})
	require.Error(t, err)
	require.False(t, errors.CanRetry(err))
}
