package nbs2

import (
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/require"
	nbs2_protos "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nbs2/protos"
	"google.golang.org/protobuf/types/known/anypb"
)

func TestUnpackCreatePartitionResult(t *testing.T) {
	result := &nbs2_protos.CreatePartitionResult{TabletId: "tablet-1"}
	raw, err := proto.Marshal(result)
	require.NoError(t, err)

	op := &nbs2_protos.Operation{
		Id:     "op",
		Ready:  true,
		Status: ydbStatusSuccess,
		Result: &anypb.Any{Value: raw},
	}

	unpacked := &nbs2_protos.CreatePartitionResult{}
	err = unpackOperationResult(op, unpacked)
	require.NoError(t, err)
	require.Equal(t, "tablet-1", unpacked.GetTabletId())
}

func TestCheckOperationNotReady(t *testing.T) {
	_, err := checkOperation(&nbs2_protos.Operation{Id: "op", Ready: false})
	require.Error(t, err)
}

func TestCheckOperationFailedStatus(t *testing.T) {
	_, err := checkOperation(&nbs2_protos.Operation{
		Id:     "op",
		Ready:  true,
		Status: 400030, // INTERNAL_ERROR
	})
	require.Error(t, err)
}

func TestNormalizeEndpoint(t *testing.T) {
	require.Equal(t, "localhost:2135", normalizeEndpoint("grpc://localhost:2135"))
	require.Equal(t, "localhost:2135", normalizeEndpoint("grpcs://localhost:2135"))
	require.Equal(t, "localhost:2135", normalizeEndpoint("localhost:2135"))
}
