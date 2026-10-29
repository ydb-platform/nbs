package util_test

import (
	"encoding/json"
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/require"
	disk_protos "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/disks/protos"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/types"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/util"
	"github.com/ydb-platform/nbs/cloud/tasks/common"
	"github.com/ydb-platform/nbs/cloud/tasks/storage"
)

func TestTaskStateToJSONIncludesVolumeLinkTransfer(t *testing.T) {
	request := &disk_protos.TransferVolumeViaLinkRequest{LogicalDiskId: "leader", DstCellId: "target-cell", ZoneId: "zone-a"}
	state := &disk_protos.TransferVolumeViaLinkTaskState{
		SrcDisk:  &types.Disk{ZoneId: "source-cell", DiskId: "leader-copy"},
		DstDisk:  &types.Disk{ZoneId: "target-cell", DiskId: "leader"},
		LinkUUID: "uuid", ReservationAcquired: true,
		Status: "LINK_STATUS_PREPARING", ErrorMessage: "diagnostic",
	}
	requestBytes, err := proto.Marshal(request)
	require.NoError(t, err)
	stateBytes, err := proto.Marshal(state)
	require.NoError(t, err)
	converted := util.TaskStateToJSON(&storage.TaskState{
		TaskType: "disks.TransferVolumeViaLink", Request: requestBytes, State: stateBytes,
		Dependencies: common.NewStringSet(),
	})
	decodedRequest := converted.Request.(*disk_protos.TransferVolumeViaLinkRequest)
	require.Equal(t, "leader", decodedRequest.LogicalDiskId)
	require.Equal(t, "target-cell", decodedRequest.DstCellId)
	require.Equal(t, "zone-a", decodedRequest.ZoneId)
	decoded := converted.State.(*disk_protos.TransferVolumeViaLinkTaskState)
	require.Equal(t, "uuid", decoded.LinkUUID)
	require.Equal(t, "leader-copy", decoded.SrcDisk.DiskId)
	require.True(t, decoded.ReservationAcquired)
	encoded, err := converted.Marshal()
	require.NoError(t, err)
	var document map[string]interface{}
	require.NoError(t, json.Unmarshal(encoded, &document))
	require.NotEmpty(t, document["request"])
	require.NotEmpty(t, document["state"])
}
