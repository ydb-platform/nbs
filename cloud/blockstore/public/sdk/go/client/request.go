package client

import (
	"fmt"
	"math/rand"
	"strings"

	"github.com/ydb-platform/nbs/cloud/blockstore/public/api/protos"
	storage_protos "github.com/ydb-platform/nbs/cloud/storage/core/protos"
)

////////////////////////////////////////////////////////////////////////////////

type request interface {
	GetHeaders() *protos.THeaders
	String() string
}

////////////////////////////////////////////////////////////////////////////////

func nextRequestId() uint64 {
	var requestId uint64 = 0
	for requestId == 0 {
		requestId = rand.Uint64()
	}
	return requestId
}

func requestName(req request) string {
	name := underlyingTypeName(req)
	if strings.HasPrefix(name, "T") && strings.HasSuffix(name, "Request") {
		// strip "T" prefix and "Request" suffix from type name
		name = name[1 : len(name)-7]
	}
	return name
}

func requestSize(req request) uint32 {
	if readReq, ok := req.(*protos.TReadBlocksRequest); ok {
		return readReq.GetBlocksCount()
	}

	if writeReq, ok := req.(*protos.TWriteBlocksRequest); ok {
		return uint32(len(writeReq.Blocks.Buffers))
	}

	if zeroReq, ok := req.(*protos.TZeroBlocksRequest); ok {
		return zeroReq.GetBlocksCount()
	}

	return 0
}

func requestLogLevel(req request) LogLevel {
	switch req.(type) {
	case *protos.TReadBlocksRequest:
		return LOG_DEBUG
	case *protos.TWriteBlocksRequest:
		return LOG_DEBUG
	case *protos.TZeroBlocksRequest:
		return LOG_DEBUG
	default:
		return LOG_INFO
	}
}

func requestDetails(req request) string {
	if readReq, ok := req.(*protos.TReadBlocksRequest); ok {
		return fmt.Sprintf(
			" (offset: %d, blocks count: %d)",
			readReq.StartIndex,
			readReq.BlocksCount)
	}

	if writeReq, ok := req.(*protos.TWriteBlocksRequest); ok {
		return fmt.Sprintf(
			" (offset: %d, buffers count: %d)",
			writeReq.StartIndex,
			len(writeReq.Blocks.Buffers))
	}

	if zeroReq, ok := req.(*protos.TZeroBlocksRequest); ok {
		return fmt.Sprintf(
			" (offset: %d, blocks count: %d)",
			zeroReq.StartIndex,
			zeroReq.BlocksCount)
	}

	if createVolumeReq, ok := req.(*protos.TCreateVolumeRequest); ok {
		return fmt.Sprintf(
			" (disk id: %s, project id: %s, block size: %d, blocks count: %d, channels count: %d, storage media kind %s, folder id: %s, cloud id: %s)",
			createVolumeReq.DiskId,
			createVolumeReq.ProjectId,
			createVolumeReq.BlockSize,
			createVolumeReq.BlocksCount,
			createVolumeReq.ChannelsCount,
			storage_protos.EStorageMediaKind_name[int32(createVolumeReq.StorageMediaKind)],
			createVolumeReq.FolderId,
			createVolumeReq.CloudId)
	}

	if destroyVolumeReq, ok := req.(*protos.TDestroyVolumeRequest); ok {
		return fmt.Sprintf(
			" (disk id: %s)",
			destroyVolumeReq.DiskId)
	}

	if resizeVolumeReq, ok := req.(*protos.TResizeVolumeRequest); ok {
		return fmt.Sprintf(
			" (disk id: %s, blocks count: %d, channels count: %d)",
			resizeVolumeReq.DiskId,
			resizeVolumeReq.BlocksCount,
			resizeVolumeReq.ChannelsCount)
	}

	if alterVolumeReq, ok := req.(*protos.TAlterVolumeRequest); ok {
		return fmt.Sprintf(
			" (disk id: %s, project id: %s, folder id: %s, cloud id: %s)",
			alterVolumeReq.DiskId,
			alterVolumeReq.ProjectId,
			alterVolumeReq.FolderId,
			alterVolumeReq.CloudId)
	}

	if assignVolumeReq, ok := req.(*protos.TAssignVolumeRequest); ok {
		return fmt.Sprintf(
			" (disk id: %s, instance id: %s, host: %s)",
			assignVolumeReq.DiskId,
			assignVolumeReq.InstanceId,
			assignVolumeReq.Host)
	}

	if mountVolumeReq, ok := req.(*protos.TMountVolumeRequest); ok {
		return fmt.Sprintf(
			" (disk id: %s, instance id: %s, volume access mode: %s, volume mount mode: %s, ipc type: %s, mount seq number: %d)",
			mountVolumeReq.DiskId,
			mountVolumeReq.InstanceId,
			protos.EVolumeAccessMode_name[int32(mountVolumeReq.VolumeAccessMode)],
			protos.EVolumeMountMode_name[int32(mountVolumeReq.VolumeMountMode)],
			protos.EClientIpcType_name[int32(mountVolumeReq.IpcType)],
			mountVolumeReq.MountSeqNumber)
	}

	if unmountVolumeReq, ok := req.(*protos.TUnmountVolumeRequest); ok {
		return fmt.Sprintf(
			" (disk id: %s, instance id: %s, session id: %s)",
			unmountVolumeReq.DiskId,
			unmountVolumeReq.InstanceId,
			unmountVolumeReq.SessionId)
	}

	if createCheckpointReq, ok := req.(*protos.TCreateCheckpointRequest); ok {
		return fmt.Sprintf(
			" (disk id: %s, checkpoint id: %s)",
			createCheckpointReq.DiskId,
			createCheckpointReq.CheckpointId)
	}

	if deleteCheckpointReq, ok := req.(*protos.TDeleteCheckpointRequest); ok {
		return fmt.Sprintf(
			" (disk id: %s, checkpoint id: %s)",
			deleteCheckpointReq.DiskId,
			deleteCheckpointReq.CheckpointId)
	}

	if getChangedBlockReq, ok := req.(*protos.TGetChangedBlocksRequest); ok {
		return fmt.Sprintf(
			" (disk id: %s, start index: %d, blocks count: %d, low checkpoint id: %s, high checkpoint id: %s, ignore base disk: %t)",
			getChangedBlockReq.DiskId,
			getChangedBlockReq.StartIndex,
			getChangedBlockReq.BlocksCount,
			getChangedBlockReq.LowCheckpointId,
			getChangedBlockReq.HighCheckpointId,
			getChangedBlockReq.IgnoreBaseDisk)
	}

	if createPlacementGroupReq, ok := req.(*protos.TCreatePlacementGroupRequest); ok {
		return fmt.Sprintf(
			" (group id: %s, placement strategy: %s, placement partition count: %d)",
			createPlacementGroupReq.GroupId,
			protos.EPlacementStrategy_name[int32(createPlacementGroupReq.PlacementStrategy)],
			createPlacementGroupReq.PlacementPartitionCount)
	}

	if destroyPlacementGroupReq, ok := req.(*protos.TDestroyPlacementGroupRequest); ok {
		return fmt.Sprintf(
			" (group id: %s)",
			destroyPlacementGroupReq.GroupId)
	}

	if alterPlacementGroupMembershipReq, ok := req.(*protos.TAlterPlacementGroupMembershipRequest); ok {
		return fmt.Sprintf(
			" (group id: %s, number of disks to add: %d, number of disks to remove: %d, placement partition index: %d)",
			alterPlacementGroupMembershipReq.GroupId,
			len(alterPlacementGroupMembershipReq.DisksToAdd),
			len(alterPlacementGroupMembershipReq.DisksToRemove),
			alterPlacementGroupMembershipReq.PlacementPartitionIndex)
	}

	return ""
}
