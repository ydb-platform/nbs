#include "service_client.h"

#include <cloud/blockstore/libs/service/request_helpers.h>
#include <cloud/blockstore/libs/storage/api/partition.h>
#include <cloud/blockstore/libs/storage/api/service.h>
#include <cloud/blockstore/libs/storage/api/service.h>
#include <cloud/blockstore/libs/storage/api/ss_proxy.h>
#include <cloud/blockstore/libs/storage/api/volume.h>
#include <cloud/blockstore/libs/storage/api/volume_proxy.h>
#include <cloud/blockstore/libs/storage/core/block_handler.h>

// TODO: invalid reference
#include <cloud/blockstore/libs/storage/partition/part_events_private.h>
#include <cloud/blockstore/libs/storage/volume/volume_events_private.h>

#include <cloud/storage/core/libs/api/hive_proxy.h>

#include <contrib/ydb/core/testlib/actors/test_runtime.h>
#include <contrib/ydb/core/testlib/test_client.h>
#include <contrib/ydb/core/tx/tx_proxy/proxy.h>

#include <util/generic/guid.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

using namespace NKikimr;

using namespace NCloud::NBlockStore::NStorage::NPartition;

////////////////////////////////////////////////////////////////////////////////

TServiceClient::TServiceClient(
        NKikimr::TTestActorRuntime& runtime,
        ui32 nodeIdx,
        NProto::ERequestSource requestSource)
    : Runtime(runtime)
    , NodeIdx(nodeIdx)
    , Sender(runtime.AllocateEdgeActor(nodeIdx))
    , ClientId(CreateGuidAsString())
    , RequestSource(requestSource)
{}

std::unique_ptr<TEvService::TEvCreateVolumeRequest> TServiceClient::CreateCreateVolumeRequest(
    const TString& diskId,
    ui64 blocksCount,
    ui32 blockSize,
    const TString& folderId,
    const TString& cloudId,
    NCloud::NProto::EStorageMediaKind mediaKind,
    const NProto::TVolumePerformanceProfile& pp,
    const TString& placementGroupId,
    ui32 placementPartitionIndex,
    ui32 partitionsCount,
    const NProto::TEncryptionSpec& encryptionSpec,
    bool isSystem,
    const TString& baseDiskId,
    const TString& baseDiskCheckpointId,
    ui64 fillGeneration)
{
    auto request = std::make_unique<TEvService::TEvCreateVolumeRequest>();
    PrepareRequestHeaders(*request);
    request->Record.SetDiskId(diskId);
    request->Record.SetBlockSize(blockSize);
    request->Record.SetBlocksCount(blocksCount);
    request->Record.SetFolderId(folderId);
    request->Record.SetCloudId(cloudId);
    request->Record.SetStorageMediaKind(mediaKind);
    request->Record.MutablePerformanceProfile()->CopyFrom(pp);
    request->Record.SetPlacementGroupId(placementGroupId);
    request->Record.SetPlacementPartitionIndex(placementPartitionIndex);
    request->Record.SetPartitionsCount(partitionsCount);
    request->Record.MutableEncryptionSpec()->CopyFrom(encryptionSpec);
    request->Record.SetIsSystem(isSystem);
    request->Record.SetBaseDiskId(baseDiskId);
    request->Record.SetBaseDiskCheckpointId(baseDiskCheckpointId);
    request->Record.SetFillGeneration(fillGeneration);
    return request;
}

std::unique_ptr<TEvService::TEvDestroyVolumeRequest> TServiceClient::CreateDestroyVolumeRequest(
    const TString& diskId,
    bool destroyIfBroken,
    bool sync,
    ui64 fillGeneration)
{
    auto request = std::make_unique<TEvService::TEvDestroyVolumeRequest>();
    PrepareRequestHeaders(*request);
    request->Record.SetDiskId(diskId);
    request->Record.SetDestroyIfBroken(destroyIfBroken);
    request->Record.SetSync(sync);
    request->Record.SetFillGeneration(fillGeneration);
    return request;
}

std::unique_ptr<TEvService::TEvAssignVolumeRequest> TServiceClient::CreateAssignVolumeRequest(
    const TString& diskId,
    const TString& instanceId,
    const TString& token,
    ui64 tokenVersion)
{
    auto request = std::make_unique<TEvService::TEvAssignVolumeRequest>();
    PrepareRequestHeaders(*request);
    request->Record.SetDiskId(diskId);
    request->Record.SetInstanceId(instanceId);
    request->Record.SetToken(token);
    request->Record.SetTokenVersion(tokenVersion);
    return request;
}

std::unique_ptr<TEvService::TEvDescribeVolumeRequest> TServiceClient::CreateDescribeVolumeRequest(
    const TString& diskId)
{
    auto request = std::make_unique<TEvService::TEvDescribeVolumeRequest>();
    PrepareRequestHeaders(*request);
    request->Record.SetDiskId(diskId);
    return request;
}

std::unique_ptr<TEvService::TEvDescribeVolumeModelRequest> TServiceClient::CreateDescribeVolumeModelRequest(
    ui64 blocksCount,
    NCloud::NProto::EStorageMediaKind mediaKind)
{
    auto request = std::make_unique<TEvService::TEvDescribeVolumeModelRequest>();
    PrepareRequestHeaders(*request);
    request->Record.SetBlocksCount(blocksCount);
    request->Record.SetBlockSize(DefaultBlockSize);
    request->Record.SetStorageMediaKind(mediaKind);
    return request;
}

std::unique_ptr<TEvService::TEvListVolumesRequest> TServiceClient::CreateListVolumesRequest()
{
    auto request = std::make_unique<TEvService::TEvListVolumesRequest>();
    PrepareRequestHeaders(*request);
    return request;
}

std::unique_ptr<TEvService::TEvMountVolumeRequest> TServiceClient::CreateMountVolumeRequest(
    const TString& diskId,
    const TString& instanceId,
    const TString& token,
    const NProto::EClientIpcType ipcType,
    const NProto::EVolumeAccessMode accessMode,
    const NProto::EVolumeMountMode mountMode,
    const ui32 mountFlags,
    const ui64 mountSeqNumber,
    const NProto::TEncryptionDesc& encryptionDesc,
    const ui64 fillSeqNumber,
    const ui64 fillGeneration)
{
    auto request = std::make_unique<TEvService::TEvMountVolumeRequest>();
    PrepareRequestHeaders(*request);
    request->Record.SetDiskId(diskId);
    request->Record.SetInstanceId(instanceId);
    request->Record.SetToken(token);
    request->Record.SetIpcType(ipcType);
    request->Record.SetVolumeAccessMode(accessMode);
    request->Record.SetVolumeMountMode(mountMode);
    request->Record.SetMountFlags(mountFlags);
    request->Record.SetMountSeqNumber(mountSeqNumber);
    auto& spec = *request->Record.MutableEncryptionSpec();
    spec.SetMode(encryptionDesc.GetMode());
    spec.SetKeyHash(encryptionDesc.GetKeyHash());
    request->Record.SetFillSeqNumber(fillSeqNumber);
    request->Record.SetFillGeneration(fillGeneration);
    return request;
}

std::unique_ptr<TEvService::TEvUnmountVolumeRequest> TServiceClient::CreateUnmountVolumeRequest(
    const TString& diskId,
    const TString& sessionId)
{
    auto request = std::make_unique<TEvService::TEvUnmountVolumeRequest>();
    PrepareRequestHeaders(*request);
    request->Record.SetDiskId(diskId);
    request->Record.SetSessionId(sessionId);
    return request;
}

std::unique_ptr<TEvService::TEvUnmountVolumeRequest>  TServiceClient::CreateUnmountVolumeRequest(
    const TString& diskId,
    const TString& sessionId,
    NProto::EControlRequestSource source)
{
    auto request = std::make_unique<TEvService::TEvUnmountVolumeRequest>();
    PrepareRequestHeaders(*request, source);
    request->Record.SetDiskId(diskId);
    request->Record.SetSessionId(sessionId);
    return request;
}

std::unique_ptr<TEvService::TEvStatVolumeRequest> TServiceClient::CreateStatVolumeRequest(
    const TString& diskId,
    const TVector<TString>& storageConfigFields)
{
    auto request = std::make_unique<TEvService::TEvStatVolumeRequest>();
    PrepareRequestHeaders(*request);
    request->Record.SetDiskId(diskId);
    for (const auto& field: storageConfigFields) {
        request->Record.AddStorageConfigFields(field);
    }
    return request;
}

std::unique_ptr<TEvService::TEvResizeVolumeRequest> TServiceClient::CreateResizeVolumeRequest(
    const TString& diskId,
    ui64 blocksCount,
    const NProto::TVolumePerformanceProfile& pp)
{
    auto request = std::make_unique<TEvService::TEvResizeVolumeRequest>();
    PrepareRequestHeaders(*request);
    request->Record.SetDiskId(diskId);
    request->Record.SetBlocksCount(blocksCount);
    request->Record.MutablePerformanceProfile()->CopyFrom(pp);
    return request;
}

std::unique_ptr<TEvService::TEvWriteBlocksRequest> TServiceClient::CreateWriteBlocksRequest(
    const TString& diskId,
    const TBlockRange64& writeRange,
    const TString& sessionId,
    char fill)
{
    auto blockContent = TString(DefaultBlockSize, fill);

    auto request = std::make_unique<TEvService::TEvWriteBlocksRequest>();
    PrepareRequestHeaders(*request);
    request->Record.SetDiskId(diskId);
    request->Record.SetStartIndex(writeRange.Start);
    request->Record.SetSessionId(sessionId);

    auto& buffers = *request->Record.MutableBlocks()->MutableBuffers();
    for (ui32 i = 0; i < writeRange.Size(); ++i) {
        *buffers.Add() = blockContent;
    }

    return request;
}

std::unique_ptr<TEvService::TEvReadBlocksRequest> TServiceClient::CreateReadBlocksRequest(
    const TString& diskId,
    ui32 blockIndex,
    const TString& sessionId,
    const TString& checkpointId)
{
    auto request = std::make_unique<TEvService::TEvReadBlocksRequest>();
    PrepareRequestHeaders(*request);
    request->Record.SetDiskId(diskId);
    request->Record.SetStartIndex(blockIndex);
    request->Record.SetSessionId(sessionId);
    request->Record.SetBlocksCount(1);
    request->Record.SetCheckpointId(checkpointId);
    return request;
}

std::unique_ptr<TEvService::TEvZeroBlocksRequest> TServiceClient::CreateZeroBlocksRequest(
    const TString& diskId,
    ui32 blockIndex,
    const TString& sessionId)
{
    auto request = std::make_unique<TEvService::TEvZeroBlocksRequest>();
    PrepareRequestHeaders(*request);
    request->Record.SetDiskId(diskId);
    request->Record.SetSessionId(sessionId);
    request->Record.SetStartIndex(blockIndex);
    request->Record.SetBlocksCount(1);
    return request;
}

std::unique_ptr<TEvService::TEvReadBlocksLocalRequest> TServiceClient::CreateReadBlocksLocalRequest(
    const TString& diskId,
    ui32 blockIndex,
    TGuardedSgList sglist,
    const TString& sessionId,
    const TString& checkpointId)
{
    auto request = std::make_unique<TEvService::TEvReadBlocksLocalRequest>();
    PrepareRequestHeaders(*request);
    request->Record.SetDiskId(diskId);
    request->Record.SetStartIndex(blockIndex);
    request->Record.SetSessionId(sessionId);
    request->Record.SetBlocksCount(1);
    request->Record.SetCheckpointId(checkpointId);
    request->Record.Sglist = std::move(sglist);
    request->Record.BlockSize = DefaultBlockSize;
    return request;
}

std::unique_ptr<TEvService::TEvAlterVolumeRequest> TServiceClient::CreateAlterVolumeRequest(
    const TString& diskId,
    const TString& projectId,
    const TString& folderId,
    const TString& cloudId,
    const TString& encryptionKeyHash)
{
    auto request = std::make_unique<TEvService::TEvAlterVolumeRequest>();
    PrepareRequestHeaders(*request);
    request->Record.SetDiskId(diskId);
    request->Record.SetProjectId(projectId);
    request->Record.SetFolderId(folderId);
    request->Record.SetCloudId(cloudId);
    request->Record.SetEncryptionKeyHash(encryptionKeyHash);
    return request;
}

std::unique_ptr<TEvService::TEvGetChangedBlocksRequest> TServiceClient::CreateGetChangedBlocksRequest(
    const TString& diskId,
    ui64 startIndex,
    ui32 blocksCount,
    const TString& lowCheckpoint,
    const TString& highCheckpoint)
{
    auto request = std::make_unique<TEvService::TEvGetChangedBlocksRequest>();
    PrepareRequestHeaders(*request);
    request->Record.SetDiskId(diskId);
    request->Record.SetStartIndex(startIndex);
    request->Record.SetBlocksCount(blocksCount);
    request->Record.SetLowCheckpointId(lowCheckpoint);
    request->Record.SetHighCheckpointId(highCheckpoint);
    return request;
}

auto TServiceClient::CreateUpdateDiskRegistryConfigRequest(
    int version,
    TVector<NProto::TKnownDiskAgent> agents,
    TVector<NProto::TDeviceOverride> deviceOverrides,
    TVector<NProto::TKnownDevicePool> devicePools,
    bool ignoreVersion)
        -> std::unique_ptr<TEvService::TEvUpdateDiskRegistryConfigRequest>
{
    auto request = std::make_unique<TEvService::TEvUpdateDiskRegistryConfigRequest>();
    PrepareRequestHeaders(*request);

    request->Record.SetVersion(version);
    request->Record.SetIgnoreVersion(ignoreVersion);

    request->Record.MutableKnownAgents()->Assign(
        std::make_move_iterator(agents.begin()),
        std::make_move_iterator(agents.end())
    );

    request->Record.MutableDeviceOverrides()->Assign(
        std::make_move_iterator(deviceOverrides.begin()),
        std::make_move_iterator(deviceOverrides.end())
    );

    request->Record.MutableKnownDevicePools()->Assign(
        std::make_move_iterator(devicePools.begin()),
        std::make_move_iterator(devicePools.end())
    );

    return request;
}

auto TServiceClient::CreateDescribeDiskRegistryConfigRequest()
    -> std::unique_ptr<TEvService::TEvDescribeDiskRegistryConfigRequest>
{
    auto request = std::make_unique<TEvService::TEvDescribeDiskRegistryConfigRequest>();
    PrepareRequestHeaders(*request);

    return request;
}

auto TServiceClient::CreateCreateVolumeFromDeviceRequest(
    const TString& diskId,
    const TString& agentId,
    const TString& path,
    const TString& folderId,
    const TString& cloudId)
        -> std::unique_ptr<TEvService::TEvCreateVolumeFromDeviceRequest>
{
    auto request = std::make_unique<TEvService::TEvCreateVolumeFromDeviceRequest>();
    PrepareRequestHeaders(*request);

    request->Record.SetDiskId(diskId);
    request->Record.SetAgentId(agentId);
    request->Record.SetPath(path);
    request->Record.SetFolderId(folderId);
    request->Record.SetCloudId(cloudId);

    return request;
}

auto TServiceClient::CreateResumeDeviceRequest(
    const TString& agentId,
    const TString& path)
        -> std::unique_ptr<TEvService::TEvResumeDeviceRequest>
{
    auto request = std::make_unique<TEvService::TEvResumeDeviceRequest>();
    PrepareRequestHeaders(*request);

    request->Record.SetAgentId(agentId);
    request->Record.SetPath(path);

    return request;
}

std::unique_ptr<TEvService::TEvCreatePlacementGroupRequest> TServiceClient::CreateCreatePlacementGroupRequest(
    const TString& groupId,
    const NProto::EPlacementStrategy placementStrategy,
    const ui32 placementPartitionCount)
{
    auto request = std::make_unique<TEvService::TEvCreatePlacementGroupRequest>();
    PrepareRequestHeaders(*request);

    request->Record.SetGroupId(groupId);
    request->Record.SetPlacementStrategy(placementStrategy);
    request->Record.SetPlacementPartitionCount(placementPartitionCount);

    return request;
}

std::unique_ptr<TEvService::TEvDestroyPlacementGroupRequest> TServiceClient::CreateDestroyPlacementGroupRequest(
    const TString& groupId)
{
    auto request = std::make_unique<TEvService::TEvDestroyPlacementGroupRequest>();
    PrepareRequestHeaders(*request);

    request->Record.SetGroupId(groupId);

    return request;
}

std::unique_ptr<TEvService::TEvAlterPlacementGroupMembershipRequest> TServiceClient::CreateAlterPlacementGroupMembershipRequest(
    const TString& groupId,
    const TVector<TString>& toAdd,
    const TVector<TString>& toRemove)
{
    auto request = std::make_unique<TEvService::TEvAlterPlacementGroupMembershipRequest>();
    PrepareRequestHeaders(*request);

    request->Record.SetGroupId(groupId);
    for (const auto& diskId: toAdd) {
        *request->Record.AddDisksToAdd() = diskId;
    }

    for (const auto& diskId: toRemove) {
        *request->Record.AddDisksToRemove() = diskId;
    }

    return request;
}

std::unique_ptr<TEvService::TEvListPlacementGroupsRequest> TServiceClient::CreateListPlacementGroupsRequest()
{
    auto request = std::make_unique<TEvService::TEvListPlacementGroupsRequest>();
    PrepareRequestHeaders(*request);

    return request;
}

std::unique_ptr<TEvService::TEvDescribePlacementGroupRequest> TServiceClient::CreateDescribePlacementGroupRequest(
    const TString& groupId)
{
    auto request = std::make_unique<TEvService::TEvDescribePlacementGroupRequest>();
    PrepareRequestHeaders(*request);

    request->Record.SetGroupId(groupId);

    return request;
}

std::unique_ptr<TEvService::TEvExecuteActionRequest> TServiceClient::CreateExecuteActionRequest(
    const TString& action,
    const TString& input)
{
    auto request = std::make_unique<TEvService::TEvExecuteActionRequest>();
    PrepareRequestHeaders(*request);

    request->Record.SetAction(action);
    request->Record.SetInput(input);

    return request;
}

std::unique_ptr<TEvService::TEvCreateCheckpointRequest> TServiceClient::CreateCreateCheckpointRequest(
    const TString& diskId,
    const TString& checkpointId)
{
    auto request = std::make_unique<TEvService::TEvCreateCheckpointRequest>();
    PrepareRequestHeaders(*request);

    request->Record.SetDiskId(diskId);
    request->Record.SetCheckpointId(checkpointId);

    return request;
}

std::unique_ptr<TEvService::TEvDeleteCheckpointRequest> TServiceClient::CreateDeleteCheckpointRequest(
    const TString& diskId,
    const TString& checkpointId)
{
    auto request = std::make_unique<TEvService::TEvDeleteCheckpointRequest>();
    PrepareRequestHeaders(*request);

    request->Record.SetDiskId(diskId);
    request->Record.SetCheckpointId(checkpointId);

    return request;
}

std::unique_ptr<TEvService::TEvGetVolumeStatsRequest> TServiceClient::CreateGetVolumeStatsRequest()
{
    auto request = std::make_unique<TEvService::TEvGetVolumeStatsRequest>();
    return request;
}


std::unique_ptr<TEvService::TEvAddTagsRequest>
TServiceClient::CreateAddTagsRequest(
    const TString& diskId,
    const TVector<TString>& tagsToAdd)
{
    auto request =
        std::make_unique<TEvService::TEvAddTagsRequest>(diskId, tagsToAdd);
    return request;
}

std::unique_ptr<TEvService::TEvCreateVolumeLinkRequest>
TServiceClient::CreateCreateVolumeLinkRequest(
    const TString& leaderDiskId,
    const TString& followerDiskId)
{
    auto request = std::make_unique<TEvService::TEvCreateVolumeLinkRequest>();
    request->Record.SetLeaderDiskId(leaderDiskId);
    request->Record.SetFollowerDiskId(followerDiskId);
    return request;
}

std::unique_ptr<TEvService::TEvDestroyVolumeLinkRequest>
TServiceClient::CreateDestroyVolumeLinkRequest(
    const TString& leaderDiskId,
    const TString& followerDiskId)
{
    auto request = std::make_unique<TEvService::TEvDestroyVolumeLinkRequest>();
    request->Record.SetLeaderDiskId(leaderDiskId);
    request->Record.SetFollowerDiskId(followerDiskId);
    return request;
}

void TServiceClient::WaitForVolume(const TString& diskId)
{
    auto request = std::make_unique<TEvVolume::TEvWaitReadyRequest>();
    request->Record.SetDiskId(diskId);
    SendRequest(MakeVolumeProxyServiceId(), std::move(request));
    auto response = RecvResponse<TEvVolume::TEvWaitReadyResponse>();
    UNIT_ASSERT_C(SUCCEEDED(response->GetStatus()), response->GetErrorReason());
}

}   // namespace NCloud::NBlockStore::NStorage
