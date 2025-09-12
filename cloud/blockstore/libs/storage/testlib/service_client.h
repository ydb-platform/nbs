#pragma once

#include <cloud/blockstore/libs/common/block_range.h>
#include <cloud/blockstore/libs/storage/api/service.h>
#include <cloud/blockstore/libs/storage/service/service_events_private.h>
#include <cloud/blockstore/libs/storage/testlib/test_env.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/datetime/base.h>
#include <util/generic/string.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

static const TString DefaultDiskId = "path_to_test_volume";
static const ui32 DefaultBlocksCount = 1024;

////////////////////////////////////////////////////////////////////////////////

class TServiceClient
{
private:
    NKikimr::TTestActorRuntime& Runtime;
    ui32 NodeIdx;
    NActors::TActorId Sender;
    TString ClientId;
    NProto::ERequestSource RequestSource;

public:
    TServiceClient(
        NKikimr::TTestActorRuntime& runtime,
        ui32 nodeIdx = 0,
        NProto::ERequestSource requestSource = NProto::SOURCE_TCP_DATA_CHANNEL);

    const TString& GetClientId() const
    {
        return ClientId;
    }

    void SetClientId(TString clientId)
    {
        ClientId = std::move(clientId);
    }

    const NActors::TActorId& GetSender() const
    {
        return Sender;
    }

    template <typename TRequest>
    void SendRequest(
        const NActors::TActorId& recipient,
        std::unique_ptr<TRequest> request)
    {
        auto* ev = new NActors::IEventHandle(
            recipient,
            Sender,
            request.release());

        Runtime.Send(ev, NodeIdx);
    }

    template <typename TResponse>
    std::unique_ptr<TResponse> RecvResponse()
    {
        TAutoPtr<NActors::IEventHandle> handle;
        Runtime.GrabEdgeEventRethrow<TResponse>(handle);
        return std::unique_ptr<TResponse>(handle->Release<TResponse>().Release());
    }

    template <typename TRequest>
    void PrepareRequestHeaders(TRequest& request)
    {
        request.Record.MutableHeaders()->SetClientId(ClientId);
        request.Record.MutableHeaders()->MutableInternal()->SetRequestSource(RequestSource);
    }

    template <typename TRequest>
    void PrepareRequestHeaders(TRequest& request, NProto::EControlRequestSource source)
    {
        PrepareRequestHeaders(request);
        request.Record.MutableHeaders()->MutableInternal()->SetControlSource(source);
    }

    std::unique_ptr<TEvService::TEvCreateVolumeRequest> CreateCreateVolumeRequest(
        const TString& diskId = DefaultDiskId,
        ui64 blocksCount = DefaultBlocksCount,
        ui32 blockSize = DefaultBlockSize,
        const TString& folderId = "",
        const TString& cloudId = "",
        NCloud::NProto::EStorageMediaKind mediaKind = NCloud::NProto::STORAGE_MEDIA_DEFAULT,
        const NProto::TVolumePerformanceProfile& pp = {},
        const TString& placementGroupId = {},
        ui32 placementPartitionIndex = 0,
        ui32 partitionsCount = 0,
        const NProto::TEncryptionSpec& encryptionSpec = {},
        bool isSystem = false,
        const TString& baseDiskId = TString(),
        const TString& baseDiskCheckpointId = TString(),
        ui64 fillGeneration = 0);

    std::unique_ptr<TEvService::TEvDestroyVolumeRequest> CreateDestroyVolumeRequest(
        const TString& diskId = DefaultDiskId,
        bool destroyIfBroken = false,
        bool sync = false,
        ui64 fillGeneration = 0,
        bool useStrictDiskId = false);

    std::unique_ptr<TEvService::TEvAssignVolumeRequest> CreateAssignVolumeRequest(
        const TString& diskId = DefaultDiskId,
        const TString& instanceId = {},
        const TString& token = {},
        ui64 tokenVersion = 0);

    std::unique_ptr<TEvService::TEvDescribeVolumeRequest> CreateDescribeVolumeRequest(
        const TString& diskId = DefaultDiskId);

    std::unique_ptr<TEvService::TEvDescribeVolumeModelRequest> CreateDescribeVolumeModelRequest(
        ui64 blocksCount,
        NCloud::NProto::EStorageMediaKind mediaKind);

    std::unique_ptr<TEvService::TEvListVolumesRequest> CreateListVolumesRequest();

    std::unique_ptr<TEvService::TEvMountVolumeRequest> CreateMountVolumeRequest(
        const TString& diskId = DefaultDiskId,
        const TString& instanceId = {},
        const TString& token = {},
        const NProto::EClientIpcType ipcType = NProto::IPC_GRPC,
        const NProto::EVolumeAccessMode accessMode = NProto::VOLUME_ACCESS_READ_WRITE,
        const NProto::EVolumeMountMode mountMode = NProto::VOLUME_MOUNT_LOCAL,
        const ui32 mountFlags = 0,
        const ui64 mountSeqNumber = 0,
        const NProto::TEncryptionDesc& encryptionDesc = {},
        const ui64 fillSeqNumber = 0,
        const ui64 fillGeneration = 0);

    std::unique_ptr<TEvService::TEvUnmountVolumeRequest> CreateUnmountVolumeRequest(
        const TString& diskId = DefaultDiskId,
        const TString& sessionId = {});

    std::unique_ptr<TEvService::TEvUnmountVolumeRequest> CreateUnmountVolumeRequest(
        const TString& diskId,
        const TString& sessionId,
        NProto::EControlRequestSource source);

    std::unique_ptr<TEvService::TEvStatVolumeRequest> CreateStatVolumeRequest(
        const TString& diskId = DefaultDiskId,
        const TVector<TString>& storageConfigFields = {});

    std::unique_ptr<TEvService::TEvResizeVolumeRequest> CreateResizeVolumeRequest(
        const TString& diskId = DefaultDiskId,
        ui64 blocksCount = DefaultBlocksCount,
        const NProto::TVolumePerformanceProfile& pp = {});

    std::unique_ptr<TEvService::TEvWriteBlocksRequest> CreateWriteBlocksRequest(
        const TString& diskId,
        const TBlockRange64& writeRange,
        const TString& sessionId,
        char fill = 0);

    std::unique_ptr<TEvService::TEvReadBlocksRequest> CreateReadBlocksRequest(
       const TString& diskId,
        ui32 blockIndex,
        const TString& sessionId,
        const TString& checkpointId = {});

    std::unique_ptr<TEvService::TEvZeroBlocksRequest> CreateZeroBlocksRequest(
        const TString& diskId,
        ui32 blockIndex,
        const TString& sessionId);

    std::unique_ptr<TEvService::TEvReadBlocksLocalRequest> CreateReadBlocksLocalRequest(
        const TString& diskId,
        ui32 blockIndex,
        TGuardedSgList sglist,
        const TString& sessionId,
        const TString& checkpointId = {});

    std::unique_ptr<TEvService::TEvAlterVolumeRequest> CreateAlterVolumeRequest(
        const TString& diskId,
        const TString& projectId,
        const TString& folderId,
        const TString& cloudId,
        const TString& encryptionKeyHash = "");

    std::unique_ptr<TEvService::TEvGetChangedBlocksRequest> CreateGetChangedBlocksRequest(
        const TString& diskId,
        ui64 startIndex,
        ui32 blocksCount,
        const TString& lowCheckpoint = {},
        const TString& highCheckpoint = {});

    auto CreateUpdateDiskRegistryConfigRequest(
        int version,
        TVector<NProto::TKnownDiskAgent> agents,
        TVector<NProto::TDeviceOverride> deviceOverrides = {},
        TVector<NProto::TKnownDevicePool> devicePools = {},
        bool ignoreVersion = false)
            -> std::unique_ptr<TEvService::TEvUpdateDiskRegistryConfigRequest>;

    auto CreateDescribeDiskRegistryConfigRequest()
        -> std::unique_ptr<TEvService::TEvDescribeDiskRegistryConfigRequest>;

    auto CreateQueryAvailableStorageRequest()
        -> std::unique_ptr<TEvService::TEvQueryAvailableStorageRequest>;

    auto CreateCreateVolumeFromDeviceRequest(
        const TString& diskId,
        const TString& agentId,
        const TString& path,
        const TString& folderId = "yc.nbs",
        const TString& cloudId = "yc.nbs-test")
            -> std::unique_ptr<TEvService::TEvCreateVolumeFromDeviceRequest>;

    auto CreateResumeDeviceRequest(const TString& agentId, const TString& path)
        -> std::unique_ptr<TEvService::TEvResumeDeviceRequest>;

    std::unique_ptr<TEvService::TEvCreatePlacementGroupRequest> CreateCreatePlacementGroupRequest(
        const TString& groupId,
        const NProto::EPlacementStrategy placementStrategy,
        const ui32 placementPartitionCount);

    std::unique_ptr<TEvService::TEvDestroyPlacementGroupRequest> CreateDestroyPlacementGroupRequest(
        const TString& groupId);

    std::unique_ptr<TEvService::TEvAlterPlacementGroupMembershipRequest> CreateAlterPlacementGroupMembershipRequest(
        const TString& groupId,
        const TVector<TString>& toAdd,
        const TVector<TString>& toRemove);

    std::unique_ptr<TEvService::TEvListPlacementGroupsRequest> CreateListPlacementGroupsRequest();

    std::unique_ptr<TEvService::TEvDescribePlacementGroupRequest> CreateDescribePlacementGroupRequest(
        const TString& groupId);

    std::unique_ptr<TEvService::TEvExecuteActionRequest> CreateExecuteActionRequest(
        const TString& action,
        const TString& input);

    std::unique_ptr<TEvService::TEvCreateCheckpointRequest> CreateCreateCheckpointRequest(
        const TString& diskId,
        const TString& checkpointId);

    std::unique_ptr<TEvService::TEvDeleteCheckpointRequest> CreateDeleteCheckpointRequest(
        const TString& diskId,
        const TString& checkpointId);

    std::unique_ptr<TEvService::TEvGetVolumeStatsRequest> CreateGetVolumeStatsRequest();

    std::unique_ptr<TEvService::TEvAddTagsRequest> CreateAddTagsRequest(
        const TString& diskId,
        const TVector<TString>& tagsToAdd);

    std::unique_ptr<TEvService::TEvCreateVolumeLinkRequest>
    CreateCreateVolumeLinkRequest(
        const TString& leaderDiskId,
        const TString& followerDiskId);

    std::unique_ptr<TEvService::TEvDestroyVolumeLinkRequest>
    CreateDestroyVolumeLinkRequest(
        const TString& leaderDiskId,
        const TString& followerDiskId);

    void WaitForVolume(const TString& diskId = DefaultDiskId);

#define BLOCKSTORE_DECLARE_METHOD(name, ns)                                    \
    template <typename... Args>                                                \
    void Send##name##Request(Args&&... args)                                   \
    {                                                                          \
        auto request = Create##name##Request(std::forward<Args>(args)...);     \
        SendRequest(MakeStorageServiceId(), std::move(request));               \
    }                                                                          \
                                                                               \
    std::unique_ptr<ns::TEv##name##Response> Recv##name##Response()            \
    {                                                                          \
        return RecvResponse<ns::TEv##name##Response>();                        \
    }                                                                          \
                                                                               \
    template <typename... Args>                                                \
    std::unique_ptr<ns::TEv##name##Response> name(Args&&... args)              \
    {                                                                          \
        auto request = Create##name##Request(std::forward<Args>(args)...);     \
        SendRequest(MakeStorageServiceId(), std::move(request));               \
                                                                               \
        auto response = RecvResponse<ns::TEv##name##Response>();               \
        UNIT_ASSERT_C(                                                         \
            SUCCEEDED(response->GetStatus()),                                  \
            response->GetErrorReason());                                       \
        return response;                                                       \
    }                                                                          \
// BLOCKSTORE_DECLARE_METHOD

    BLOCKSTORE_STORAGE_SERVICE(BLOCKSTORE_DECLARE_METHOD, TEvService)
    BLOCKSTORE_SERVICE_REQUESTS(BLOCKSTORE_DECLARE_METHOD, TEvService)

#undef BLOCKSTORE_DECLARE_METHOD
};

}   // namespace NCloud::NBlockStore::NStorage
