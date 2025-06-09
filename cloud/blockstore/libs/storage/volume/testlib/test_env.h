#pragma once

#include <cloud/blockstore/libs/diagnostics/block_digest.h>
#include <cloud/blockstore/libs/diagnostics/critical_events.h>
#include <cloud/blockstore/libs/diagnostics/profile_log.h>
#include <cloud/blockstore/libs/diagnostics/public.h>
#include <cloud/blockstore/libs/rdma_test/client_test.h>
#include <cloud/blockstore/libs/service/request_helpers.h>
#include <cloud/blockstore/libs/storage/api/bootstrapper.h>
#include <cloud/blockstore/libs/storage/api/disk_registry.h>
#include <cloud/blockstore/libs/storage/api/disk_registry_proxy.h>
#include <cloud/blockstore/libs/storage/api/partition.h>
#include <cloud/blockstore/libs/storage/api/service.h>
#include <cloud/blockstore/libs/storage/api/ss_proxy.h>
#include <cloud/blockstore/libs/storage/api/stats_service.h>
#include <cloud/blockstore/libs/storage/api/volume.h>
#include <cloud/blockstore/libs/storage/core/config.h>
#include <cloud/blockstore/libs/storage/core/proto_helpers.h>
#include <cloud/blockstore/libs/storage/disk_agent/model/public.h>
#include <cloud/blockstore/libs/storage/model/channel_data_kind.h>
#include <cloud/blockstore/libs/storage/stats_service/stats_service.h>
#include <cloud/blockstore/libs/storage/testlib/disk_agent_mock.h>
#include <cloud/blockstore/libs/storage/testlib/disk_registry_proxy_mock.h>
#include <cloud/blockstore/libs/storage/testlib/test_runtime.h>
#include <cloud/blockstore/libs/storage/volume/volume.h>
#include <cloud/blockstore/libs/storage/volume/volume_events_private.h>
#include <cloud/blockstore/public/api/protos/volume.pb.h>

#include <cloud/storage/core/libs/api/hive_proxy.h>
#include <cloud/storage/core/libs/common/sglist_test.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>
#include <cloud/storage/core/libs/diagnostics/trace_serializer.h>

// TODO: invalid reference
#include <cloud/blockstore/libs/storage/partition/part_events_private.h>
#include <cloud/blockstore/libs/storage/partition_nonrepl/part_nonrepl_events_private.h>
#include <cloud/blockstore/libs/storage/service/service_events_private.h>

#include <cloud/blockstore/libs/diagnostics/config.h>
#include <cloud/blockstore/libs/kikimr/helpers.h>

#include <contrib/ydb/core/blockstore/core/blockstore.h>
#include <contrib/ydb/core/mind/local.h>
#include <library/cpp/lwtrace/mon/mon_lwtrace.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/size_literals.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

using namespace NKikimr;

using namespace NCloud::NBlockStore::NStorage::NPartition;

using namespace NCloud::NStorage;

namespace NTestVolume {

////////////////////////////////////////////////////////////////////////////////

constexpr TDuration WaitTimeout = TDuration::Seconds(5);

TString GetBlockContent(char fill = 0, size_t size = DefaultBlockSize);

TDiagnosticsConfigPtr CreateTestDiagnosticsConfig();

////////////////////////////////////////////////////////////////////////////////

void CheckForkJoin(const NLWTrace::TShuttleTrace& trace, bool forkRequired);
bool HasProbe(const NLWTrace::TShuttleTrace& trace, const TString& probeName);

////////////////////////////////////////////////////////////////////////////////

class TFakeHiveProxy final
    : public TActor<TFakeHiveProxy>
{
private:
    ui32 PartitionGeneration = 0;

public:
    TFakeHiveProxy()
        : TActor(&TThis::StateWork)
    {}

private:
    STFUNC(StateWork)
    {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvHiveProxy::TEvGetStorageInfoRequest, HandleGetStorageInfo);
            HFunc(TEvHiveProxy::TEvBootExternalRequest, HandleBootExternal);
            IgnoreFunc(TEvLocal::TEvTabletMetrics);

            default:
                HandleUnexpectedEvent(
                    ev,
                    TBlockStoreComponents::HIVE_PROXY,
                    __PRETTY_FUNCTION__);
        }
    }

    void HandleGetStorageInfo(
        const TEvHiveProxy::TEvGetStorageInfoRequest::TPtr& ev,
        const TActorContext& ctx);

    void HandleBootExternal(
        const TEvHiveProxy::TEvBootExternalRequest::TPtr& ev,
        const TActorContext& ctx);
};

////////////////////////////////////////////////////////////////////////////////

class TFakeStorageService final
    : public NActors::TActor<TFakeStorageService>
{
public:
    TFakeStorageService()
        : TActor(&TThis::StateWork)
    {}

private:
    STFUNC(StateWork)
    {
        Y_UNUSED(ev);

        // do nothing
    }
};

////////////////////////////////////////////////////////////////////////////////

class TFakeStorageStatsService final
    : public NActors::TActor<TFakeStorageStatsService>
{
public:
    TFakeStorageStatsService()
        : TActor(&TThis::StateWork)
    {}

private:
    STFUNC(StateWork)
    {
        Y_UNUSED(ev);

        // do nothing
    }
};

////////////////////////////////////////////////////////////////////////////////
// Test Actor System helpers
// implemented as macros because otherwise UNIT_ASSERTs would show wrong line
// numbers in case of failure

#define TICK(runtime) runtime->AdvanceCurrentTime(TDuration::Seconds(1));
// TICK

#define TEST_RESPONSE_IMPL(volume, eventNamespace, name, status, timeout) {         \
    auto response =                                                                 \
        volume.TryRecvResponse<eventNamespace::TEv ## name ## Response>(            \
            timeout);                                                               \
    UNIT_ASSERT(response);                                                          \
    UNIT_ASSERT_VALUES_EQUAL(status, response->GetStatus());                        \
} // TEST_RESPONSE_IMPL

#define TEST_RESPONSE(volume, name, status, timeout) {                              \
    TEST_RESPONSE_IMPL(volume, TEvService, name, status, timeout);                  \
} // TEST_RESPONSE

#define TEST_RESPONSE_VOLUME_EVENT(volume, name, status, timeout) {                 \
    TEST_RESPONSE_IMPL(volume, TEvVolume, name, status, timeout);                   \
} // TEST_RESPONSE_VOLUME_EVENT

#define TEST_QUICK_RESPONSE_IMPL(runtime, eventNamespace, name, status) {           \
    runtime->DispatchEvents({}, TInstant::Zero());                                  \
    auto evList = runtime->CaptureEvents();                                         \
    std::unique_ptr<IEventHandle> handle;                                           \
    for (auto& ev : evList) {                                                       \
        if (ev->GetTypeRewrite() == eventNamespace::Ev ## name ## Response) {       \
            handle.reset(ev.Release());                                             \
            break;                                                                  \
        }                                                                           \
    }                                                                               \
    UNIT_ASSERT(handle);                                                            \
    auto response =                                                                 \
        handle->Release<eventNamespace::TEv ## name ## Response>();                 \
    UNIT_ASSERT_VALUES_EQUAL(status, response->GetStatus());                        \
    runtime->PushEventsFront(evList);                                               \
} // TEST_QUICK_RESPONSE

#define TEST_QUICK_RESPONSE(runtime, name, status) {                                \
    TEST_QUICK_RESPONSE_IMPL(runtime, TEvService, name, status);                    \
} // TEST_QUICK_RESPONSE

#define TEST_QUICK_RESPONSE_VOLUME_EVENT(runtime, name, status) {                   \
    TEST_QUICK_RESPONSE_IMPL(runtime, TEvVolume, name, status);                     \
} // TEST_QUICK_RESPONSE_VOLUME_EVENT

#define TEST_NO_RESPONSE_IMPL(runtime, eventNamespace, name) {                      \
    runtime->DispatchEvents({}, TInstant::Zero());                                  \
    auto evList = runtime->CaptureEvents();                                         \
    for (auto& ev : evList) {                                                       \
        UNIT_ASSERT(                                                                \
            ev->GetTypeRewrite() != eventNamespace::Ev ## name ## Response);        \
    }                                                                               \
    runtime->PushEventsFront(evList);                                               \
} // TEST_NO_RESPONSE

#define TEST_NO_RESPONSE(runtime, name) {                                           \
    TEST_NO_RESPONSE_IMPL(runtime, TEvService, name);                               \
} // TEST_NO_RESPONSE

#define TEST_NO_RESPONSE_VOLUME_EVENT(runtime, name) {                              \
    TEST_NO_RESPONSE_IMPL(runtime, TEvVolume, name);                                \
} // TEST_NO_RESPONSE_VOLUME_EVENT

////////////////////////////////////////////////////////////////////////////////

class TVolumeClient
{
private:
    TTestActorRuntime& Runtime;
    const ui32 NodeIdx;
    const ui64 VolumeTabletId;

    const TActorId Sender;
    TActorId PipeClient;

public:
    TVolumeClient(
            TTestActorRuntime& runtime,
            ui32 nodeIdx = 0,
            ui64 volumeTabletId = TestVolumeTablets[0])
        : Runtime(runtime)
        , NodeIdx(nodeIdx)
        , VolumeTabletId(volumeTabletId)
        , Sender(runtime.AllocateEdgeActor(nodeIdx))
    {
        ReconnectPipe();
    }

    TActorId GetSender() const;

    void ReconnectPipe();

    // Attention! During reboot the filter set via runtime->SetObserverFunc()
    // will not get messages.
    void RebootTablet();

    void RebootSysTablet();

    template <typename TRequest>
    void SendToPipe(std::unique_ptr<TRequest> request, ui64 cookie = 0)
    {
        Runtime.SendToPipe(
            PipeClient,
            Sender,
            request.release(),
            NodeIdx,
            cookie);
    }

    void ForwardToPipe(TAutoPtr<IEventHandle>& event)
    {
        Runtime.SendToPipe(
            PipeClient,
            event->Sender,
            event->ReleaseBase().Release(),
            NodeIdx,
            event->Cookie);
    }

    template <typename TResponse>
    std::unique_ptr<TResponse> TryRecvResponse(TDuration timeout = WaitTimeout)
    {
        TAutoPtr<IEventHandle> handle;
        Runtime.GrabEdgeEventRethrow<TResponse>(handle, timeout);

        if (handle) {
            return std::unique_ptr<TResponse>(
                handle->Release<TResponse>().Release());
        } else {
            return nullptr;
        }
    }

    template <typename TResponse>
    std::unique_ptr<TResponse> RecvResponse()
    {
        TAutoPtr<IEventHandle> handle;
        Runtime.GrabEdgeEventRethrow<TResponse>(handle, WaitTimeout);

        UNIT_ASSERT(handle);
        return std::unique_ptr<TResponse>(handle->Release<TResponse>().Release());
    }

    std::unique_ptr<TEvBlockStore::TEvUpdateVolumeConfig>
    CreateUpdateVolumeConfigRequest(
        ui64 maxBandwidth = 0,
        ui32 maxIops = 0,
        ui32 burstPercentage = 0,
        ui64 maxPostponedWeight = 0,
        bool throttlingEnabled = false,
        ui32 version = 1,
        NCloud::NProto::EStorageMediaKind mediaKind =
            NCloud::NProto::EStorageMediaKind::STORAGE_MEDIA_HYBRID,
        ui64 blockCount = 1024,
        TString diskId = "vol0",
        TString cloudId = "cloud",
        TString folderId = "folder",
        ui32 partitionCount = 1,
        ui32 blocksPerStripe = 0,
        TString tags = "",
        TString baseDiskId = "",
        TString baseDiskCheckpointId = "",
        NProto::EEncryptionMode encryption =
            NProto::EEncryptionMode::NO_ENCRYPTION);

    auto CreateReallocateDiskRequest()
    {
        return std::make_unique<TEvVolume::TEvReallocateDiskRequest>();
    }

    template <typename... Args>
    void SendUpdateVolumeConfigRequest(Args&&... args)
    {
        auto request =
            CreateUpdateVolumeConfigRequest(std::forward<Args>(args)...);

        SendToPipe(std::move(request));
    }

    std::unique_ptr<TEvBlockStore::TEvUpdateVolumeConfigResponse> RecvUpdateVolumeConfigResponse()
    {
        return RecvResponse<TEvBlockStore::TEvUpdateVolumeConfigResponse>();
    }

    template <typename... Args>
    void UpdateVolumeConfig(Args&&... args)
    {
        SendUpdateVolumeConfigRequest(std::forward<Args>(args)...);
        auto response = RecvUpdateVolumeConfigResponse();
        UNIT_ASSERT_C(
            response->Record.GetStatus() == NKikimrBlockStore::OK,
            "Unexpected status: " <<
            NKikimrBlockStore::EStatus_Name(response->Record.GetStatus()));
    }

    void CreateVolume(
        std::unique_ptr<TEvBlockStore::TEvUpdateVolumeConfig> request)
    {
        // Send CreateVolumeRequest to SchemeShard.
        Send(
            Runtime,
            MakeSSProxyServiceId(),
            NActors::TActorId(),
            std::make_unique<TEvSSProxy::TEvCreateVolumeRequest>(
                request->Record.GetVolumeConfig()));

        // Send UpdateVolumeConfig to VolumeActor.
        SendToPipe(std::move(request));
    }

    auto TagUpdater(NProto::EStorageMediaKind mediaKind, ui64 blockCount)
    {
        return [this, mediaKind, blockCount, version = 1] (auto tags) mutable {
            UpdateVolumeConfig(
                0,
                0,
                0,
                0,
                false,
                version++,
                mediaKind,
                blockCount,
                "vol0",
                "cloud",
                "folder",
                1,
                0,
                tags);
        };
    }

    std::unique_ptr<TEvVolume::TEvWaitReadyRequest> CreateWaitReadyRequest();

    std::unique_ptr<TEvVolume::TEvAddClientRequest> CreateAddClientRequest(
        const NProto::TVolumeClientInfo& info);

    std::unique_ptr<TEvVolume::TEvRemoveClientRequest> CreateRemoveClientRequest(
        const TString& clientId);

    std::unique_ptr<TEvService::TEvStatVolumeRequest> CreateStatVolumeRequest(
        const TString& clientId = {},
        const TVector<TString>& storageConfigFields = {},
        const bool noPartition = false);

    std::unique_ptr<TEvService::TEvReadBlocksRequest> CreateReadBlocksRequest(
        const TBlockRange64& readRange,
        const TString& clientId,
        const TString& checkpointId = {});

    std::unique_ptr<TEvService::TEvReadBlocksLocalRequest> CreateReadBlocksLocalRequest(
        const TBlockRange64& readRange,
        const TGuardedSgList& sglist,
        const TString& clientId,
        const TString& checkpointId = {});

    std::unique_ptr<TEvService::TEvWriteBlocksRequest> CreateWriteBlocksRequest(
        const TBlockRange64& writeRange,
        const TString& clientId,
        char fill = 0);

    std::unique_ptr<TEvService::TEvWriteBlocksRequest> CreateWriteBlocksRequest(
        const TBlockRange64& writeRange,
        const TString& clientId,
        const TString& blockContent);

    std::unique_ptr<TEvService::TEvWriteBlocksLocalRequest> CreateWriteBlocksLocalRequest(
        const TBlockRange64& writeRange,
        const TString& clientId,
        const TString& blockContent);

    std::unique_ptr<TEvService::TEvZeroBlocksRequest> CreateZeroBlocksRequest(
        const TBlockRange64& zeroRange,
        const TString& clientId);

    std::unique_ptr<TEvVolume::TEvDescribeBlocksRequest> CreateDescribeBlocksRequest(
        const TBlockRange64& range,
        const TString& clientId,
        ui32 blocksCountToRead = 0);

    std::unique_ptr<TEvService::TEvCreateCheckpointRequest> CreateCreateCheckpointRequest(
        const TString& checkpointId,
        NProto::ECheckpointType checkpointType = NProto::ECheckpointType::NORMAL);

    std::unique_ptr<TEvService::TEvDeleteCheckpointRequest> CreateDeleteCheckpointRequest(
        const TString& checkpointId);

    std::unique_ptr<TEvService::TEvGetChangedBlocksRequest> CreateGetChangedBlocksRequest(
        const TBlockRange64& range,
        const TString& lowCheckpointId,
        const TString& highCheckpointId);

    std::unique_ptr<TEvVolume::TEvDeleteCheckpointDataRequest> CreateDeleteCheckpointDataRequest(
        const TString& checkpointId);

    std::unique_ptr<TEvService::TEvGetCheckpointStatusRequest>
    CreateGetCheckpointStatusRequest(const TString& checkpointId);

    std::unique_ptr<TEvPartition::TEvBackpressureReport> CreateBackpressureReport(
        const TBackpressureReport& report);

    std::unique_ptr<TEvVolume::TEvCompactRangeRequest> CreateCompactRangeRequest(
        const TBlockRange64& range,
        const TString& operationId);

    std::unique_ptr<TEvVolume::TEvGetCompactionStatusRequest> CreateGetCompactionStatusRequest(
        const TString& operationId);

    std::unique_ptr<TEvVolume::TEvUpdateUsedBlocksRequest> CreateUpdateUsedBlocksRequest(
        const TVector<TBlockRange64>& ranges,
        bool used);

    std::unique_ptr<NMon::TEvRemoteHttpInfo> CreateRemoteHttpInfo(
        const TString& params,
        HTTP_METHOD method);

    std::unique_ptr<TEvVolume::TEvRebuildMetadataRequest> CreateRebuildMetadataRequest(
        NProto::ERebuildMetadataType type,
        ui32 batchSize);

    std::unique_ptr<TEvVolume::TEvGetRebuildMetadataStatusRequest> CreateGetRebuildMetadataStatusRequest();

    std::unique_ptr<NMon::TEvRemoteHttpInfo> CreateRemoteHttpInfo(
        const TString& params);

    std::unique_ptr<TEvVolume::TEvGetVolumeInfoRequest> CreateGetVolumeInfoRequest();

    std::unique_ptr<TEvVolume::TEvUpdateVolumeParamsRequest> CreateUpdateVolumeParamsRequest(
        const TString& diskId,
        const THashMap<TString, NProto::TUpdateVolumeParamsMapValue>& volumeParams);

    std::unique_ptr<TEvVolume::TEvChangeStorageConfigRequest> CreateChangeStorageConfigRequest(
        NProto::TStorageServiceConfig patch);

    std::unique_ptr<TEvVolume::TEvGetStorageConfigRequest> CreateGetStorageConfigRequest();

    std::unique_ptr<TEvVolumePrivate::TEvDeviceTimedOutRequest>
    CreateDeviceTimedOutRequest(TString deviceUUID);

    std::unique_ptr<TEvVolumePrivate::TEvUpdateShadowDiskStateRequest> CreateUpdateShadowDiskStateRequest(
        TString checkpointId,
        TEvVolumePrivate::TEvUpdateShadowDiskStateRequest::EReason reason,
        ui64 processedBlockCount);

    std::unique_ptr<TEvVolumePrivate::TEvReadMetaHistoryRequest>
    CreateReadMetaHistoryRequest();

    std::unique_ptr<TEvVolume::TEvGracefulShutdownRequest>
    CreateGracefulShutdownRequest();

    std::unique_ptr<TEvVolume::TEvLinkLeaderVolumeToFollowerRequest>
    CreateLinkLeaderVolumeToFollowerRequest(
        const TString& leaderDiskId,
        const TString& followerDiskId);

    std::unique_ptr<TEvVolume::TEvUnlinkLeaderVolumeFromFollowerRequest>
    CreateUnlinkLeaderVolumeFromFollowerRequest(
        const TString& leaderDiskId,
        const TString& followerDiskId);

    std::unique_ptr<TEvVolumePrivate::TEvUpdateFollowerStateRequest>
    CreateUpdateFollowerStateRequest(
        TString followerUuid,
        TEvVolumePrivate::TUpdateFollowerStateRequest::EReason reason,
        std::optional<ui64> migratedBytes);

    void SendRemoteHttpInfo(
        const TString& params,
        HTTP_METHOD method);

    void SendRemoteHttpInfo(
        const TString& params);

    std::unique_ptr<NMon::TEvRemoteHttpInfoRes> RecvCreateRemoteHttpInfoRes();

    std::unique_ptr<NMon::TEvRemoteHttpInfoRes> RemoteHttpInfo(
        const TString& params,
        HTTP_METHOD method);

    std::unique_ptr<NMon::TEvRemoteHttpInfoRes> RemoteHttpInfo(
        const TString& params);

void SendPartitionTabletMetrics(
    ui64 tabletId,
    const NKikimrTabletBase::TMetrics& metrics);

#define BLOCKSTORE_DECLARE_METHOD(name, ns)                                    \
    template <typename... Args>                                                \
    void Send##name##Request(Args&&... args)                                   \
    {                                                                          \
        SendToPipe(                                                            \
            Create##name##Request(std::forward<Args>(args)...));               \
    }                                                                          \
                                                                               \
    std::unique_ptr<ns::TEv##name##Response> Recv##name##Response()            \
    {                                                                          \
        return RecvResponse<ns::TEv##name##Response>();                        \
    }                                                                          \
                                                                               \
    std::unique_ptr<ns::TEv##name##Response> Recv##name##Response(             \
        TDuration timeout)                                                     \
    {                                                                          \
        return TryRecvResponse<ns::TEv##name##Response>(timeout);              \
    }                                                                          \
                                                                               \
    template <typename... Args>                                                \
    std::unique_ptr<ns::TEv##name##Response> name(Args&&... args)              \
    {                                                                          \
        Send##name##Request(std::forward<Args>(args)...);                      \
        auto response = Recv##name##Response();                                \
        UNIT_ASSERT_C(                                                         \
            SUCCEEDED(response->GetStatus()),                                  \
            response->GetErrorReason());                                       \
        return response;                                                       \
    }                                                                          \
// BLOCKSTORE_DECLARE_METHOD

    BLOCKSTORE_VOLUME_REQUESTS(BLOCKSTORE_DECLARE_METHOD, TEvVolume)
    BLOCKSTORE_VOLUME_REQUESTS_PRIVATE(BLOCKSTORE_DECLARE_METHOD, TEvVolumePrivate)
    BLOCKSTORE_VOLUME_REQUESTS_FWD_SERVICE(BLOCKSTORE_DECLARE_METHOD, TEvService)

#undef BLOCKSTORE_DECLARE_METHOD
};

////////////////////////////////////////////////////////////////////////////////

inline NProto::TDeviceConfig MakeDevice(
    const TString& uuid,
    const TString& name,
    const TString& transportId)
{
    NProto::TDeviceConfig device;
    device.SetAgentId("agent-1");
    device.SetNodeId(0);
    device.SetBlocksCount(DefaultDeviceBlockCount);
    device.SetDeviceUUID(uuid);
    device.SetDeviceName(name);
    device.SetTransportId(transportId);
    device.SetBlockSize(DefaultDeviceBlockSize);
    return device;
};

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<TTestActorRuntime> PrepareTestActorRuntime(
    NProto::TStorageServiceConfig storageServiceConfig = {},
    TDiskRegistryStatePtr diskRegistryState = {},
    NProto::TFeaturesConfig featuresConfig = {},
    NRdma::IClientPtr rdmaClient = {},
    TVector<TDiskAgentStatePtr> diskAgentStates = {});

struct TTestRuntimeBuilder
{
    NProto::TStorageServiceConfig StorageServiceConfig;
    TDiskRegistryStatePtr DiskRegistryState = MakeIntrusive<TDiskRegistryState>();

    TTestRuntimeBuilder& With(NProto::TStorageServiceConfig config);

    TTestRuntimeBuilder& With(TDiskRegistryStatePtr state);

    std::unique_ptr<TTestActorRuntime> Build();
};

////////////////////////////////////////////////////////////////////////////////

NProto::TVolumeClientInfo CreateVolumeClientInfo(
    NProto::EVolumeAccessMode accessMode,
    NProto::EVolumeMountMode mountMode,
    ui32 mountFlags,
    ui64 mountSeqNumber = 0);

TString BuildRemoteHttpQuery(
    ui64 tabletId,
    const TVector<std::pair<TString, TString>>& keyValues);

template <typename TRequest>
void SendUndeliverableRequest(
    TTestActorRuntimeBase& runtime,
    TAutoPtr<IEventHandle>& event,
    std::unique_ptr<TRequest> request)
{
    auto fakeRecipient = TActorId(
        event->Recipient.NodeId(),
        event->Recipient.PoolID(),
        0,
        event->Recipient.Hint());
    auto undeliveryActor = event->GetForwardOnNondeliveryRecipient();
    runtime.Send(
        new IEventHandle(
            fakeRecipient,
            event->Sender,
            request.release(),
            event->Flags,
            0,
            &undeliveryActor),
        0);
}

void CheckVolumeSendsStatsEvenIfPartitionsAreDead(
    std::unique_ptr<TTestActorRuntime> runtime,
    TVolumeClient& volume,
    ui64 expectedBytesCount,
    bool isReplicatedVolume);

void CheckRebuildMetadata(ui32 partCount, ui32 blocksPerStripe);

}   // namespace NTestVolume

}   // namespace NCloud::NBlockStore::NStorage
