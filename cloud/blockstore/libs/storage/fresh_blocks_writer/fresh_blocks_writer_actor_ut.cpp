#include "cloud/blockstore/libs/storage/fresh_blocks_writer/fresh_blocks_writer_actor.h"
#include "cloud/blockstore/libs/storage/partition/part_actor.h"
#include <cloud/blockstore/libs/diagnostics/block_digest.h>
#include <cloud/blockstore/libs/diagnostics/config.h>
#include <cloud/blockstore/libs/diagnostics/critical_events.h>
#include <cloud/blockstore/libs/diagnostics/profile_log.h>
#include <cloud/blockstore/libs/kikimr/helpers.h>
#include <cloud/blockstore/libs/storage/api/partition.h>
#include <cloud/blockstore/libs/storage/api/service.h>
#include <cloud/blockstore/libs/storage/api/stats_service.h>
#include <cloud/blockstore/libs/storage/api/volume.h>
#include <cloud/blockstore/libs/storage/api/volume_proxy.h>
#include <cloud/blockstore/libs/storage/core/block_handler.h>
#include <cloud/blockstore/libs/storage/core/compaction_options.h>
#include <cloud/blockstore/libs/storage/core/config.h>
#include <cloud/blockstore/libs/storage/model/channel_data_kind.h>
#include <cloud/blockstore/libs/storage/partition/model/fresh_blob.h>
#include <cloud/blockstore/libs/storage/partition/model/fresh_blob_test.h>
#include <cloud/blockstore/libs/storage/partition/part.h>
#include <cloud/blockstore/libs/storage/partition/part_events_private.h>
#include <cloud/blockstore/libs/storage/partition_common/events_private.h>
#include <cloud/blockstore/libs/storage/testlib/test_env.h>
#include <cloud/blockstore/libs/storage/testlib/test_runtime.h>
#include <cloud/blockstore/libs/storage/testlib/ut_helpers.h>
#include <cloud/blockstore/public/api/protos/volume.pb.h>

// TODO: invalid reference
#include <cloud/blockstore/libs/storage/service/service_events_private.h>
#include <cloud/blockstore/libs/storage/volume/volume_events_private.h>

#include <cloud/storage/core/libs/api/hive_proxy.h>
#include <cloud/storage/core/libs/common/helpers.h>
#include <cloud/storage/core/libs/common/sglist_test.h>
#include <cloud/storage/core/libs/tablet/blob_id.h>

#include <contrib/ydb/core/base/blobstorage.h>
#include <contrib/ydb/core/blobstorage/vdisk/common/vdisk_events.h>
#include <contrib/ydb/core/testlib/basics/storage.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/bitmap.h>
#include <util/generic/size_literals.h>
#include <util/generic/variant.h>

namespace NCloud::NBlockStore::NStorage::NPartition {

using namespace NActors;

using namespace NKikimr;

using namespace NCloud::NStorage;

using namespace NLWTrace;

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr TDuration WaitTimeout = TDuration::Seconds(5);
constexpr ui32 DataChannelOffset = 3;
const TActorId VolumeActorId(0, "VVV");

TString GetBlockContent(char fill = 0, size_t size = DefaultBlockSize)
{
    return TString(size, fill);
}

NProto::TStorageServiceConfig DefaultConfig(ui32 flushBlobSizeThreshold = 4_KB)
{
    NProto::TStorageServiceConfig config;
    config.SetFlushBlobSizeThreshold(flushBlobSizeThreshold);
    config.SetFreshByteCountThresholdForBackpressure(400_KB);
    config.SetFreshByteCountLimitForBackpressure(1200_KB);
    config.SetFreshByteCountFeatureMaxValue(6);
    config.SetCollectGarbageThreshold(10);
    config.SetDiskPrefixLengthWithBlockChecksumsInBlobs(1_GB);
    config.SetFreshChannelWriteRequestsEnabled(true);
    config.SetFreshChannelZeroRequestsEnabled(true);

    return config;
}

TDiagnosticsConfigPtr CreateTestDiagnosticsConfig()
{
    NProto::TDiagnosticsConfig config;
    config.SetPassTraceIdToBlobstorage(true);
    return std::make_shared<TDiagnosticsConfig>(std::move(config));
}

////////////////////////////////////////////////////////////////////////////////

struct TTestPartitionInfo
{
    TString DiskId = "test";
    TString BaseDiskId;
    TString BaseDiskCheckpointId;
    ui64 TabletId = TestTabletId;
    ui64 BaseTabletId = 0;
    NCloud::NProto::EStorageMediaKind MediaKind =
        NCloud::NProto::STORAGE_MEDIA_DEFAULT;
    TMaybe<ui32> MaxBlocksInBlob;
};

////////////////////////////////////////////////////////////////////////////////

NProto::TPartitionConfig GetPartitionConfig(
    TTestPartitionInfo partitionInfo,
    ui32 blockCount,
    ui32 channelCount)
{
    NProto::TPartitionConfig partConfig;

    partConfig.SetDiskId(partitionInfo.DiskId);
    partConfig.SetBaseDiskId(partitionInfo.BaseDiskId);
    partConfig.SetBaseDiskCheckpointId(partitionInfo.BaseDiskCheckpointId);
    partConfig.SetBaseDiskTabletId(partitionInfo.BaseTabletId);
    partConfig.SetStorageMediaKind(partitionInfo.MediaKind);

    partConfig.SetBlockSize(DefaultBlockSize);
    partConfig.SetBlocksCount(blockCount);

    if (partitionInfo.MaxBlocksInBlob) {
        partConfig.SetMaxBlocksInBlob(*partitionInfo.MaxBlocksInBlob);
    }

    auto* cps = partConfig.MutableExplicitChannelProfiles();
    cps->Add()->SetDataKind(static_cast<ui32>(EChannelDataKind::System));
    cps->Add()->SetDataKind(static_cast<ui32>(EChannelDataKind::Log));
    cps->Add()->SetDataKind(static_cast<ui32>(EChannelDataKind::Index));

    for (ui32 i = 0; i < channelCount - DataChannelOffset - 1; ++i) {
        cps->Add()->SetDataKind(static_cast<ui32>(EChannelDataKind::Merged));
    }

    cps->Add()->SetDataKind(static_cast<ui32>(EChannelDataKind::Fresh));
    return partConfig;
}

////////////////////////////////////////////////////////////////////////////////

struct TConfigs {
    TStorageConfigPtr StorageConfig;
    TDiagnosticsConfigPtr DiagnosticsConfig;
};

struct TMyTestEnv
{
    std::shared_ptr<TConfigs> Configs;
    NProto::TPartitionConfig PartitionConfig;
    std::unique_ptr<TTestActorRuntime> Runtime;

    std::shared_ptr<TActorId> PartitionActorId = std::make_shared<TActorId>();

    void SetupRegisterObserver() const
    {
        Runtime->SetRegistrationObserverFunc(
            [partActorId = PartitionActorId](
                TTestActorRuntimeBase& runtime,
                const TActorId& parentId,
                const TActorId& actorId)
            {
                Y_UNUSED(parentId);
                auto actor =
                    dynamic_cast<TPartitionActor*>(runtime.FindActor(actorId));
                if (actor) {
                    Cerr << "PART_ACTOR_ID: " << actorId << Endl;
                    *partActorId = actorId;
                }
            });
    }
};

void InitLogSettings(TTestActorRuntime& runtime)
{
    for (ui32 i = TBlockStoreComponents::START; i < TBlockStoreComponents::END;
         ++i)
    {
        // runtime.SetLogPriority(i, NLog::PRI_INFO);
        runtime.SetLogPriority(i, NLog::PRI_DEBUG);
    }
    // runtime.SetLogPriority(NLog::InvalidComponent, NLog::PRI_DEBUG);
    runtime.SetLogPriority(NKikimrServices::BS_NODE, NLog::PRI_ERROR);
}

TMyTestEnv PrepareTestActorRuntime(
    NProto::TStorageServiceConfig config = DefaultConfig(),
    ui32 blockCount = 1024,
    TMaybe<ui32> channelsCount = {},
    const TTestPartitionInfo& testPartitionInfo = TTestPartitionInfo(),
    EStorageAccessMode storageAccessMode = EStorageAccessMode::Default)
{
    auto runtime = std::make_unique<TTestBasicRuntime>(1);

    runtime->AppendToLogSettings(
        TBlockStoreComponents::START,
        TBlockStoreComponents::END,
        GetComponentName);

    InitLogSettings(*runtime);

    SetupTabletServices(*runtime);

    std::unique_ptr<TTabletStorageInfo> tabletInfo(CreateTestTabletInfo(
        testPartitionInfo.TabletId,
        TTabletTypes::BlockStorePartition));

    if (channelsCount) {
        auto& channels = tabletInfo->Channels;
        channels.resize(*channelsCount);

        for (ui64 i = 0; i < channels.size(); ++i) {
            auto& channel = channels[i];
            channel.History.resize(1);
        }
    }

    auto storageConfig = std::make_shared<TStorageConfig>(
        config,
        std::make_shared<NFeatures::TFeaturesConfig>(
            NCloud::NProto::TFeaturesConfig())
    );

    auto partConfig = GetPartitionConfig(
        std::move(testPartitionInfo),
        blockCount,
        tabletInfo->Channels.size());

    auto diagConfig = CreateTestDiagnosticsConfig();

    auto configs = std::make_shared<TConfigs>(
        std::move(storageConfig),
        std::move(diagConfig));

    auto createFunc =
        [=] (const TActorId& owner, TTabletStorageInfo* info) {
            auto tablet = CreatePartitionTablet(
                owner,
                info,
                configs->StorageConfig,
                configs->DiagnosticsConfig,
                CreateProfileLogStub(),
                CreateBlockDigestGeneratorStub(),
                partConfig,
                storageAccessMode,
                0,  // partitionIndex
                1,  // siblingCount
                VolumeActorId,
                0  // volumeTabletId
            );
            return tablet.release();
        };

    auto bootstrapper = CreateTestBootstrapper(*runtime, tabletInfo.release(), createFunc);
    runtime->EnableScheduleForActor(bootstrapper);

    auto testEnv = TMyTestEnv{
        std::move(configs),
        partConfig,
        std::move(runtime),
    };

    testEnv.SetupRegisterObserver();
    return testEnv;
}

////////////////////////////////////////////////////////////////////////////////

template <typename T>
void AssertEqual(const TVector<T>& l, const TVector<T>& r)
{
    UNIT_ASSERT_VALUES_EQUAL(l.size(), r.size());
    for (size_t i = 0; i < l.size(); ++i) {
        UNIT_ASSERT_VALUES_EQUAL(l[i], r[i]);
    }
}

////////////////////////////////////////////////////////////////////////////////

class TPartitionClient
{
private:
    TTestActorRuntime& Runtime;
    ui32 NodeIdx = 0;
    ui64 TabletId;

    const TActorId Sender;
    TActorId PipeClient;

public:
    TPartitionClient(
            TTestActorRuntime& runtime,
            ui32 nodeIdx = 0,
            ui64 tabletId = TestTabletId)
        : Runtime(runtime)
        , NodeIdx(nodeIdx)
        , TabletId(tabletId)
        , Sender(runtime.AllocateEdgeActor(NodeIdx))
    {
        PipeClient = Runtime.ConnectToPipe(
            TabletId,
            Sender,
            NodeIdx,
            NKikimr::GetPipeConfigWithRetries());
    }

    void Send(TActorId actor, IEventBasePtr event, ui64 cookie = 0)
    {
        Runtime.SendAsync(
            new IEventHandle(actor, Sender, event.release(), 0, cookie));
    }

    void RebootTablet()
    {
        TVector<ui64> tablets = { TabletId };
        auto guard = CreateTabletScheduledEventsGuard(
            tablets,
            Runtime,
            Sender);

        NKikimr::RebootTablet(Runtime, TabletId, Sender);

        // sooner or later after reset pipe will reconnect
        // but we do not want to wait
        ReconnectPipe();
    }

    void ReconnectPipe()
    {
        PipeClient = Runtime.ConnectToPipe(
            TabletId,
            Sender,
            NodeIdx,
            NKikimr::GetPipeConfigWithRetries());
    }

    void KillTablet()
    {
        SendToPipe(std::make_unique<TEvents::TEvPoisonPill>());
        Runtime.DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));
    }

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

    template <typename TResponse>
    std::unique_ptr<TResponse> RecvResponse()
    {
        TAutoPtr<IEventHandle> handle;
        Runtime.GrabEdgeEventRethrow<TResponse>(handle, WaitTimeout);

        UNIT_ASSERT_C(handle, TypeName<TResponse>() << " is expected");
        return std::unique_ptr<TResponse>(handle->Release<TResponse>().Release());
    }

    std::unique_ptr<TEvPartition::TEvStatPartitionRequest> CreateStatPartitionRequest()
    {
        return std::make_unique<TEvPartition::TEvStatPartitionRequest>();
    }

    std::unique_ptr<TEvService::TEvWriteBlocksRequest> CreateWriteBlocksRequest(
        ui32 blockIndex,
        TString blockContent)
    {
        auto request = std::make_unique<TEvService::TEvWriteBlocksRequest>();
        request->Record.SetStartIndex(blockIndex);
        *request->Record.MutableBlocks()->MutableBuffers()->Add() = blockContent;
        return request;
    }

    std::unique_ptr<TEvService::TEvWriteBlocksRequest> CreateWriteBlocksRequest(
        const TBlockRange32& writeRange,
        char fill = 0)
    {
        auto blockContent = GetBlockContent(fill);

        auto request = std::make_unique<TEvService::TEvWriteBlocksRequest>();
        request->Record.SetStartIndex(writeRange.Start);

        auto& buffers = *request->Record.MutableBlocks()->MutableBuffers();
        for (ui32 i = 0; i < writeRange.Size(); ++i) {
            *buffers.Add() = blockContent;
        }

        return request;
    }

    std::unique_ptr<TEvService::TEvWriteBlocksLocalRequest> CreateWriteBlocksLocalRequest(
        const TBlockRange32& writeRange,
        TStringBuf blockContent)
    {
        TSgList sglist;
        sglist.resize(writeRange.Size(), {blockContent.data(), blockContent.size()});

        auto request = std::make_unique<TEvService::TEvWriteBlocksLocalRequest>();
        request->Record.SetStartIndex(writeRange.Start);
        request->Record.Sglist = TGuardedSgList(std::move(sglist));
        request->Record.BlocksCount = writeRange.Size();
        request->Record.BlockSize = blockContent.size() / writeRange.Size();
        return request;
    }

    std::unique_ptr<TEvService::TEvWriteBlocksRequest> CreateWriteBlocksRequest(
        ui32 blockIndex,
        char fill = 0)
    {
        return CreateWriteBlocksRequest(blockIndex, GetBlockContent(fill));
    }

    std::unique_ptr<TEvService::TEvWriteBlocksLocalRequest> CreateWriteBlocksLocalRequest(
        ui32 blockIndex,
        TStringBuf blockContent)
    {
        return CreateWriteBlocksLocalRequest(
            TBlockRange32::MakeClosedInterval(blockIndex, blockIndex),
            std::move(blockContent));
    }

    std::unique_ptr<TEvService::TEvZeroBlocksRequest> CreateZeroBlocksRequest(
        ui32 blockIndex)
    {
        auto request = std::make_unique<TEvService::TEvZeroBlocksRequest>();
        request->Record.SetStartIndex(blockIndex);
        request->Record.SetBlocksCount(1);
        return request;
    }

    std::unique_ptr<TEvService::TEvZeroBlocksRequest> CreateZeroBlocksRequest(
        const TBlockRange32& writeRange)
    {
        auto request = std::make_unique<TEvService::TEvZeroBlocksRequest>();
        request->Record.SetStartIndex(writeRange.Start);
        request->Record.SetBlocksCount(writeRange.Size());
        return request;
    }

    std::unique_ptr<TEvService::TEvReadBlocksRequest> CreateReadBlocksRequest(
        const TBlockRange32& range,
        const TString& checkpointId = {})
    {
        auto request = std::make_unique<TEvService::TEvReadBlocksRequest>();
        request->Record.SetStartIndex(range.Start);
        request->Record.SetBlocksCount(range.Size());
        request->Record.SetCheckpointId(checkpointId);
        return request;
    }

    std::unique_ptr<TEvService::TEvReadBlocksRequest> CreateReadBlocksRequest(
        ui32 blockIndex,
        const TString& checkpointId = {})
    {
        return CreateReadBlocksRequest(TBlockRange32::WithLength(blockIndex, 1), checkpointId);
    }

    std::unique_ptr<TEvService::TEvReadBlocksLocalRequest> CreateReadBlocksLocalRequest(
        ui32 blockIndex,
        TStringBuf buffer,
        const TString& checkpointId = {})
    {
        return CreateReadBlocksLocalRequest(blockIndex, TGuardedSgList{{TBlockDataRef(buffer.data(), buffer.size())}}, checkpointId);
    }

    std::unique_ptr<TEvService::TEvReadBlocksLocalRequest> CreateReadBlocksLocalRequest(
        ui32 blockIndex,
        const TGuardedSgList& sglist,
        const TString& checkpointId = {})
    {
        return CreateReadBlocksLocalRequest(
            TBlockRange32::MakeClosedInterval(blockIndex, blockIndex),
            sglist,
            checkpointId);
    }

    std::unique_ptr<TEvService::TEvReadBlocksLocalRequest> CreateReadBlocksLocalRequest(
        const TBlockRange32& readRange,
        const TSgList& sglist,
        const TString& checkpointId = {})
    {
        return CreateReadBlocksLocalRequest(readRange, TGuardedSgList(sglist), checkpointId);
    }

    std::unique_ptr<TEvService::TEvReadBlocksLocalRequest> CreateReadBlocksLocalRequest(
        const TBlockRange32& readRange,
        const TGuardedSgList& sglist,
        const TString& checkpointId = {})
    {
        auto request = std::make_unique<TEvService::TEvReadBlocksLocalRequest>();
        request->Record.SetCheckpointId(checkpointId);
        request->Record.SetStartIndex(readRange.Start);
        request->Record.SetBlocksCount(readRange.Size());

        request->Record.Sglist = sglist;
        request->Record.BlockSize = DefaultBlockSize;
        return request;
    }

    std::unique_ptr<TEvService::TEvCreateCheckpointRequest> CreateCreateCheckpointRequest(
        const TString& checkpointId)
    {
        auto request = std::make_unique<TEvService::TEvCreateCheckpointRequest>();
        request->Record.SetCheckpointId(checkpointId);
        return request;
    }

    std::unique_ptr<TEvService::TEvCreateCheckpointRequest> CreateCreateCheckpointRequest(
        const TString& checkpointId,
        const TString& idempotenceId,
        bool withoutData = false)
    {
        auto request = std::make_unique<TEvService::TEvCreateCheckpointRequest>();
        request->Record.SetCheckpointId(checkpointId);
        request->Record.SetCheckpointType(withoutData ? NProto::ECheckpointType::WITHOUT_DATA : NProto::ECheckpointType::NORMAL);
        request->Record.MutableHeaders()->SetIdempotenceId(idempotenceId);
        return request;
    }

    std::unique_ptr<TEvService::TEvDeleteCheckpointRequest> CreateDeleteCheckpointRequest(
        const TString& checkpointId)
    {
        auto request = std::make_unique<TEvService::TEvDeleteCheckpointRequest>();
        request->Record.SetCheckpointId(checkpointId);
        return request;
    }

    std::unique_ptr<TEvService::TEvDeleteCheckpointRequest> CreateDeleteCheckpointRequest(
        const TString& checkpointId,
        const TString& idempotenceId)
    {
        auto request = std::make_unique<TEvService::TEvDeleteCheckpointRequest>();
        request->Record.SetCheckpointId(checkpointId);
        request->Record.MutableHeaders()->SetIdempotenceId(idempotenceId);
        return request;
    }

    std::unique_ptr<TEvVolume::TEvDeleteCheckpointDataRequest> CreateDeleteCheckpointDataRequest(
        const TString& checkpointId)
    {
        auto request = std::make_unique<TEvVolume::TEvDeleteCheckpointDataRequest>();
        request->Record.SetCheckpointId(checkpointId);
        return request;
    }

    std::unique_ptr<TEvService::TEvGetChangedBlocksRequest> CreateGetChangedBlocksRequest(
        const TBlockRange32& range,
        const TString& lowCheckpointId,
        const TString& highCheckpointId,
        const bool ignoreBaseDisk)
    {
        auto request = std::make_unique<TEvService::TEvGetChangedBlocksRequest>();
        request->Record.SetStartIndex(range.Start);
        request->Record.SetBlocksCount(range.Size());
        request->Record.SetLowCheckpointId(lowCheckpointId);
        request->Record.SetHighCheckpointId(highCheckpointId);
        request->Record.SetIgnoreBaseDisk(ignoreBaseDisk);
        return request;
    }

    std::unique_ptr<TEvPartition::TEvWaitReadyRequest> CreateWaitReadyRequest()
    {
        return std::make_unique<TEvPartition::TEvWaitReadyRequest>();
    }

    std::unique_ptr<TEvPartitionPrivate::TEvFlushRequest> CreateFlushRequest()
    {
        return std::make_unique<TEvPartitionPrivate::TEvFlushRequest>();
    }

    std::unique_ptr<TEvPartitionCommonPrivate::TEvTrimFreshLogRequest> CreateTrimFreshLogRequest()
    {
        return std::make_unique<TEvPartitionCommonPrivate::TEvTrimFreshLogRequest>();
    }

    template <typename... TArgs>
    std::unique_ptr<TEvPartitionPrivate::TEvCompactionRequest> CreateCompactionRequest(TArgs&&... args)
    {
        return std::make_unique<TEvPartitionPrivate::TEvCompactionRequest>(std::forward<TArgs>(args)...);
    }

    std::unique_ptr<TEvPartitionPrivate::TEvMetadataRebuildBlockCountRequest> CreateMetadataRebuildBlockCountRequest(
        TPartialBlobId blobId,
        ui32 count,
        TPartialBlobId lastBlobId)
    {
        return std::make_unique<TEvPartitionPrivate::TEvMetadataRebuildBlockCountRequest>(
            blobId,
            count,
            lastBlobId,
            TBlockCountRebuildState());
    }

    std::unique_ptr<TEvPartitionPrivate::TEvScanDiskBatchRequest> CreateScanDiskBatchRequest(
        TPartialBlobId blobId,
        ui32 count,
        TPartialBlobId lastBlobId)
    {
        return std::make_unique<TEvPartitionPrivate::TEvScanDiskBatchRequest>(
            blobId,
            count,
            lastBlobId);
    }

    std::unique_ptr<TEvPartitionPrivate::TEvCleanupRequest> CreateCleanupRequest()
    {
        return std::make_unique<TEvPartitionPrivate::TEvCleanupRequest>();
    }

    std::unique_ptr<TEvTablet::TEvGetCounters> CreateGetCountersRequest()
    {
        auto request = std::make_unique<TEvTablet::TEvGetCounters>();
        return request;
    }

    void SendGetCountersRequest()
    {
        auto request = CreateGetCountersRequest();
        SendToPipe(std::move(request));
    }

    std::unique_ptr<TEvTablet::TEvGetCountersResponse> RecvGetCountersResponse()
    {
        return RecvResponse<TEvTablet::TEvGetCountersResponse>();
    }

    std::unique_ptr<TEvTablet::TEvGetCountersResponse> GetCounters()
    {
        auto request = CreateGetCountersRequest();
        SendToPipe(std::move(request));

        auto response = RecvResponse<TEvTablet::TEvGetCountersResponse>();
        return response;
    }

    std::unique_ptr<TEvPartitionPrivate::TEvCollectGarbageRequest> CreateCollectGarbageRequest()
    {
        return std::make_unique<TEvPartitionPrivate::TEvCollectGarbageRequest>();
    }

    std::unique_ptr<TEvVolume::TEvDescribeBlocksRequest> CreateDescribeBlocksRequest(
        ui32 startIndex,
        ui32 blockCount,
        const TString& checkpointId = "")
    {
        auto request = std::make_unique<TEvVolume::TEvDescribeBlocksRequest>();
        request->Record.SetStartIndex(startIndex);
        request->Record.SetBlocksCount(blockCount);
        request->Record.SetCheckpointId(checkpointId);
        return request;
    }

    std::unique_ptr<TEvVolume::TEvDescribeBlocksRequest> CreateDescribeBlocksRequest(
        const TBlockRange32& range, const TString& checkpointId = "")
    {
        return CreateDescribeBlocksRequest(range.Start, range.Size(), checkpointId);
    }

    std::unique_ptr<TEvVolume::TEvGetUsedBlocksRequest> CreateGetUsedBlocksRequest()
    {
        return std::make_unique<TEvVolume::TEvGetUsedBlocksRequest>();
    }

    std::unique_ptr<TEvPartitionCommonPrivate::TEvReadBlobRequest> CreateReadBlobRequest(
        const NKikimr::TLogoBlobID& blobId,
        const ui32 bSGroupId,
        const TVector<ui16>& blobOffsets,
        TSgList sglist)
    {
        auto request =
            std::make_unique<TEvPartitionCommonPrivate::TEvReadBlobRequest>(
                blobId,
                MakeBlobStorageProxyID(bSGroupId),
                blobOffsets,
                TGuardedSgList(std::move(sglist)),
                bSGroupId,
                false,           // async
                TInstant::Max(), // deadline
                false            // shouldCalculateChecksums
            );
        return request;
    }

    std::unique_ptr<TEvVolume::TEvCompactRangeRequest> CreateCompactRangeRequest(
        ui32 blockIndex,
        ui32 blockCount)
    {
        auto request = std::make_unique<TEvVolume::TEvCompactRangeRequest>();
        request->Record.SetStartIndex(blockIndex);
        request->Record.SetBlocksCount(blockCount);
        return request;
    }

    std::unique_ptr<TEvVolume::TEvGetCompactionStatusRequest> CreateGetCompactionStatusRequest(
        const TString& operationId)
    {
        auto request = std::make_unique<TEvVolume::TEvGetCompactionStatusRequest>();
        request->Record.SetOperationId(operationId);
        return request;
    }

    std::unique_ptr<TEvPartition::TEvDrainRequest> CreateDrainRequest()
    {
        return std::make_unique<TEvPartition::TEvDrainRequest>();
    }

    std::unique_ptr<NMon::TEvRemoteHttpInfo> CreateRemoteHttpInfo(
        const TString& params,
        HTTP_METHOD method)
    {
        return std::make_unique<NMon::TEvRemoteHttpInfo>(params, method);
    }

    std::unique_ptr<NMon::TEvRemoteHttpInfo> CreateRemoteHttpInfo(
        const TString& params)
    {
        return std::make_unique<NMon::TEvRemoteHttpInfo>(params);
    }

    void SendRemoteHttpInfo(
        const TString& params,
        HTTP_METHOD method)
    {
        auto request = CreateRemoteHttpInfo(params, method);
        SendToPipe(std::move(request));
    }

    void SendRemoteHttpInfo(
        const TString& params)
    {
        auto request = CreateRemoteHttpInfo(params);
        SendToPipe(std::move(request));
    }

    std::unique_ptr<NMon::TEvRemoteHttpInfoRes> RecvCreateRemoteHttpInfoRes()
    {
        return RecvResponse<NMon::TEvRemoteHttpInfoRes>();
    }

    std::unique_ptr<NMon::TEvRemoteHttpInfoRes> RemoteHttpInfo(
        const TString& params,
        HTTP_METHOD method)
    {
        auto request = CreateRemoteHttpInfo(params, method);
        SendToPipe(std::move(request));

        auto response = RecvResponse<NMon::TEvRemoteHttpInfoRes>();
        return response;
    }

    std::unique_ptr<NMon::TEvRemoteHttpInfoRes> RemoteHttpInfo(
        const TString& params)
    {
        return RemoteHttpInfo(params, HTTP_METHOD::HTTP_METHOD_GET);
    }

    std::unique_ptr<TEvVolume::TEvRebuildMetadataRequest> CreateRebuildMetadataRequest(
        NProto::ERebuildMetadataType type,
        ui32 batchSize)
    {
        auto request = std::make_unique<TEvVolume::TEvRebuildMetadataRequest>();
        request->Record.SetMetadataType(type);
        request->Record.SetBatchSize(batchSize);
        return request;
    }

    std::unique_ptr<TEvVolume::TEvGetRebuildMetadataStatusRequest> CreateGetRebuildMetadataStatusRequest()
    {
        auto request = std::make_unique<TEvVolume::TEvGetRebuildMetadataStatusRequest>();
        return request;
    }

    std::unique_ptr<TEvVolume::TEvScanDiskRequest> CreateScanDiskRequest(ui32 blobsPerBatch)
    {
        auto request = std::make_unique<TEvVolume::TEvScanDiskRequest>();
        request->Record.SetBatchSize(blobsPerBatch);
        return request;
    }

    std::unique_ptr<TEvVolume::TEvGetScanDiskStatusRequest> CreateGetScanDiskStatusRequest()
    {
        return std::make_unique<TEvVolume::TEvGetScanDiskStatusRequest>();
    }

    std::unique_ptr<TEvVolume::TEvCheckRangeRequest>
    CreateCheckRangeRequest(TString id, ui32 startIndex, ui32 size)
    {
        auto request = std::make_unique<TEvVolume::TEvCheckRangeRequest>();
        request->Record.SetDiskId(id);
        request->Record.SetStartIndex(startIndex);
        request->Record.SetBlocksCount(size);
        return request;
    }

    auto CreateGetPartitionInfoRequest()
    {
        auto request =
            std::make_unique<TEvVolume::TEvGetPartitionInfoRequest>();
        return request;
    }

#define BLOCKSTORE_DECLARE_METHOD(name, ns)                                    \
    template <typename... Args>                                                \
    void Send##name##Request(Args&&... args)                                   \
    {                                                                          \
        auto request = Create##name##Request(std::forward<Args>(args)...);     \
        SendToPipe(std::move(request));                                        \
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
        SendToPipe(std::move(request));                                        \
                                                                               \
        auto response = RecvResponse<ns::TEv##name##Response>();               \
        UNIT_ASSERT_C(                                                         \
            SUCCEEDED(response->GetStatus()),                                  \
            response->GetErrorReason());                                       \
        return response;                                                       \
    }                                                                          \
// BLOCKSTORE_DECLARE_METHOD

    BLOCKSTORE_PARTITION_REQUESTS(BLOCKSTORE_DECLARE_METHOD, TEvPartition)
    BLOCKSTORE_PARTITION_REQUESTS_PRIVATE(BLOCKSTORE_DECLARE_METHOD, TEvPartitionPrivate)
    BLOCKSTORE_PARTITION_COMMON_REQUESTS_PRIVATE(BLOCKSTORE_DECLARE_METHOD, TEvPartitionCommonPrivate)
    BLOCKSTORE_PARTITION_REQUESTS_FWD_SERVICE(BLOCKSTORE_DECLARE_METHOD, TEvService)
    BLOCKSTORE_PARTITION_REQUESTS_FWD_VOLUME(BLOCKSTORE_DECLARE_METHOD, TEvVolume)

#undef BLOCKSTORE_DECLARE_METHOD
};

class TFreshBlocksWriterClient
{
private:
    TTestActorRuntime& Runtime;
    ui32 NodeIdx = 0;
    TActorId FreshBlocksWriterActor;

    const TActorId Sender;

public:
    TFreshBlocksWriterClient(
            TTestActorRuntime& runtime,
            TActorId freshBlocksWriterActor,
            ui32 nodeIdx = 0)
        : Runtime(runtime)
        , NodeIdx(nodeIdx)
        , FreshBlocksWriterActor(freshBlocksWriterActor)
        , Sender(runtime.AllocateEdgeActor(NodeIdx))
    {
    }

    void SetFreshBlocksWriterActor(TActorId freshBlocksWriterActor)
    {
        FreshBlocksWriterActor = freshBlocksWriterActor;
    }

    void Send(IEventBasePtr event, ui64 cookie = 0)
    {
        Runtime.SendAsync(new IEventHandle(
            FreshBlocksWriterActor,
            Sender,
            event.release(),
            0,
            cookie));
    }

    template <typename TResponse>
    std::unique_ptr<TResponse> RecvResponse()
    {
        TAutoPtr<IEventHandle> handle;
        Runtime.GrabEdgeEventRethrow<TResponse>(handle, WaitTimeout);

        UNIT_ASSERT_C(handle, TypeName<TResponse>() << " is expected");
        return std::unique_ptr<TResponse>(handle->Release<TResponse>().Release());
    }

    auto CreateWaitReadyRequest()
    {
        return std::make_unique<
            NFreshBlocksWriter::TEvFreshBlocksWriter::TEvWaitReadyRequest>();
    }

    auto CreateReadFreshBlocksRequest(TBlockRange32 blockRange, ui64 commitId = Max<ui64>())
    {
        auto req = std::make_unique<NFreshBlocksWriter::TEvFreshBlocksWriter::
                                        TEvReadFreshBlocksRequest>();
        req->Record.SetStartIndex(blockRange.Start);
        req->Record.SetBlocksCount(blockRange.Size());

        req->Record.CommitId = commitId;

        return std::move(req);
    }

    auto CreateReadFreshBlocksRequest(
        ui64 blockIndex,
        ui64 commitId = Max<ui64>())
    {
        return CreateReadFreshBlocksRequest(
            TBlockRange32::WithLength(blockIndex, 1),
            commitId);
    }

#define BLOCKSTORE_DECLARE_METHOD(name, ns)                                    \
    template <typename... Args>                                                \
    void Send##name##Request(Args&&... args)                                   \
    {                                                                          \
        auto request = Create##name##Request(std::forward<Args>(args)...);     \
        Send(std::move(request));                                              \
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
        Send(std::move(request));                                              \
                                                                               \
        auto response = RecvResponse<ns::TEv##name##Response>();               \
        UNIT_ASSERT_C(                                                         \
            SUCCEEDED(response->GetStatus()),                                  \
            response->GetErrorReason());                                       \
        return response;                                                       \
    }                                                                          \
// BLOCKSTORE_DECLARE_METHOD

    BLOCKSTORE_FRESH_BLOCKS_WRITER_REQUESTS(BLOCKSTORE_DECLARE_METHOD, NFreshBlocksWriter::TEvFreshBlocksWriter);

#undef BLOCKSTORE_DECLARE_METHOD
};


////////////////////////////////////////////////////////////////////////////////

TString GetBlockContent(
    const TEvService::TEvReadBlocksResponse& response)
{
    if (response.Record.GetBlocks().BuffersSize() == 1) {
        return response.Record.GetBlocks().GetBuffers(0);
    }
    return {};
}

TString GetBlocksContent(
    const TEvService::TEvReadBlocksResponse& response)
{
    const auto& blocks = response.Record.GetBlocks();

    {
        bool empty = true;
        for (size_t i = 0; i < blocks.BuffersSize(); ++i) {
            if (blocks.GetBuffers(i)) {
                empty = false;
            }
        }

        if (empty) {
            return TString();
        }
    }

    TString result;

    for (size_t i = 0; i < blocks.BuffersSize(); ++i) {
        const auto& block = blocks.GetBuffers(i);
        if (!block) {
            result += GetBlockContent(char(0));
            continue;
        }
        result += block;
    }

    return result;
}

// TString GetBlockContent(
//     const std::unique_ptr<TEvService::TEvReadBlocksResponse>& response)
// {
//     return GetBlockContent(*response);
// }

TString GetBlocksContent(
    const std::unique_ptr<TEvService::TEvReadBlocksResponse>& response)
{
    return GetBlocksContent(*response);
}

TString GetBlockContent(
    const std::unique_ptr<
        NFreshBlocksWriter::TEvFreshBlocksWriter::TEvReadFreshBlocksResponse>&
        response)
{
    return GetBlockContent(response->Record);
}

TString GetBlocksContent(
    const std::unique_ptr<
        NFreshBlocksWriter::TEvFreshBlocksWriter::TEvReadFreshBlocksResponse>&
        response)
{
    return GetBlocksContent(response->Record);
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TFreshBlocksWriterTest)
{
    Y_UNIT_TEST(ShouldWaitReady)
    {
        auto config = DefaultConfig();
        config.SetFreshBlocksWriterEnabled(true);

        auto testEnv = PrepareTestActorRuntime(config);

        TPartitionClient partition(*testEnv.Runtime);
        partition.WaitReady();

        auto freshBlocksWriter = testEnv.Runtime->Register(
            std::make_unique<NFreshBlocksWriter::TFreshBlocksWriterActor>(
                testEnv.Configs->StorageConfig,
                testEnv.PartitionConfig,
                EStorageAccessMode::Default,
                0,
                1,
                *testEnv.PartitionActorId,
                TestTabletId)
                .release());

        TFreshBlocksWriterClient fbwClient(*testEnv.Runtime, freshBlocksWriter);

        fbwClient.WaitReady();
    }

    Y_UNIT_TEST(ShouldLoadFreshBlobsFromPartition)
    {
        auto config = DefaultConfig(4_MB);
        config.SetFreshBlocksWriterEnabled(false);

        auto testEnv = PrepareTestActorRuntime(config);

        TPartitionClient partition(*testEnv.Runtime);
        partition.WaitReady();

        partition.WriteBlocks(0, 1);
        partition.WriteBlocks(2, 3);
        partition.WriteBlocks(TBlockRange32::WithLength(3, 2), 45);

        config.SetFreshBlocksWriterEnabled(true);
        testEnv.Configs->StorageConfig = std::make_shared<TStorageConfig>(
            config,
            std::make_shared<NFeatures::TFeaturesConfig>(
                NCloud::NProto::TFeaturesConfig()));

        auto partitionResponse =
            partition.ReadBlocks(TBlockRange32::WithLength(0, 5));

        partition.KillTablet();
        partition.ReconnectPipe();
        partition.WaitReady();

        auto freshBlocksWriter = testEnv.Runtime->Register(
            std::make_unique<NFreshBlocksWriter::TFreshBlocksWriterActor>(
                testEnv.Configs->StorageConfig,
                testEnv.PartitionConfig,
                EStorageAccessMode::Default,
                0,
                1,
                *testEnv.PartitionActorId,
                TestTabletId)
                .release());
        TFreshBlocksWriterClient fbwClient(*testEnv.Runtime, freshBlocksWriter);

        fbwClient.WaitReady();

        auto response =
            fbwClient.ReadFreshBlocks(TBlockRange32::WithLength(0, 5));

        UNIT_ASSERT_VALUES_EQUAL(
            GetBlocksContent(partitionResponse),
            GetBlocksContent(response));
    }

    Y_UNIT_TEST(ShouldHandleBSErrorsOnInitFreshBlocksFromChannel)
    {
        auto config = DefaultConfig();
        config.SetFreshBlocksWriterEnabled(false);

        auto testEnv = PrepareTestActorRuntime(config);

        TPartitionClient partition(*testEnv.Runtime);
        partition.WaitReady();

        partition.WriteBlocks(TBlockRange32::MakeOneBlock(0), 1);
        partition.WriteBlocks(TBlockRange32::MakeOneBlock(1), 2);
        partition.WriteBlocks(TBlockRange32::MakeOneBlock(2), 3);

        bool evRangeResultSeen = false;

        ui32 evLoadFreshBlobsCompletedCount = 0;

        THashSet<TActorId> deadActors;

        testEnv.Runtime->SetEventFilter(
            [&](TTestActorRuntimeBase& runtime, TAutoPtr<IEventHandle>& event)
            {
                switch (event->GetTypeRewrite()) {
                    case TEvBlobStorage::EvRangeResult: {
                        using TEvent = TEvBlobStorage::TEvRangeResult;
                        auto* msg = event->Get<TEvent>();

                        if (msg->From.Channel() != 4 || evRangeResultSeen) {
                            return false;
                        }

                        evRangeResultSeen = true;

                        auto response = std::make_unique<TEvent>(
                            NKikimrProto::ERROR,
                            TLogoBlobID(),  // doesn't matter
                            TLogoBlobID(),  // doesn't matter
                            0);             // doesn't matter

                        auto* handle = new IEventHandle(
                            event->Recipient,
                            event->Sender,
                            response.release(),
                            0,
                            event->Cookie);

                        runtime.Send(handle, 0);

                        return true;
                    }
                    case TEvPartitionCommonPrivate::EvLoadFreshBlobsCompleted: {
                        ++evLoadFreshBlobsCompletedCount;
                        return false;
                    }
                    case TEvents::TEvPoisonTaken::EventType: {
                        deadActors.emplace(event->Sender);
                        return false;
                    }
                    default: {
                        return false;
                    }
                }
            });

        config.SetFreshBlocksWriterEnabled(true);
        testEnv.Configs->StorageConfig = std::make_shared<TStorageConfig>(
            config,
            std::make_shared<NFeatures::TFeaturesConfig>(
                NCloud::NProto::TFeaturesConfig()));

        auto partitionResponse =
            partition.ReadBlocks(TBlockRange32::WithLength(0, 5));

        partition.KillTablet();
        partition.ReconnectPipe();
        partition.WaitReady();

        auto firstFreshBlocksWriter = testEnv.Runtime->Register(
            std::make_unique<NFreshBlocksWriter::TFreshBlocksWriterActor>(
                testEnv.Configs->StorageConfig,
                testEnv.PartitionConfig,
                EStorageAccessMode::Default,
                0,
                1,
                *testEnv.PartitionActorId,
                TestTabletId)
                .release());

        testEnv.Runtime->DispatchEvents({}, TDuration::MilliSeconds(10));

        UNIT_ASSERT(deadActors.contains(firstFreshBlocksWriter));

        auto freshBlocksWriter = testEnv.Runtime->Register(
            std::make_unique<NFreshBlocksWriter::TFreshBlocksWriterActor>(
                testEnv.Configs->StorageConfig,
                testEnv.PartitionConfig,
                EStorageAccessMode::Default,
                0,
                1,
                *testEnv.PartitionActorId,
                TestTabletId)
                .release());

        TFreshBlocksWriterClient fbwClient(*testEnv.Runtime, freshBlocksWriter);

        fbwClient.WaitReady();

        UNIT_ASSERT_VALUES_EQUAL(true, evRangeResultSeen);

        // tablet rebooted twice (after explicit RebootTablet() and on fail)
        UNIT_ASSERT_VALUES_EQUAL(2, evLoadFreshBlobsCompletedCount);

        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(1),
            GetBlockContent(fbwClient.ReadFreshBlocks(0)));
        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(2),
            GetBlockContent(fbwClient.ReadFreshBlocks(1)));
        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(3),
            GetBlockContent(fbwClient.ReadFreshBlocks(2)));
    }

    Y_UNIT_TEST(ShouldHandleCorruptedFreshBlobOnInitFreshBlocks)
    {
        auto config = DefaultConfig();
        config.SetFreshBlocksWriterEnabled(false);

        auto testEnv = PrepareTestActorRuntime(config);

        TPartitionClient partition(*testEnv.Runtime);
        partition.WaitReady();

        partition.WriteBlocks(TBlockRange32::MakeOneBlock(0), 1);
        partition.WriteBlocks(TBlockRange32::MakeOneBlock(1), 2);
        partition.WriteBlocks(TBlockRange32::MakeOneBlock(2), 3);

        bool evRangeResultSeen = false;

        ui32 evLoadFreshBlobsCompletedCount = 0;

        THashSet<TActorId> deadActors;

        testEnv.Runtime->SetEventFilter(
            [&] (TTestActorRuntimeBase& runtime, TAutoPtr<IEventHandle>& event)
            {
                switch (event->GetTypeRewrite()) {
                    case TEvBlobStorage::EvRangeResult: {
                        using TEvent = TEvBlobStorage::TEvRangeResult;
                        auto* msg = event->Get<TEvent>();

                        if (msg->From.Channel() != 4 || evRangeResultSeen) {
                            return false;
                        }

                        evRangeResultSeen = true;

                        auto response = std::make_unique<TEvent>(
                            msg->Status,
                            msg->From,
                            msg->To,
                            msg->GroupId);

                        response->Responses = std::move(msg->Responses);

                        // corrupt
                        auto& buffer = response->Responses[0].Buffer;
                        std::memset(buffer.Detach(), 0, 4);

                        auto* handle = new IEventHandle(
                            event->Recipient,
                            event->Sender,
                            response.release(),
                            0,
                            event->Cookie);

                        runtime.Send(handle, 0);

                        return true;
                    }
                    case TEvPartitionCommonPrivate::EvLoadFreshBlobsCompleted: {
                        ++evLoadFreshBlobsCompletedCount;
                        return false;
                    }
                    case TEvents::TEvPoisonTaken::EventType: {
                        deadActors.emplace(event->Sender);
                        return false;
                    }
                    default: {
                        return false;
                    }
                }
            }
        );

        config.SetFreshBlocksWriterEnabled(true);
        testEnv.Configs->StorageConfig = std::make_shared<TStorageConfig>(
            config,
            std::make_shared<NFeatures::TFeaturesConfig>(
                NCloud::NProto::TFeaturesConfig()));

        auto partitionResponse =
            partition.ReadBlocks(TBlockRange32::WithLength(0, 5));

        partition.KillTablet();
        partition.ReconnectPipe();
        partition.WaitReady();

        auto firstFreshBlocksWriter = testEnv.Runtime->Register(
            std::make_unique<NFreshBlocksWriter::TFreshBlocksWriterActor>(
                testEnv.Configs->StorageConfig,
                testEnv.PartitionConfig,
                EStorageAccessMode::Default,
                0,
                1,
                *testEnv.PartitionActorId,
                TestTabletId)
                .release());

        testEnv.Runtime->DispatchEvents({}, TDuration::MilliSeconds(10));

        UNIT_ASSERT(deadActors.contains(firstFreshBlocksWriter));

        auto freshBlocksWriter = testEnv.Runtime->Register(
            std::make_unique<NFreshBlocksWriter::TFreshBlocksWriterActor>(
                testEnv.Configs->StorageConfig,
                testEnv.PartitionConfig,
                EStorageAccessMode::Default,
                0,
                1,
                *testEnv.PartitionActorId,
                TestTabletId)
                .release());

        TFreshBlocksWriterClient fbwClient(*testEnv.Runtime, freshBlocksWriter);
        fbwClient.WaitReady();

        UNIT_ASSERT_VALUES_EQUAL(true, evRangeResultSeen);

        // tablet rebooted twice (after explicit RebootTablet() and on fail)
        UNIT_ASSERT_VALUES_EQUAL(2, evLoadFreshBlobsCompletedCount);

        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(1),
            GetBlockContent(fbwClient.ReadFreshBlocks(0)));
        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(2),
            GetBlockContent(fbwClient.ReadFreshBlocks(1)));
        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(3),
            GetBlockContent(fbwClient.ReadFreshBlocks(2)));
    }
}

}   // namespace NCloud::NBlockStore::NStorage::NPartition
