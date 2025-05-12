#include "part.h"

#include "part_events_private.h"

#include <cloud/blockstore/public/api/protos/volume.pb.h>

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
#include <cloud/blockstore/libs/storage/partition/model/fresh_blob_test.h>
#include <cloud/blockstore/libs/storage/partition/model/fresh_blob.h>
#include <cloud/blockstore/libs/storage/partition/part.h>
#include <cloud/blockstore/libs/storage/partition/part_events_private.h>
#include <cloud/blockstore/libs/storage/partition_common/events_private.h>
#include <cloud/blockstore/libs/storage/testlib/test_env.h>
#include <cloud/blockstore/libs/storage/testlib/test_runtime.h>
#include <cloud/blockstore/libs/storage/testlib/ut_helpers.h>

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

namespace {

////////////////////////////////////////////////////////////////////////////////

#define BLOCKSTORE_PARTITION_REQUESTS_MON(xxx, ...)                            \
    xxx(RemoteHttpInfo,         __VA_ARGS__)                                   \
// BLOCKSTORE_PARTITION_REQUESTS_MON

////////////////////////////////////////////////////////////////////////////////

constexpr TDuration WaitTimeout = TDuration::Seconds(5);
constexpr ui32 DataChannelOffset = 3;
const TActorId VolumeActorId(0, "VVV");

TString GetBlockContent(char fill = 0, size_t size = DefaultBlockSize)
{
    return TString(size, fill);
}

TString GetBlocksContent(
    char fill = 0,
    ui32 blockCount = 1,
    size_t blockSize = DefaultBlockSize)
{
    TString result;
    for (ui32 i = 0; i < blockCount; ++i) {
        result += GetBlockContent(fill, blockSize);
    }
    return result;
}

void CheckRangesArePartition(
    TVector<TBlockRange32> ranges,
    const TBlockRange32& unionRange)
{
    UNIT_ASSERT(ranges.size() > 0);

    Sort(ranges, [](const auto& l, const auto& r) {
            return l.Start < r.Start;
        });

    const auto* prev = &ranges.front();
    UNIT_ASSERT_VALUES_EQUAL(unionRange.Start, prev->Start);

    for (size_t i = 1; i < ranges.size(); ++i) {
        UNIT_ASSERT_VALUES_EQUAL_C(prev->End + 1, ranges[i].Start,
            "during iteration #" << i);
        prev = &ranges[i];
    }

    UNIT_ASSERT_VALUES_EQUAL(unionRange.End, ranges.back().End);
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

    return config;
}

TDiagnosticsConfigPtr CreateTestDiagnosticsConfig()
{
    return std::make_shared<TDiagnosticsConfig>(NProto::TDiagnosticsConfig());
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
};

////////////////////////////////////////////////////////////////////////////////

class TDummyActor final
    : public TActor<TDummyActor>
{
public:
    TDummyActor()
        : TActor(&TThis::StateWork)
    {
    }

private:
    STFUNC(StateWork)
    {
        Y_UNUSED(ev);
    }
};

////////////////////////////////////////////////////////////////////////////////

void InitTestActorRuntime(
    TTestActorRuntime& runtime,
    const NProto::TStorageServiceConfig& config,
    ui32 blockCount,
    ui32 channelCount,
    std::unique_ptr<TTabletStorageInfo> tabletInfo,
    TTestPartitionInfo partitionInfo = TTestPartitionInfo(),
    EStorageAccessMode storageAccessMode = EStorageAccessMode::Default)
{
    auto storageConfig = std::make_shared<TStorageConfig>(
        config,
        std::make_shared<NFeatures::TFeaturesConfig>(
            NCloud::NProto::TFeaturesConfig())
    );

    NProto::TPartitionConfig partConfig;

    partConfig.SetDiskId(partitionInfo.DiskId);
    partConfig.SetBaseDiskId(partitionInfo.BaseDiskId);
    partConfig.SetBaseDiskCheckpointId(partitionInfo.BaseDiskCheckpointId);
    partConfig.SetBaseDiskTabletId(partitionInfo.BaseTabletId);
    partConfig.SetStorageMediaKind(partitionInfo.MediaKind);

    partConfig.SetBlockSize(DefaultBlockSize);
    partConfig.SetBlocksCount(blockCount);

    auto* cps = partConfig.MutableExplicitChannelProfiles();
    cps->Add()->SetDataKind(static_cast<ui32>(EChannelDataKind::System));
    cps->Add()->SetDataKind(static_cast<ui32>(EChannelDataKind::Log));
    cps->Add()->SetDataKind(static_cast<ui32>(EChannelDataKind::Index));

    for (ui32 i = 0; i < channelCount - DataChannelOffset - 1; ++i) {
        cps->Add()->SetDataKind(static_cast<ui32>(EChannelDataKind::Merged));
    }

    cps->Add()->SetDataKind(static_cast<ui32>(EChannelDataKind::Fresh));

    auto diagConfig = CreateTestDiagnosticsConfig();

    auto createFunc =
        [=] (const TActorId& owner, TTabletStorageInfo* info) {
            auto tablet = CreatePartitionTablet(
                owner,
                info,
                storageConfig,
                diagConfig,
                CreateProfileLogStub(),
                CreateBlockDigestGeneratorStub(),
                partConfig,
                storageAccessMode,
                1,  // siblingCount
                VolumeActorId
            );
            return tablet.release();
        };

    auto bootstrapper = CreateTestBootstrapper(runtime, tabletInfo.release(), createFunc);
    runtime.EnableScheduleForActor(bootstrapper);
}

////////////////////////////////////////////////////////////////////////////////

auto InitTestActorRuntime(
    TTestEnv& env,
    TTestActorRuntime& runtime,
    ui32 channelCount,
    ui32 tabletInfoChannelCount,  // usually should be equal to channelCount
    const NProto::TStorageServiceConfig& config = DefaultConfig())
{
    {
        env.CreateSubDomain("nbs");
        auto storageConfig = CreateTestStorageConfig(config);
        env.CreateBlockStoreNode(
            "nbs",
            storageConfig,
            CreateTestDiagnosticsConfig()
        );
    }

    const auto tabletId = NKikimr::MakeTabletID(1, HiveId, 1);
    std::unique_ptr<TTabletStorageInfo> x(new TTabletStorageInfo());

    x->TabletID = tabletId;
    x->TabletType = TTabletTypes::BlockStorePartition;
    auto& channels = x->Channels;
    channels.resize(tabletInfoChannelCount);

    for (ui64 channel = 0; channel < channels.size(); ++channel) {
        channels[channel].Channel = channel;
        channels[channel].Type =
            TBlobStorageGroupType(BootGroupErasure);
        channels[channel].History.resize(1);
        channels[channel].History[0].FromGeneration = 0;
        const auto gidx =
            channel > DataChannelOffset ? channel - DataChannelOffset : 0;
        channels[channel].History[0].GroupID = env.GetGroupIds()[gidx];
    }

    InitTestActorRuntime(runtime, config, 1024, channelCount, std::move(x));

    return tabletId;
}

////////////////////////////////////////////////////////////////////////////////

void InitLogSettings(TTestActorRuntime& runtime)
{
    for (ui32 i = TBlockStoreComponents::START; i < TBlockStoreComponents::END; ++i) {
        runtime.SetLogPriority(i, NLog::PRI_INFO);
        // runtime.SetLogPriority(i, NLog::PRI_DEBUG);
    }
    // runtime.SetLogPriority(NLog::InvalidComponent, NLog::PRI_DEBUG);
    runtime.SetLogPriority(NKikimrServices::BS_NODE, NLog::PRI_ERROR);
}

std::unique_ptr<TTestActorRuntime> PrepareTestActorRuntime(
    NProto::TStorageServiceConfig config = DefaultConfig(),
    ui32 blockCount = 1024,
    TMaybe<ui32> channelsCount = {},
    const TTestPartitionInfo& testPartitionInfo = TTestPartitionInfo(),
    IActorPtr volumeProxy = {},
    EStorageAccessMode storageAccessMode = EStorageAccessMode::Default)
{
    auto runtime = std::make_unique<TTestBasicRuntime>(1);

    if (volumeProxy) {
        runtime->AddLocalService(
            MakeVolumeProxyServiceId(),
            TActorSetupCmd(volumeProxy.release(), TMailboxType::Simple, 0));
    }

    runtime->AddLocalService(
        VolumeActorId,
        TActorSetupCmd(new TDummyActor, TMailboxType::Simple, 0));

    runtime->AddLocalService(
        MakeHiveProxyServiceId(),
        TActorSetupCmd(new TDummyActor, TMailboxType::Simple, 0));

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

    InitTestActorRuntime(
        *runtime,
        config,
        blockCount,
        channelsCount ? *channelsCount : tabletInfo->Channels.size(),
        std::move(tabletInfo),
        testPartitionInfo,
        storageAccessMode
    );

    return runtime;
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

TString GetBlockContent(
    const std::unique_ptr<TEvService::TEvReadBlocksResponse>& response)
{
    if (response->Record.GetBlocks().BuffersSize() == 1) {
        return response->Record.GetBlocks().GetBuffers(0);
    }
    return {};
}

TString GetBlocksContent(
    const std::unique_ptr<TEvService::TEvReadBlocksResponse>& response)
{
    const auto& blocks = response->Record.GetBlocks();

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

////////////////////////////////////////////////////////////////////////////////

TDynBitMap GetBitMap(const TString& s)
{
    TDynBitMap mask;

    if (s) {
        mask.Reserve(s.size() * 8);
        Y_ABORT_UNLESS(mask.GetChunkCount() * sizeof(TDynBitMap::TChunk) == s.size());
        auto* dst = const_cast<TDynBitMap::TChunk*>(mask.GetChunks());
        memcpy(dst, s.data(), s.size());
    }

    return mask;
}

TDynBitMap GetUnencryptedBlockMask(
    const std::unique_ptr<TEvService::TEvReadBlocksResponse>& response)
{
    return GetBitMap(response->Record.GetUnencryptedBlockMask());
}

TDynBitMap GetUnencryptedBlockMask(
    const std::unique_ptr<TEvService::TEvReadBlocksLocalResponse>& response)
{
    return GetBitMap(response->Record.GetUnencryptedBlockMask());
}

TDynBitMap CreateBitmap(size_t size)
{
    TDynBitMap bitmap;
    bitmap.Reserve(size);
    bitmap.Set(0, size);
    return bitmap;
}

void MarkZeroedBlocks(TDynBitMap& bitmap, const TBlockRange32& range)
{
    if (range.End >= bitmap.Size()) {
        bitmap.Reserve(range.End);
    }
    bitmap.Set(range.Start, range.End + 1);
}

void MarkWrittenBlocks(TDynBitMap& bitmap, const TBlockRange32& range)
{
    if (range.End >= bitmap.Size()) {
        bitmap.Reserve(range.End);
    }
    bitmap.Reset(range.Start, range.End + 1);
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
    CreateCheckRangeRequest(TString id, ui32 startIndex, ui32 size, bool calculateChecksums = false)
    {
        auto request = std::make_unique<TEvVolume::TEvCheckRangeRequest>();
        request->Record.SetDiskId(id);
        request->Record.SetStartIndex(startIndex);
        request->Record.SetBlocksCount(size);
        request->Record.SetCalculateChecksums(calculateChecksums);
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

TTestActorRuntime::TEventObserver StorageStateChanger(
    ui32  flag,
    TMaybe<ui32> groupIdFilter = {})
{
    return [=] (TAutoPtr<IEventHandle>& event) {
        switch (event->GetTypeRewrite()) {
            case TEvBlobStorage::EvPutResult: {
                auto* msg = event->Get<TEvBlobStorage::TEvPutResult>();
                if (!groupIdFilter.Defined() || *groupIdFilter == msg->GroupId) {
                    const_cast<TStorageStatusFlags&>(msg->StatusFlags).Merge(
                        ui32(NKikimrBlobStorage::StatusIsValid) | ui32(flag)
                    );
                    break;
                }
            }
        }

        return TTestActorRuntime::DefaultObserverFunc(event);
    };
}

TTestActorRuntime::TEventObserver PartitionBatchWriteCollector(TTestActorRuntime& runtime, ui32 eventCount)
{
    bool dropProcessWriteQueue = true;
    ui32 cnt = 0;
    bool batchSeen = false;
    NActors::TActorId partActorId;
    return [=, &runtime] (TAutoPtr<IEventHandle>& event) mutable {
        switch (event->GetTypeRewrite()) {
            case TEvPartitionPrivate::EvProcessWriteQueue: {
                batchSeen = true;
                if (dropProcessWriteQueue) {
                    partActorId = event->Sender;
                    return TTestActorRuntime::EEventAction::DROP;
                }
                break;
            }
            case TEvService::EvWriteBlocksRequest: {
                if (++cnt == eventCount && batchSeen) {
                    dropProcessWriteQueue = false;
                    auto req =
                        std::make_unique<TEvPartitionPrivate::TEvProcessWriteQueue>();
                    runtime.Send(
                        new IEventHandle(
                            partActorId,
                            event->Sender,
                            req.release(),
                            0, // flags
                            0),
                        0);
                }
                break;
            }
        }
        return TTestActorRuntime::DefaultObserverFunc(event);
    };
}

////////////////////////////////////////////////////////////////////////////////

struct TEmpty {};

struct TBlob
{
    TBlob(
        ui32 number,
        ui8 offset,
        ui8 blockCount = 1,
        ui32 channel = 0,
        ui32 generation = 0
    )
        : Number(number)
        , Offset(offset)
        , BlockCount(blockCount)
        , Channel(channel)
        , Generation(generation)
    {}

    ui32 Number;
    ui8 Offset;
    ui8 BlockCount;
    ui32 Channel;
    ui32 Generation;
};

struct TFresh
{
    TFresh(ui8 value)
        : Value(value)
    {}

    ui8 Value;
};

using TBlockDescription = std::variant<TEmpty, TBlob, TFresh>;

using TPartitionContent = TVector<TBlockDescription>;

////////////////////////////////////////////////////////////////////////////////

struct TPartitionWithRuntime
{
    std::unique_ptr<TTestActorRuntime> Runtime;
    std::unique_ptr<TPartitionClient> Partition;
};

////////////////////////////////////////////////////////////////////////////////

class TTestVolumeProxyActor final
    : public TActorBootstrapped<TTestVolumeProxyActor>
{
private:
    ui64 BaseTabletId;
    TString BaseDiskId;
    TString BaseDiskCheckpointId;
    TPartitionContent BasePartitionContent;
    ui32 BlockCount;
    ui32 BaseBlockSize;

public:
    TTestVolumeProxyActor(
        ui64 baseTabletId,
        const TString& baseDiskId,
        const TString& baseDiskCheckpointId,
        const TPartitionContent& basePartitionContent,
        ui32 blockCount,
        ui32 baseBlockSize = DefaultBlockSize);

    void Bootstrap(const TActorContext& ctx);

private:
    STFUNC(StateWork);

    void HandleDescribeBlocksRequest(
        const TEvVolume::TEvDescribeBlocksRequest::TPtr& ev,
        const TActorContext& ctx);

    void HandleGetUsedBlocksRequest(
        const TEvVolume::TEvGetUsedBlocksRequest::TPtr& ev,
        const TActorContext& ctx);

    void HandleGetChangedBlocksRequest(
        const TEvService::TEvGetChangedBlocksRequest::TPtr& ev,
        const TActorContext& ctx);
};

////////////////////////////////////////////////////////////////////////////////

TTestVolumeProxyActor::TTestVolumeProxyActor(
        ui64 baseTabletId,
        const TString& baseDiskId,
        const TString& baseDiskCheckpointId,
        const TPartitionContent& basePartitionContent,
        ui32 blockCount,
        ui32 baseBlockSize)
    : BaseTabletId(baseTabletId)
    , BaseDiskId(baseDiskId)
    , BaseDiskCheckpointId(baseDiskCheckpointId)
    , BasePartitionContent(std::move(basePartitionContent))
    , BlockCount(blockCount)
    , BaseBlockSize(baseBlockSize)
{}

void TTestVolumeProxyActor::Bootstrap(const TActorContext& ctx)
{
    Y_UNUSED(ctx);

    Become(&TThis::StateWork);
}

void TTestVolumeProxyActor::HandleDescribeBlocksRequest(
    const TEvVolume::TEvDescribeBlocksRequest::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();
    const auto& record = msg->Record;
    const auto& diskId = record.GetDiskId();
    const auto& checkpoint = record.GetCheckpointId();

    UNIT_ASSERT_VALUES_EQUAL(BaseDiskId, diskId);
    UNIT_ASSERT_VALUES_EQUAL(BaseDiskCheckpointId, checkpoint);

    auto response = std::make_unique<TEvVolume::TEvDescribeBlocksResponse>();
    auto blockIndex = 0;

    for (const auto& descr: BasePartitionContent) {
        if (std::holds_alternative<TBlob>(descr)) {
            const auto& blob = std::get<TBlob>(descr);
            auto& blobPiece = *response->Record.AddBlobPieces();
            NKikimr::TLogoBlobID blobId(
                BaseTabletId,
                blob.Generation,
                blob.Number,
                blob.Channel,
                BaseBlockSize * 0x100,
                0);
            LogoBlobIDFromLogoBlobID(
                blobId,
                blobPiece.MutableBlobId());

            auto* range = blobPiece.AddRanges();
            range->SetBlobOffset(blob.Offset);
            range->SetBlockIndex(blockIndex);
            range->SetBlocksCount(blob.BlockCount);
            blockIndex += blob.BlockCount;
        } else if (std::holds_alternative<TFresh>(descr)) {
            const auto& fresh = std::get<TFresh>(descr);
            auto& freshBlockRange = *response->Record.AddFreshBlockRanges();
            freshBlockRange.SetStartIndex(blockIndex);
            freshBlockRange.SetBlocksCount(1);
            freshBlockRange.SetBlocksContent(
                TString(BaseBlockSize, char(fresh.Value)));
            ++blockIndex;
        } else {
            Y_ABORT_UNLESS(std::holds_alternative<TEmpty>(descr));
            ++blockIndex;
        }
    }

    ctx.Send(ev->Sender, response.release());
}

void TTestVolumeProxyActor::HandleGetUsedBlocksRequest(
    const TEvVolume::TEvGetUsedBlocksRequest::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();
    const auto& record = msg->Record;
    const auto& diskId = record.GetDiskId();

    UNIT_ASSERT_VALUES_EQUAL(BaseDiskId, diskId);

    auto response = std::make_unique<TEvVolume::TEvGetUsedBlocksResponse>();
    ui64 blockIndex = 0;

    TCompressedBitmap bitmap(BlockCount);

    for (const auto& descr: BasePartitionContent) {
        if (std::holds_alternative<TBlob>(descr)) {
            const auto& blob = std::get<TBlob>(descr);
            bitmap.Set(blockIndex, blockIndex + blob.BlockCount);
            blockIndex += blob.BlockCount;
        } else if (std::holds_alternative<TFresh>(descr)) {
            bitmap.Set(blockIndex, blockIndex + 1);
            ++blockIndex;
        } else {
            Y_ABORT_UNLESS(std::holds_alternative<TEmpty>(descr));
            ++blockIndex;
        }
    }

    auto serializer = bitmap.RangeSerializer(0, bitmap.Capacity());
    TCompressedBitmap::TSerializedChunk chunk;
    while (serializer.Next(&chunk)) {
        if (!TCompressedBitmap::IsZeroChunk(chunk)) {
            auto* usedBlock = response->Record.AddUsedBlocks();
            usedBlock->SetChunkIdx(chunk.ChunkIdx);
            usedBlock->SetData(chunk.Data.data(), chunk.Data.size());
        }
    }

    // serialize/deserialize for better testing (to catch the bug NBS-3934)
    auto serialized = response->Record.SerializeAsString();
    UNIT_ASSERT(response->Record.ParseFromString(serialized));

    ctx.Send(ev->Sender, response.release());
}

void TTestVolumeProxyActor::HandleGetChangedBlocksRequest(
    const TEvService::TEvGetChangedBlocksRequest::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();
    const auto& record = msg->Record;

    UNIT_ASSERT_VALUES_EQUAL(BaseDiskId, record.GetDiskId());
    UNIT_ASSERT_VALUES_EQUAL(BaseDiskCheckpointId, record.GetHighCheckpointId());
    UNIT_ASSERT_VALUES_EQUAL("", record.GetLowCheckpointId());

    auto response = std::make_unique<TEvService::TEvGetChangedBlocksResponse>();
    ui64 blockIndex = 0;

    TVector<ui8> changedBlocks((record.GetBlocksCount() + 7) / 8);

    auto fillBlock = [&](ui64 block) {
        if (block < record.GetStartIndex() || block >= record.GetStartIndex() + record.GetBlocksCount()) {
            return;
        }

        ui64 bit = block - record.GetStartIndex();
        changedBlocks[bit / 8] |= 1 << (bit % 8);
    };

    for (const auto& descr: BasePartitionContent) {
        if (std::holds_alternative<TBlob>(descr)) {
            const auto& blob = std::get<TBlob>(descr);
            for (ui64 block = blockIndex; block < blockIndex + blob.BlockCount; block++) {
                fillBlock(block);
            }
            blockIndex += blob.BlockCount;
        } else if (std::holds_alternative<TFresh>(descr)) {
            fillBlock(blockIndex);
            ++blockIndex;
        } else {
            Y_ABORT_UNLESS(std::holds_alternative<TEmpty>(descr));
            ++blockIndex;
        }
    }

    for (const auto& b: changedBlocks) {
        response->Record.MutableMask()->push_back(b);
    }

    ctx.Send(ev->Sender, response.release());
}

STFUNC(TTestVolumeProxyActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvVolume::TEvDescribeBlocksRequest, HandleDescribeBlocksRequest);
        HFunc(TEvVolume::TEvGetUsedBlocksRequest, HandleGetUsedBlocksRequest);
        HFunc(TEvService::TEvGetChangedBlocksRequest, HandleGetChangedBlocksRequest);
        IgnoreFunc(TEvVolume::TEvMapBaseDiskIdToTabletId);
        IgnoreFunc(TEvVolume::TEvClearBaseDiskIdToTabletIdMapping);

        default:
            HandleUnexpectedEvent(ev, TBlockStoreComponents::VOLUME_PROXY);
            break;
    }
}

////////////////////////////////////////////////////////////////////////////////

bool EventHandler(
    ui64 tabletId,
    const TEvBlobStorage::TEvGet::TPtr& ev,
    TTestActorRuntimeBase& runtime,
    ui32 blockSize = DefaultBlockSize)
{
    bool result = false;

    auto* msg = ev->Get();

    auto response =
        std::make_unique<TEvBlobStorage::TEvGetResult>(
            NKikimrProto::OK, msg->QuerySize, 0 /* groupId */);

    for (ui32 i = 0; i < msg->QuerySize; ++i) {
        const auto& q = msg->Queries[i];
        const auto& blobId = q.Id;

        UNIT_ASSERT_C(
            !result || blobId.TabletID() == tabletId,
            "All blobs in one TEvGet request should belong to one partition;"
            " tabletId: " << tabletId <<
            " blobId: " << blobId);

        if (blobId.TabletID() == tabletId) {
            result = true;

            auto& r = response->Responses[i];

            r.Id = blobId;
            r.Status = NKikimrProto::OK;

            UNIT_ASSERT(q.Size % blockSize == 0);
            UNIT_ASSERT(q.Shift % blockSize == 0);

            auto blobOffset = q.Shift / blockSize;

            const auto blobEnd = blobOffset + q.Size / blockSize;
            for (; blobOffset < blobEnd; ++blobOffset) {
                UNIT_ASSERT_C(
                    blobOffset <= 0xff,
                    "Blob offset should fit in one byte");

                // Debugging is easier when block content is equal to blob offset.
                r.Buffer.Insert(r.Buffer.End(), TRope(TString(blockSize, char(blobOffset))));
            }
        }
    }

    if (result) {
        runtime.Schedule(
            new IEventHandle(
                ev->Sender,
                ev->Recipient,
                response.release(),
                0,
                ev->Cookie),
            TDuration());
    }
    return result;
}

TPartitionWithRuntime SetupOverlayPartition(
    ui64 overlayTabletId,
    ui64 baseTabletId,
    const TPartitionContent& basePartitionContent = {},
    TMaybe<ui32> channelsCount = {},
    ui32 blockSize = DefaultBlockSize,
    ui32 blockCount = 1024,
    const NProto::TStorageServiceConfig& config = DefaultConfig())
{
    TPartitionWithRuntime result;

    result.Runtime = PrepareTestActorRuntime(
        config,
        blockCount,
        channelsCount,
        {
            "overlay-disk",
            "base-disk",
            "checkpoint",
            overlayTabletId,
            baseTabletId,
            NCloud::NProto::STORAGE_MEDIA_DEFAULT
        },
        std::make_unique<TTestVolumeProxyActor>(
            baseTabletId,
            "base-disk",
            "checkpoint",
            basePartitionContent,
            blockCount,
            blockSize));

    bool baseDiskIsMapped = false;
    result.Runtime->SetEventFilter([&]
        (TTestActorRuntimeBase& runtime, TAutoPtr<IEventHandle>& ev)
    {
        Y_UNUSED(runtime);

        if (ev->GetTypeRewrite() == TEvVolume::EvMapBaseDiskIdToTabletId) {
            const auto* msg = ev->Get<TEvVolume::TEvMapBaseDiskIdToTabletId>();

            UNIT_ASSERT_VALUES_EQUAL(msg->BaseDiskId, "base-disk");
            UNIT_ASSERT_VALUES_EQUAL(msg->BaseTabletId, baseTabletId);
            baseDiskIsMapped = true;
        }
        return false;
    });

    result.Partition = std::make_unique<TPartitionClient>(*result.Runtime);
    result.Partition->WaitReady();

    UNIT_ASSERT(baseDiskIsMapped);

    result.Runtime->SetEventFilter([baseTabletId, blockSize] (TTestActorRuntimeBase& runtime, TAutoPtr<IEventHandle>& ev) {
        bool handled = false;

        const auto wrapped =
            [&] (const auto& ev) {
                handled = EventHandler(baseTabletId, ev, runtime, blockSize);
            };

        switch (ev->GetTypeRewrite()) {
            hFunc(TEvBlobStorage::TEvGet, wrapped);
        }
        return handled;
    });

    return result;
}

TString GetBlocksContent(
    const TPartitionContent& content,
    size_t blockSize = DefaultBlockSize)
{
    TString result;

    for (const auto& descr: content) {
        if (std::holds_alternative<TEmpty>(descr)) {
            result += TString(blockSize, char(0));
        } else if (std::holds_alternative<TBlob>(descr)) {
            const auto& blob = std::get<TBlob>(descr);
            for (auto i = 0; i < blob.BlockCount; ++i) {
                const auto blobOffset = blob.Offset + i;
                // Debugging is easier when block content is equal to blob offset.
                result += TString(blockSize, char(blobOffset));
            }
        } else if (std::holds_alternative<TFresh>(descr)) {
            const auto& fresh = std::get<TFresh>(descr);
            result += TString(blockSize, char(fresh.Value));
        } else {
            Y_ABORT_UNLESS(false);
        }
    }

    return result;
}

TString BuildRemoteHttpQuery(
    ui64 tabletId,
    const TVector<std::pair<TString, TString>>& keyValues,
    const TString& fragment = "")
{
    auto res = TStringBuilder()
        << "/app?TabletID="
        << tabletId;
    for (const auto& p: keyValues) {
        res << "&" << p.first << "=" << p.second;
    }
    if (fragment) {
        res << "#" << fragment;
    }
    return res;
}

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

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TPartitionTest)
{
    Y_UNIT_TEST(ShouldWaitReady)
    {
        auto runtime = PrepareTestActorRuntime();

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        partition.StatPartition();
    }

    Y_UNIT_TEST(ShouldRecoverStateOnReboot)
    {
        auto runtime = PrepareTestActorRuntime();

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        partition.RebootTablet();

        partition.StatPartition();
    }

    Y_UNIT_TEST(ShouldStoreBlocks)
    {
        auto runtime = PrepareTestActorRuntime();

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        partition.WriteBlocks(1, 1);
        partition.WriteBlocks(2, 2);
        partition.WriteBlocks(3, 3);

        auto response = partition.StatPartition();
        const auto& stats = response->Record.GetStats();
        UNIT_ASSERT_VALUES_EQUAL(stats.GetFreshBlocksCount(), 3);

        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(1),
            GetBlockContent(partition.ReadBlocks(1))
        );
        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(2),
            GetBlockContent(partition.ReadBlocks(2))
        );
        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(3),
            GetBlockContent(partition.ReadBlocks(3))
        );

        UNIT_ASSERT(stats.GetUserWriteCounters().GetExecTime() != 0);
    }

    Y_UNIT_TEST(ShouldStoreBlocksInFreshChannel)
    {
        auto config = DefaultConfig();
        config.SetFreshChannelCount(1);
        config.SetFreshChannelWriteRequestsEnabled(true);

        auto runtime = PrepareTestActorRuntime(config);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        partition.WriteBlocks(1, 1);
        partition.WriteBlocks(2, 2);
        partition.WriteBlocks(3, 3);

        auto response = partition.StatPartition();
        const auto& stats = response->Record.GetStats();
        UNIT_ASSERT_VALUES_EQUAL(stats.GetFreshBlocksCount(), 3);

        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(1),
            GetBlockContent(partition.ReadBlocks(1))
        );
        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(2),
            GetBlockContent(partition.ReadBlocks(2))
        );
        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(3),
            GetBlockContent(partition.ReadBlocks(3))
        );

        UNIT_ASSERT(stats.GetUserWriteCounters().GetExecTime() != 0);
    }

    Y_UNIT_TEST(ShouldStoreBlocksAtTheEndOfAMaxSizeDisk)
    {
        auto config = DefaultConfig();
        config.SetWriteBlobThreshold(1_MB);
        auto runtime = PrepareTestActorRuntime(
            config,
            MaxPartitionBlocksCount
        );

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        const auto blockRange = TBlockRange32::MakeClosedInterval(
            Max<ui32>() - 500,
            Max<ui32>() - 2);
        partition.WriteBlocks(blockRange, 1);

        for (auto blockIndex: xrange(blockRange)) {
            UNIT_ASSERT_VALUES_EQUAL(
                GetBlockContent(1),
                GetBlockContent(partition.ReadBlocks(blockIndex))
            );
        }

        partition.WriteBlocks(Max<ui32>() - 2, 2);
        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(2),
            GetBlockContent(partition.ReadBlocks(Max<ui32>() - 2))
        );

        partition.Flush();
        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(2),
            GetBlockContent(partition.ReadBlocks(Max<ui32>() - 2))
        );

        partition.Compaction();
        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(2),
            GetBlockContent(partition.ReadBlocks(Max<ui32>() - 2))
        );
    }

    Y_UNIT_TEST(ShouldBatchSmallWritesToMixedChannelIfThresholdExceeded)
    {
        NProto::TStorageServiceConfig config;
        config.SetWriteRequestBatchingEnabled(true);
        config.SetWriteBlobThreshold(2_MB);
        auto runtime = PrepareTestActorRuntime(config);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        const ui32 blockCount = 1000;
        runtime->SetObserverFunc(
            PartitionBatchWriteCollector(*runtime, blockCount));

        for (ui32 i = 0; i < blockCount; ++i) {
            partition.SendWriteBlocksRequest(i, i);
        }

        for (ui32 i = 0; i < blockCount; ++i) {
            auto response = partition.RecvWriteBlocksResponse();
            UNIT_ASSERT(SUCCEEDED(response->GetStatus()));
        }

        auto response = partition.StatPartition();
        const auto& stats = response->Record.GetStats();
        UNIT_ASSERT(stats.GetMixedBlobsCount());
        UNIT_ASSERT_VALUES_EQUAL(blockCount - 1, stats.GetMixedBlocksCount());
        UNIT_ASSERT_VALUES_EQUAL(0, stats.GetMergedBlocksCount());
        UNIT_ASSERT_VALUES_EQUAL(1, stats.GetFreshBlocksCount());
        UNIT_ASSERT_VALUES_EQUAL(blockCount, stats.GetUsedBlocksCount());
        UNIT_ASSERT_VALUES_EQUAL(
            blockCount,
            stats.GetUserWriteCounters().GetRequestsCount());
        const auto batchCount = stats.GetUserWriteCounters().GetBatchCount();
        UNIT_ASSERT(batchCount < blockCount);
        UNIT_ASSERT(batchCount > 0);

        for (ui32 i = 0; i < blockCount; ++i) {
            UNIT_ASSERT_VALUES_EQUAL(
                GetBlockContent(i),
                GetBlockContent(partition.ReadBlocks(i)));
        }

        UNIT_ASSERT(stats.GetUserWriteCounters().GetExecTime() != 0);

        // checking that drain-related counters are in a consistent state
        partition.Drain();
    }

    Y_UNIT_TEST(ShouldBatchIntersectingWrites)
    {
        NProto::TStorageServiceConfig config;
        config.SetWriteRequestBatchingEnabled(true);
        auto runtime = PrepareTestActorRuntime(config);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        runtime->SetObserverFunc(PartitionBatchWriteCollector(*runtime, 1000));

        for (ui32 i = 0; i < 10; ++i) {
            for (ui32 j = 0; j < 100; ++j) {
                partition.SendWriteBlocksRequest(
                    TBlockRange32::WithLength(i * 100, j + 1),
                    i + 1);
            }
        }

        for (ui32 i = 0; i < 1000; ++i) {
            auto response = partition.RecvWriteBlocksResponse();
            UNIT_ASSERT(SUCCEEDED(response->GetStatus()));
        }

        auto response = partition.StatPartition();
        const auto& stats = response->Record.GetStats();
        UNIT_ASSERT(stats.GetMixedBlobsCount());
        UNIT_ASSERT_VALUES_EQUAL(999, stats.GetMixedBlocksCount());
        UNIT_ASSERT_VALUES_EQUAL(0, stats.GetMergedBlocksCount());
        UNIT_ASSERT_VALUES_EQUAL(100, stats.GetFreshBlocksCount());
        UNIT_ASSERT_VALUES_EQUAL(1000, stats.GetUsedBlocksCount());
        UNIT_ASSERT_VALUES_EQUAL(
            1000,
            stats.GetUserWriteCounters().GetRequestsCount());
        const auto batchCount = stats.GetUserWriteCounters().GetBatchCount();
        UNIT_ASSERT(batchCount < 1000);
        UNIT_ASSERT(batchCount > 0);

        for (ui32 i = 0; i < 10; ++i) {
            for (ui32 j = 0; j < 100; ++j) {
                UNIT_ASSERT_VALUES_EQUAL(
                    GetBlockContent(i + 1),
                    GetBlockContent(partition.ReadBlocks(i * 100 + j)));
            }
        }

        UNIT_ASSERT(stats.GetUserWriteCounters().GetExecTime() != 0);

        // checking that drain-related counters are in a consistent state
        partition.Drain();
    }

    Y_UNIT_TEST(ShouldRespectMaxBlobRangeSizeDuringBatching)
    {
        NProto::TStorageServiceConfig config = DefaultConfig();
        const auto maxBlobRangeSize = 2048;
        config.SetMaxBlobRangeSize(maxBlobRangeSize * 4_KB);
        config.SetWriteRequestBatchingEnabled(true);
        auto runtime = PrepareTestActorRuntime(config, maxBlobRangeSize * 2);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        for (ui32 i = 0; i < maxBlobRangeSize; ++i) {
            partition.SendWriteBlocksRequest(i % 2 ? i : maxBlobRangeSize + i, i);
        }

        runtime->SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) {
                switch (event->GetTypeRewrite()) {
                    case TEvPartitionPrivate::EvAddBlobsRequest: {
                        auto* msg = event->Get<TEvPartitionPrivate::TEvAddBlobsRequest>();
                        if (msg->Mode == EAddBlobMode::ADD_WRITE_RESULT) {
                            const auto& mixedBlobs = msg->MixedBlobs;
                            for (const auto& blob: mixedBlobs) {
                                const auto blobRangeSize =
                                    blob.Blocks.back() - blob.Blocks.front();
                                Cdbg << blobRangeSize << Endl;
                                UNIT_ASSERT(blobRangeSize <= maxBlobRangeSize);
                            }
                        }

                        break;
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            }
        );

        for (ui32 i = 0; i < maxBlobRangeSize; ++i) {
            auto response = partition.RecvWriteBlocksResponse();
            UNIT_ASSERT(SUCCEEDED(response->GetStatus()));
        }

        auto response = partition.StatPartition();
        const auto& stats = response->Record.GetStats();
        UNIT_ASSERT(stats.GetMixedBlobsCount());

        for (ui32 i = 0; i < maxBlobRangeSize; ++i) {
            const auto block =
                partition.ReadBlocks(i % 2 ? i : maxBlobRangeSize + i);
            UNIT_ASSERT_VALUES_EQUAL(GetBlockContent(i), GetBlockContent(block));
        }

        UNIT_ASSERT(stats.GetUserWriteCounters().GetExecTime() != 0);

        // checking that drain-related counters are in a consistent state
        partition.Drain();
    }

    Y_UNIT_TEST(ShouldStoreBlocksUsingLocalAPI)
    {
        auto runtime = PrepareTestActorRuntime();

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        auto blockContent1 = GetBlockContent(1);
        partition.WriteBlocksLocal(1, blockContent1);
        auto blockContent2 = GetBlockContent(2);
        partition.WriteBlocksLocal(2, blockContent2);
        auto blockContent3 = GetBlockContent(3);
        partition.WriteBlocksLocal(3, blockContent3);

        auto response = partition.StatPartition();
        const auto& stats = response->Record.GetStats();
        UNIT_ASSERT_VALUES_EQUAL(3, stats.GetFreshBlocksCount());

        {
            TString block(DefaultBlockSize, 0);
            partition.ReadBlocksLocal(1, block);
            UNIT_ASSERT_VALUES_EQUAL(GetBlockContent(1), block);
        }

        {
            TString block(DefaultBlockSize, 0);
            partition.ReadBlocksLocal(2, block);
            UNIT_ASSERT_VALUES_EQUAL(GetBlockContent(2), block);
        }

        {
            TString block(DefaultBlockSize, 0);
            partition.ReadBlocksLocal(3, block);
            UNIT_ASSERT_VALUES_EQUAL(GetBlockContent(3), block);
        }
    }

    Y_UNIT_TEST(ShouldRecoverBlocksOnReboot)
    {
        auto runtime = PrepareTestActorRuntime();

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        partition.WriteBlocks(1, 1);
        partition.WriteBlocks(2, 2);
        partition.WriteBlocks(3, 3);

        auto response = partition.StatPartition();
        const auto& stats = response->Record.GetStats();
        UNIT_ASSERT_VALUES_EQUAL(stats.GetFreshBlocksCount(), 3);

        partition.RebootTablet();

        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(1),
            GetBlockContent(partition.ReadBlocks(1))
        );
        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(2),
            GetBlockContent(partition.ReadBlocks(2))
        );
        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(3),
            GetBlockContent(partition.ReadBlocks(3))
        );
    }

    Y_UNIT_TEST(ShouldRecoverBlocksOnRebootFromFreshChannel)
    {
        auto config = DefaultConfig();
        config.SetFreshChannelCount(1);
        config.SetFreshChannelWriteRequestsEnabled(true);

        auto runtime = PrepareTestActorRuntime(config);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        partition.WriteBlocks(1, 1);
        partition.WriteBlocks(2, 2);
        partition.WriteBlocks(3, 3);

        auto response = partition.StatPartition();
        const auto& stats = response->Record.GetStats();
        UNIT_ASSERT_VALUES_EQUAL(3, stats.GetFreshBlocksCount());

        partition.RebootTablet();

        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(1),
            GetBlockContent(partition.ReadBlocks(1))
        );
        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(2),
            GetBlockContent(partition.ReadBlocks(2))
        );
        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(3),
            GetBlockContent(partition.ReadBlocks(3))
        );
    }

    Y_UNIT_TEST(ShouldRecoverBlocksOnRebootFromFreshChannelWithLongHistory)
    {
        const ui32 blockCount = 1024;
        const ui32 channelCount = DataChannelOffset + 2;
        const ui32 freshChannelNo = channelCount - 1;
        const ui32 freshChannelHistorySize = 3;
        const auto groupCount =
            channelCount - DataChannelOffset + freshChannelHistorySize - 1;
        const ui32 nodeIdx = 0;

        // initializing test runtime

        TTestEnv env(0, 1, channelCount, groupCount);
        auto& runtime = env.GetRuntime();
        InitLogSettings(runtime);

        auto config = DefaultConfig();
        config.SetFreshChannelCount(1);
        config.SetFreshChannelWriteRequestsEnabled(true);

        {
            env.CreateSubDomain("nbs");
            auto storageConfig = CreateTestStorageConfig(config);
            env.CreateBlockStoreNode(
                "nbs",
                storageConfig,
                CreateTestDiagnosticsConfig());
        }

        const auto tabletId = NKikimr::MakeTabletID(1, HiveId, 1);
        std::unique_ptr<TTabletStorageInfo> x(new TTabletStorageInfo());

        x->TabletID = tabletId;
        x->TabletType = TTabletTypes::BlockStorePartition;
        auto& channels = x->Channels;
        channels.resize(channelCount);

        for (ui64 channel = 0; channel < channels.size(); ++channel) {
            channels[channel].Channel = channel;
            channels[channel].Type =
                TBlobStorageGroupType(BootGroupErasure);
            channels[channel].History.resize(1);
            channels[channel].History[0].FromGeneration = 0;
            const auto gidx =
                channel > DataChannelOffset ? channel - DataChannelOffset : 0;
            channels[channel].History[0].GroupID = env.GetGroupIds()[gidx];
        }

        // filling fresh channel history with 3 items

        auto& freshChannel = channels[freshChannelNo];
        freshChannel.Channel = freshChannelNo;
        freshChannel.Type = TBlobStorageGroupType(BootGroupErasure);
        auto& history = freshChannel.History;
        history.resize(freshChannelHistorySize);
        auto setHistoryItem = [&] (ui32 i, ui32 gen) {
            history[i].FromGeneration = gen;
            const auto gidx = freshChannelNo - DataChannelOffset + i;
            history[i].GroupID = env.GetGroupIds()[gidx];
        };
        setHistoryItem(0, 1);
        setHistoryItem(1, 2);
        setHistoryItem(2, 3); // this one is a bit fake - it's from the future

        InitTestActorRuntime(
            runtime,
            config,
            blockCount,
            channelCount,
            std::move(x));

        // fresh blob builder func

        auto makeResponse = [&] (ui32 gen, ui32 step, ui32 block, TString data) {
            TVector<TVector<TString>> buffers{{std::move(data)}};
            const auto holders = GetHolders(buffers);
            auto blob = BuildWriteFreshBlocksBlobContent(
                {TBlockRange32::MakeOneBlock(block)},
                holders);

            TPartialBlobId blobId(
                gen,
                step,
                freshChannelNo,
                static_cast<ui32>(blob.size()),
                0,  // cookie
                0   // partId
            );

            return TEvBlobStorage::TEvRangeResult::TResponse(
                MakeBlobId(tabletId, blobId),
                std::move(blob));
        };

        // setting up event observer (filter) to replace real EvRangeResult
        // with a fake one

        TVector<ui32> groupsRequested;
        runtime.SetEventFilter(
            [&] (TTestActorRuntimeBase& runtime, TAutoPtr<IEventHandle>& event)
            {
                switch (event->GetTypeRewrite()) {
                    case TEvBlobStorage::EvRangeResult: {
                        using TEvent = TEvBlobStorage::TEvRangeResult;
                        auto* msg = event->Get<TEvent>();

                        if (msg->From.Channel() != freshChannelNo) {
                            return false;
                        }

                        auto response = std::make_unique<TEvent>(
                            msg->Status,
                            msg->From,
                            msg->To,
                            msg->GroupId);

                        groupsRequested.push_back(msg->GroupId);

                        if (msg->GroupId == history[0].GroupID) {
                            response->Responses.push_back(makeResponse(
                                0,
                                1,
                                0,
                                TString(DefaultBlockSize, 'A')));
                        } else if (msg->GroupId == history[1].GroupID) {
                            response->Responses.push_back(makeResponse(
                                1,
                                1,
                                1,
                                TString(DefaultBlockSize, 'B')));
                        } else if (msg->GroupId == history[2].GroupID) {
                            response->Responses.push_back(makeResponse(
                                2,
                                1,
                                2,
                                TString(DefaultBlockSize, 'C')));
                        } else {
                            UNIT_ASSERT_C(
                                false,
                                TStringBuilder() << "unexpected group: "
                                    << msg->GroupId);
                        }

                        auto* handle = new IEventHandle(
                            event->Recipient,
                            event->Sender,
                            response.release(),
                            0,
                            event->Cookie);

                        runtime.Send(handle, 0);

                        return true;
                    }

                    default: return false;
                }
            }
        );

        TPartitionClient partition(runtime, nodeIdx, tabletId);
        partition.WaitReady();

        // all groups should've been requested upon first tablet launch

        Sort(groupsRequested);
        UNIT_ASSERT_VALUES_EQUAL(3, groupsRequested.size());
        UNIT_ASSERT_VALUES_EQUAL(history[0].GroupID, groupsRequested[0]);
        UNIT_ASSERT_VALUES_EQUAL(history[1].GroupID, groupsRequested[1]);
        UNIT_ASSERT_VALUES_EQUAL(history[2].GroupID, groupsRequested[2]);
        groupsRequested.clear();

        // checking that our data wasn't corrupted

        UNIT_ASSERT_VALUES_EQUAL(
            TString(DefaultBlockSize, 'A'),
            GetBlockContent(partition.ReadBlocks(0)));
        UNIT_ASSERT_VALUES_EQUAL(
            TString(DefaultBlockSize, 'B'),
            GetBlockContent(partition.ReadBlocks(1)));
        UNIT_ASSERT_VALUES_EQUAL(
            TString(DefaultBlockSize, 'C'),
            GetBlockContent(partition.ReadBlocks(2)));

        // triggering Trim to initialize TrimFreshLogToCommitId

        partition.TrimFreshLog();

        // all fresh blocks loaded upon startup acquire barriers in
        // TrimFreshLogBarriers, that's why we need to cause another Flush
        // to move our TrimFreshLogToCommitId in meta past some of those blocks

        partition.WriteBlocks(3, TString(DefaultBlockSize, 'D'));
        partition.Flush();

        partition.RebootTablet();
        partition.WaitReady();

        // after reboot our tablet shouldn't load fresh blobs from the groups
        // that certainly contain only the data that was generated before
        // TrimFreshLogToCommitId

        Sort(groupsRequested);
        UNIT_ASSERT_VALUES_EQUAL(2, groupsRequested.size());
        UNIT_ASSERT_VALUES_EQUAL(history[1].GroupID, groupsRequested[0]);
        UNIT_ASSERT_VALUES_EQUAL(history[2].GroupID, groupsRequested[1]);
        groupsRequested.clear();

        // checking that our data wasn't corrupted

        UNIT_ASSERT_VALUES_EQUAL(
            TString(DefaultBlockSize, 'A'),
            GetBlockContent(partition.ReadBlocks(0)));
        UNIT_ASSERT_VALUES_EQUAL(
            TString(DefaultBlockSize, 'B'),
            GetBlockContent(partition.ReadBlocks(1)));
        UNIT_ASSERT_VALUES_EQUAL(
            TString(DefaultBlockSize, 'C'),
            GetBlockContent(partition.ReadBlocks(2)));
        UNIT_ASSERT_VALUES_EQUAL(
            TString(DefaultBlockSize, 'D'),
            GetBlockContent(partition.ReadBlocks(3)));
    }

    Y_UNIT_TEST(ShouldFlushAsNewBlob)
    {
        auto runtime = PrepareTestActorRuntime();

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        partition.WriteBlocks(1, 1);
        partition.WriteBlocks(2, 2);
        partition.WriteBlocks(3, 3);

        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(3, stats.GetFreshBlocksCount());
            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetMixedBlocksCount());
            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetMixedBlobsCount());
        }

        partition.Flush();

        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetFreshBlocksCount());
            UNIT_ASSERT_VALUES_EQUAL(3, stats.GetMixedBlocksCount());
            UNIT_ASSERT_VALUES_EQUAL(1, stats.GetMixedBlobsCount());

            UNIT_ASSERT(stats.GetSysWriteCounters().GetExecTime() != 0);
        }

        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(1),
            GetBlockContent(partition.ReadBlocks(1))
        );
        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(2),
            GetBlockContent(partition.ReadBlocks(2))
        );
        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(3),
            GetBlockContent(partition.ReadBlocks(3))
        );
    }

    Y_UNIT_TEST(ShouldFlushBlocksFromFreshChannelAsNewBlob)
    {
        auto config = DefaultConfig();
        config.SetFreshChannelCount(1);
        config.SetFreshChannelWriteRequestsEnabled(true);

        auto runtime = PrepareTestActorRuntime(config);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        partition.WriteBlocks(1, 1);
        partition.WriteBlocks(2, 2);
        partition.WriteBlocks(3, 3);

        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(3, stats.GetFreshBlocksCount());
            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetMixedBlocksCount());
            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetMixedBlobsCount());
        }

        partition.Flush();

        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetFreshBlocksCount());
            UNIT_ASSERT_VALUES_EQUAL(3, stats.GetMixedBlocksCount());
            UNIT_ASSERT_VALUES_EQUAL(1, stats.GetMixedBlobsCount());

            UNIT_ASSERT(stats.GetSysWriteCounters().GetExecTime() != 0);
        }

        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(1),
            GetBlockContent(partition.ReadBlocks(1))
        );
        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(2),
            GetBlockContent(partition.ReadBlocks(2))
        );
        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(3),
            GetBlockContent(partition.ReadBlocks(3))
        );
    }

    Y_UNIT_TEST(ShouldAutomaticallyFlush)
    {
        auto config = DefaultConfig();
        config.SetWriteBlobThreshold(4_MB);

        auto runtime = PrepareTestActorRuntime(config);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        partition.WriteBlocks(TBlockRange32::WithLength(0, MaxBlocksCount - 1));

        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(
                MaxBlocksCount - 1,
                stats.GetFreshBlocksCount()
            );
            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetMixedBlocksCount());
            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetMixedBlobsCount());
        }

        partition.WriteBlocks(MaxBlocksCount - 1, 0);

        // wait for background operations completion
        runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetFreshBlocksCount());
            UNIT_ASSERT_VALUES_EQUAL(MaxBlocksCount, stats.GetMixedBlocksCount());
            UNIT_ASSERT_VALUES_EQUAL(1, stats.GetMixedBlobsCount());
        }
    }

    Y_UNIT_TEST(ShouldAutomaticallyFlushBlocksFromFreshChannel)
    {
        auto config = DefaultConfig();
        config.SetWriteBlobThreshold(4_MB);
        config.SetFreshChannelCount(1);
        config.SetFreshChannelWriteRequestsEnabled(true);

        auto runtime = PrepareTestActorRuntime(config);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        partition.WriteBlocks(TBlockRange32::WithLength(0, MaxBlocksCount - 1));

        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(
                MaxBlocksCount - 1,
                stats.GetFreshBlocksCount()
            );
            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetMixedBlocksCount());
            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetMixedBlobsCount());
        }

        partition.WriteBlocks(MaxBlocksCount - 1, 0);

        // wait for background operations completion
        runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetFreshBlocksCount());
            UNIT_ASSERT_VALUES_EQUAL(MaxBlocksCount, stats.GetMixedBlocksCount());
            UNIT_ASSERT_VALUES_EQUAL(1, stats.GetMixedBlobsCount());
        }
    }

    Y_UNIT_TEST(ShouldAutomaticallyFlushBlocksWhenFreshBlobCountThresholdIsReached)
    {
        auto config = DefaultConfig();
        config.SetWriteBlobThreshold(4_MB);
        config.SetFreshBlobCountFlushThreshold(4);
        config.SetFreshBlobByteCountFlushThreshold(999999999);
        config.SetFreshChannelCount(1);
        config.SetFreshChannelWriteRequestsEnabled(true);

        auto runtime = PrepareTestActorRuntime(config);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        partition.WriteBlocks(TBlockRange32::WithLength(0, MaxBlocksCount - 1));
        partition.WriteBlocks(TBlockRange32::WithLength(0, MaxBlocksCount - 1));
        partition.WriteBlocks(TBlockRange32::WithLength(0, MaxBlocksCount - 1));

        runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(
                MaxBlocksCount - 1,
                stats.GetFreshBlocksCount()
            );
            UNIT_ASSERT_VALUES_EQUAL(3, stats.GetFreshBlobsCount());
            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetMixedBlocksCount());
            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetMixedBlobsCount());
        }

        partition.WriteBlocks(0, 0);

        // wait for background operations completion
        runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetFreshBlocksCount());
            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetFreshBlobsCount());
            UNIT_ASSERT_VALUES_EQUAL(
                MaxBlocksCount - 1,
                stats.GetMixedBlocksCount()
            );
            UNIT_ASSERT_VALUES_EQUAL(1, stats.GetMixedBlobsCount());
        }
    }

    Y_UNIT_TEST(ShouldAutomaticallyFlushBlocksWhenFreshBlobByteCountThresholdIsReached)
    {
        auto config = DefaultConfig();
        config.SetWriteBlobThreshold(4_MB);
        config.SetFreshBlobCountFlushThreshold(999999);
        config.SetFreshBlobByteCountFlushThreshold(15_MB);
        config.SetFreshChannelCount(1);
        config.SetFreshChannelWriteRequestsEnabled(true);

        auto runtime = PrepareTestActorRuntime(config);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        partition.WriteBlocks(TBlockRange32::WithLength(0, MaxBlocksCount - 1));
        partition.WriteBlocks(TBlockRange32::WithLength(0, MaxBlocksCount - 1));
        partition.WriteBlocks(TBlockRange32::WithLength(0, MaxBlocksCount - 1));

        runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(
                MaxBlocksCount - 1,
                stats.GetFreshBlocksCount()
            );
            UNIT_ASSERT_VALUES_EQUAL(3, stats.GetFreshBlobsCount());
            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetMixedBlocksCount());
            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetMixedBlobsCount());
        }

        partition.WriteBlocks(TBlockRange32::WithLength(0, MaxBlocksCount - 1));

        // wait for background operations completion
        runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetFreshBlocksCount());
            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetFreshBlobsCount());
            UNIT_ASSERT_VALUES_EQUAL(
                MaxBlocksCount - 1,
                stats.GetMixedBlocksCount()
            );
            UNIT_ASSERT_VALUES_EQUAL(1, stats.GetMixedBlobsCount());
        }
    }


    Y_UNIT_TEST(ShouldAutomaticallyTrimFreshLogOnFlush)
    {
        auto config = DefaultConfig();
        config.SetFreshChannelCount(1);
        config.SetFreshChannelWriteRequestsEnabled(true);

        auto runtime = PrepareTestActorRuntime(config);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        partition.WriteBlocks(1, 1);
        partition.WriteBlocks(2, 2);
        partition.WriteBlocks(3, 3);

        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(3, stats.GetFreshBlocksCount());
        }

        bool trimSeen = false;
        bool trimCompletedSeen = false;

        runtime->SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) {
                switch (event->GetTypeRewrite()) {
                    case TEvBlobStorage::EvCollectGarbage: {
                        auto* msg = event->Get<TEvBlobStorage::TEvCollectGarbage>();
                        if (msg->Channel == 4) {
                            trimSeen = true;
                        }
                        break;
                    }
                    case TEvPartitionCommonPrivate::EvTrimFreshLogCompleted: {
                        auto* msg = event->Get<TEvPartitionCommonPrivate::TEvTrimFreshLogCompleted>();
                        UNIT_ASSERT(SUCCEEDED(msg->GetStatus()));
                        trimCompletedSeen = true;
                        break;
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            }
        );

        partition.Flush();

        // wait for background operations completion
        runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        UNIT_ASSERT_VALUES_EQUAL(true, trimSeen);
        UNIT_ASSERT_VALUES_EQUAL(true, trimCompletedSeen);
    }

    Y_UNIT_TEST(ShouldAutomaticallyTrimFreshBlobsFromPreviousGeneration)
    {
        auto config = DefaultConfig();
        config.SetFreshChannelCount(1);
        config.SetFreshChannelWriteRequestsEnabled(true);

        auto runtime = PrepareTestActorRuntime(config);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        partition.WriteBlocks(1, 1);
        partition.WriteBlocks(2, 2);
        partition.WriteBlocks(3, 3);

        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(3, stats.GetFreshBlocksCount());
        }

        bool trimSeen = false;
        bool trimCompletedSeen = false;

        runtime->SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) {
                switch (event->GetTypeRewrite()) {
                    case TEvBlobStorage::EvCollectGarbage: {
                        auto* msg = event->Get<TEvBlobStorage::TEvCollectGarbage>();
                        if (msg->Channel == 4) {
                            trimSeen = true;
                        }
                        break;
                    }
                    case TEvPartitionCommonPrivate::EvTrimFreshLogCompleted: {
                        trimCompletedSeen = true;
                        auto* msg = event->Get<TEvPartitionCommonPrivate::TEvTrimFreshLogCompleted>();
                        UNIT_ASSERT(SUCCEEDED(msg->GetStatus()));

                        break;
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            }
        );

        partition.KillTablet();

        {
            TPartitionClient partition(*runtime);
            partition.WaitReady();

            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetFreshBlocksCount());

            UNIT_ASSERT_VALUES_EQUAL(true, trimSeen);
            UNIT_ASSERT_VALUES_EQUAL(true, trimCompletedSeen);
        }
    }

    Y_UNIT_TEST(ShouldTrimUpToTheFirstUnflushedBlockCommitId)
    {
        auto config = DefaultConfig();
        config.SetFreshChannelCount(1);
        config.SetFreshChannelWriteRequestsEnabled(true);

        auto runtime = PrepareTestActorRuntime(config);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        partition.WriteBlocks(1, 1);
        partition.WriteBlocks(2, 2);

        TAutoPtr<IEventHandle> trim;

        ui32 collectGen = 0;
        ui32 collectStep = 0;

        runtime->SetObserverFunc([&] (TAutoPtr<IEventHandle>& event)
            {
                switch (event->GetTypeRewrite()) {
                    case TEvPartitionCommonPrivate::EvTrimFreshLogRequest: {
                        UNIT_ASSERT(!trim);
                        trim = event.Release();
                        return TTestActorRuntime::EEventAction::DROP;
                    }

                    case TEvBlobStorage::EvCollectGarbage: {
                        auto* msg = event->Get<TEvBlobStorage::TEvCollectGarbage>();
                        if (msg->Channel == 4) {
                            collectGen = msg->CollectGeneration;
                            collectStep = msg->CollectStep;
                        }
                        break;
                    }
                }

                return TTestActorRuntime::DefaultObserverFunc(event);
            }
        );

        partition.Flush();
        partition.WriteBlocks(TBlockRange32::WithLength(0, 1024), 3);
        partition.WriteBlocks(3, 4);

        UNIT_ASSERT(trim);
        runtime->Send(trim.Release());

        {
            TDispatchOptions options;
            options.FinalEvents.emplace_back(
                TEvPartitionCommonPrivate::EvTrimFreshLogCompleted);
            runtime->DispatchEvents(options);
        }

        UNIT_ASSERT_VALUES_EQUAL(2, collectGen);
        UNIT_ASSERT_VALUES_EQUAL(4, collectStep);

        partition.Flush();
        UNIT_ASSERT(trim);
        runtime->Send(trim.Release());

        {
            TDispatchOptions options;
            options.FinalEvents.emplace_back(
                TEvPartitionCommonPrivate::EvTrimFreshLogCompleted);
            runtime->DispatchEvents(options);
        }

        UNIT_ASSERT_VALUES_EQUAL(2, collectGen);
        UNIT_ASSERT_VALUES_EQUAL(6, collectStep);
    }

    Y_UNIT_TEST(ShouldNotAddSmallNonDeletionBlobsDuringFlush)
    {
        auto config = DefaultConfig(4_MB);
        config.SetWriteBlobThreshold(4_MB);

        auto runtime = PrepareTestActorRuntime(config, 2048);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        partition.WriteBlocks(TBlockRange32::WithLength(0, MaxBlocksCount - 1));

        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(
                MaxBlocksCount - 1,
                stats.GetFreshBlocksCount()
            );
            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetMixedBlocksCount());
            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetMixedBlobsCount());
        }

        constexpr ui32 extraBlocks = 4;
        partition.WriteBlocks(
            TBlockRange32::WithLength(MaxBlocksCount - 1, extraBlocks + 1));

        // wait for background operations completion
        runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(extraBlocks, stats.GetFreshBlocksCount());
            UNIT_ASSERT_VALUES_EQUAL(
                MaxBlocksCount,
                stats.GetMixedBlocksCount()
            );
            UNIT_ASSERT_VALUES_EQUAL(1, stats.GetMixedBlobsCount());
        }
    }

    Y_UNIT_TEST(ShouldAddSmallBlobsDuringFlushIfThereAreAnyBlobsInFreshChannel)
    {
        auto config = DefaultConfig(4_MB);
        config.SetWriteBlobThreshold(4_MB);
        config.SetFreshChannelCount(1);
        config.SetFreshChannelWriteRequestsEnabled(true);

        auto runtime = PrepareTestActorRuntime(config, 2048);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        partition.WriteBlocks(TBlockRange32::WithLength(0, MaxBlocksCount - 1));

        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(
                MaxBlocksCount - 1,
                stats.GetFreshBlocksCount()
            );
            UNIT_ASSERT_VALUES_EQUAL(1, stats.GetFreshBlobsCount());
            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetMixedBlocksCount());
            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetMixedBlobsCount());
        }

        constexpr ui32 extraBlocks = 4;
        partition.WriteBlocks(
            TBlockRange32::WithLength(MaxBlocksCount - 1, extraBlocks + 1));

        // wait for background operations completion
        runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetFreshBlocksCount());
            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetFreshBlobsCount());
            UNIT_ASSERT_VALUES_EQUAL(
                MaxBlocksCount + extraBlocks,
                stats.GetMixedBlocksCount()
            );
            UNIT_ASSERT_VALUES_EQUAL(2, stats.GetMixedBlobsCount());
        }
    }


    Y_UNIT_TEST(ShouldMergeVersionsOnRead)
    {
        auto runtime = PrepareTestActorRuntime();

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        partition.WriteBlocks(1, 1);
        partition.WriteBlocks(2, 2);
        partition.WriteBlocks(3, 3);
        partition.Flush();

        partition.WriteBlocks(1, 11);
        partition.Flush();

        partition.WriteBlocks(2, 22);
        partition.Flush();

        partition.WriteBlocks(3, 33);
        // partition.Flush();

        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(11),
            GetBlockContent(partition.ReadBlocks(1))
        );
        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(22),
            GetBlockContent(partition.ReadBlocks(2))
        );
        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(33),
            GetBlockContent(partition.ReadBlocks(3))
        );
    }

    Y_UNIT_TEST(ShouldReplaceBlobsOnCompaction)
    {
        auto runtime = PrepareTestActorRuntime();

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        partition.WriteBlocks(1, 1);
        partition.WriteBlocks(2, 2);
        partition.WriteBlocks(3, 3);
        partition.Flush();

        partition.WriteBlocks(1, 11);
        partition.Flush();

        partition.WriteBlocks(2, 22);
        partition.Flush();

        partition.WriteBlocks(3, 33);
        partition.Flush();

        partition.Compaction();

        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(11),
            GetBlockContent(partition.ReadBlocks(1))
        );
        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(22),
            GetBlockContent(partition.ReadBlocks(2))
        );
        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(33),
            GetBlockContent(partition.ReadBlocks(3))
        );

        auto response = partition.StatPartition();
        const auto& stats = response->Record.GetStats();

        UNIT_ASSERT(stats.GetSysReadCounters().GetExecTime() != 0);
        UNIT_ASSERT(stats.GetSysWriteCounters().GetExecTime() != 0);
        UNIT_ASSERT(stats.GetUserReadCounters().GetExecTime() != 0);
    }

    Y_UNIT_TEST(ShouldAutomaticallyRunCompaction)
    {
        static constexpr ui32 compactionThreshold = 4;

        auto config = DefaultConfig();
        config.SetSSDMaxBlobsPerRange(compactionThreshold);
        config.SetHDDMaxBlobsPerRange(compactionThreshold);

        auto runtime = PrepareTestActorRuntime(config);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        ui64 compactionByBlobCount = 0;
        ui64 compactionByReadStats = 0;

        runtime->SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) {
                switch (event->GetTypeRewrite()) {
                    case TEvStatsService::EvVolumePartCounters: {
                        auto* msg =
                            event->Get<TEvStatsService::TEvVolumePartCounters>();
                        const auto& cc = msg->DiskCounters->Cumulative;
                        compactionByBlobCount =
                            cc.CompactionByBlobCountPerRange.Value;
                        compactionByReadStats = cc.CompactionByReadStats.Value;
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            }
        );

        for (size_t i = 1; i < compactionThreshold; ++i) {
            partition.WriteBlocks(i, i);
            partition.Flush();
        }

        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(
                compactionThreshold - 1,
                stats.GetMixedBlobsCount()
            );
            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetMergedBlobsCount());
        }

        partition.WriteBlocks(0, 0);
        partition.Flush();

        // wait for background operations completion
        runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(
                compactionThreshold,
                stats.GetMixedBlobsCount()
            );
            UNIT_ASSERT_VALUES_EQUAL(1, stats.GetMergedBlobsCount());
        }

        partition.SendToPipe(
            std::make_unique<TEvPartitionPrivate::TEvUpdateCounters>());
        {
            TDispatchOptions options;
            options.FinalEvents.emplace_back(
                TEvStatsService::EvVolumePartCounters);
            runtime->DispatchEvents(options);
        }
        UNIT_ASSERT_EQUAL(1, compactionByBlobCount);
        UNIT_ASSERT_EQUAL(0, compactionByReadStats);
    }

    Y_UNIT_TEST(ShouldAutomaticallyRunCompactionForDeletionMarkers)
    {
        static constexpr ui32 compactionThreshold = 4;

        auto config = DefaultConfig();
        config.SetSSDMaxBlobsPerRange(compactionThreshold);
        config.SetHDDMaxBlobsPerRange(compactionThreshold);

        auto runtime = PrepareTestActorRuntime(config);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        for (size_t i = 1; i < compactionThreshold; ++i) {
            partition.ZeroBlocks(TBlockRange32::WithLength(0, 1024));
        }

        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(
                compactionThreshold - 1,
                stats.GetMergedBlobsCount()
            );
        }

        partition.ZeroBlocks(TBlockRange32::WithLength(0, 1024));

        // wait for background operations completion
        runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(
                compactionThreshold + 1,
                stats.GetMergedBlobsCount()
            );
        }
    }

    Y_UNIT_TEST(ShouldRespectCompactionDelay)
    {
        static constexpr ui32 compactionThreshold = 4;

        auto config = DefaultConfig();
        config.SetSSDMaxBlobsPerRange(compactionThreshold);
        config.SetHDDMaxBlobsPerRange(compactionThreshold);
        config.SetMinCompactionDelay(10000);
        config.SetMaxCompactionDelay(10000);
        config.SetCompactionScoreHistorySize(1);

        auto runtime = PrepareTestActorRuntime(config);
        runtime->AdvanceCurrentTime(TDuration::Seconds(10));

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        for (size_t i = 1; i < compactionThreshold + 1; ++i) {
            partition.WriteBlocks(i, i);
            partition.Flush();
        }

        // wait for background operations completion
        runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(
                compactionThreshold,
                stats.GetMixedBlobsCount()
            );
            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetMergedBlobsCount());
        }

        runtime->AdvanceCurrentTime(TDuration::Seconds(10));

        // wait for background operations completion
        runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(
                compactionThreshold,
                stats.GetMixedBlobsCount()
            );
            UNIT_ASSERT_VALUES_EQUAL(1, stats.GetMergedBlobsCount());
        }
    }

    Y_UNIT_TEST(ShouldRespectCleanupDelay)
    {
        auto config = DefaultConfig();
        config.SetCleanupThreshold(1);
        config.SetMinCleanupDelay(10000);
        config.SetMaxCleanupDelay(10000);
        config.SetCleanupScoreHistorySize(1);
        config.SetCollectGarbageThreshold(999999);

        auto runtime = PrepareTestActorRuntime(config);
        runtime->AdvanceCurrentTime(TDuration::Seconds(10));

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        partition.WriteBlocks(TBlockRange32::WithLength(0, 1024), 1);
        partition.WriteBlocks(TBlockRange32::WithLength(0, 1024), 2);

        partition.Compaction();

        // wait for background operations completion
        runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetGarbageQueueSize());
        }

        runtime->AdvanceCurrentTime(TDuration::Seconds(10));

        // wait for background operations completion
        runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(2, stats.GetGarbageQueueSize());
        }
    }

    Y_UNIT_TEST(ShouldCalculateCompactionAndCleanupDelays)
    {
        static constexpr ui32 compactionThreshold = 4;
        static constexpr ui32 cleanupThreshold = 10;

        auto config = DefaultConfig();
        config.SetSSDMaxBlobsPerRange(compactionThreshold);
        config.SetHDDMaxBlobsPerRange(compactionThreshold);
        config.SetCleanupThreshold(cleanupThreshold);
        config.SetMinCompactionDelay(0);
        config.SetMaxCompactionDelay(999'999'999);
        config.SetMinCleanupDelay(0);
        config.SetMaxCleanupDelay(999'999'999);
        config.SetMaxCompactionExecTimePerSecond(1);
        config.SetMaxCleanupExecTimePerSecond(1);

        auto runtime = PrepareTestActorRuntime(config);
        runtime->AdvanceCurrentTime(TDuration::Seconds(10));

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        partition.WriteBlocks(TBlockRange32::WithLength(0, 1024));
        // initializing compaction exec time
        partition.Compaction();

        for (size_t i = 0; i < compactionThreshold - 1; ++i) {
            partition.WriteBlocks(TBlockRange32::WithLength(0, 1024));
        }

        TDuration delay;
        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(
                compactionThreshold + 1,
                stats.GetMergedBlobsCount()
            );
            delay = TDuration::MilliSeconds(stats.GetCompactionDelay());
            UNIT_ASSERT_VALUES_UNEQUAL(0, delay.MicroSeconds());
        }

        runtime->AdvanceCurrentTime(delay);

        // wait for background operations completion
        runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(
                compactionThreshold + 2,
                stats.GetMergedBlobsCount()
            );
        }

        // initializing cleanup exec time
        partition.Cleanup();

        // generating enough dirty blobs for automatic cleanup
        for (ui32 i = 0; i < cleanupThreshold - 1; ++i) {
            partition.WriteBlocks(TBlockRange32::WithLength(0, 1024));
        }
        partition.Compaction();

        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT(stats.GetMergedBlobsCount() >= cleanupThreshold);
            delay = TDuration::MilliSeconds(stats.GetCleanupDelay());
            UNIT_ASSERT_VALUES_UNEQUAL(0, delay.MicroSeconds());
        }

        runtime->AdvanceCurrentTime(delay);

        // wait for background operations completion
        runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(1, stats.GetMergedBlobsCount());
        }
    }

    Y_UNIT_TEST(ShouldCompactAndCleanupWithoutDelayUponScoreOverflow)
    {
        static constexpr ui32 compactionThreshold = 4;
        static constexpr ui32 cleanupThreshold = 10;

        auto config = DefaultConfig();
        config.SetHDDCompactionType(NProto::CT_LOAD);
        config.SetHDDMaxBlobsPerRange(compactionThreshold - 1);
        config.SetCleanupThreshold(cleanupThreshold);
        config.SetMinCompactionDelay(999'999'999);
        config.SetMaxCompactionDelay(999'999'999);
        config.SetMinCleanupDelay(999'999'999);
        config.SetMaxCleanupDelay(999'999'999);
        config.SetCompactionScoreLimitForThrottling(compactionThreshold);
        config.SetCleanupQueueBytesLimitForThrottling(16_MB);

        auto runtime = PrepareTestActorRuntime(config);
        runtime->AdvanceCurrentTime(TDuration::Seconds(10));

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        for (size_t i = 0; i < compactionThreshold; ++i) {
            partition.WriteBlocks(TBlockRange32::WithLength(0, 1024));
        }
        // initializing compaction exec time
        partition.Compaction();
        // initializing cleanup exec time
        partition.Cleanup();

        for (size_t i = 0; i < compactionThreshold - 1; ++i) {
            partition.WriteBlocks(TBlockRange32::WithLength(0, 1024));
        }

        // wait for background operations completion
        runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(
                compactionThreshold + 1,
                stats.GetMergedBlobsCount()
            );
        }

        // generating enough dirty blobs for automatic cleanup
        for (ui32 i = 0; i < cleanupThreshold - compactionThreshold - 1; ++i) {
            partition.WriteBlocks(TBlockRange32::WithLength(0, 1024));
        }
        partition.Compaction(0);

        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(1, stats.GetMergedBlobsCount());
        }
    }

    Y_UNIT_TEST(ShouldAutomaticallyRunLoadOptimizingCompaction)
    {
        auto config = DefaultConfig();
        config.SetHDDCompactionType(NProto::CT_LOAD);
        config.SetHDDMaxBlobsPerRange(999);

        auto runtime = PrepareTestActorRuntime(config);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        for (ui32 i = 0; i < 512; ++i) {
            partition.WriteBlocks(TBlockRange32::WithLength(i * 2, 2));
            partition.Flush();
        }

        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(512, stats.GetMixedBlobsCount());
        }

        ui64 compactionByBlobCount = -1;
        ui64 compactionByReadStats = -1;

        bool compactionRequestObserved = false;
        runtime->SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) {
                switch (event->GetTypeRewrite()) {
                    case TEvPartitionPrivate::EvCompactionRequest: {
                        auto* msg = event->Get<TEvPartitionPrivate::TEvCompactionRequest>();
                        if (msg->Mode == TEvPartitionPrivate::RangeCompaction) {
                            compactionRequestObserved = true;
                        }
                        break;
                    }
                    case TEvStatsService::EvVolumePartCounters: {
                        auto* msg =
                            event->Get<TEvStatsService::TEvVolumePartCounters>();

                        const auto& cc = msg->DiskCounters->Cumulative;
                        compactionByBlobCount =
                            cc.CompactionByBlobCountPerRange.Value;
                        compactionByReadStats = cc.CompactionByReadStats.Value;
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            }
        );

        for (ui32 i = 0; i < 10; ++i) {
            for (ui32 j = 0; j < 10; ++j) {
                partition.ReadBlocks(TBlockRange32::WithLength(j, 101));
            }
        }

        // triggering EnqueueCompactionIfNeeded(...)
        partition.WriteBlocks(0, 0);
        partition.Flush();

        // wait for background operations completion
        runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        UNIT_ASSERT(compactionRequestObserved);
        partition.SendToPipe(
            std::make_unique<TEvPartitionPrivate::TEvUpdateCounters>());
        {
            TDispatchOptions options;
            options.FinalEvents.emplace_back(
                TEvStatsService::EvVolumePartCounters);
            runtime->DispatchEvents(options);
        }
        UNIT_ASSERT_EQUAL(0, compactionByBlobCount);
        UNIT_ASSERT_EQUAL(1, compactionByReadStats);
    }

    Y_UNIT_TEST(ShouldAutomaticallyRunGarbageCompaction)
    {
        auto config = DefaultConfig();
        config.SetHDDCompactionType(NProto::CT_LOAD);
        config.SetV1GarbageCompactionEnabled(true);
        config.SetCompactionGarbageThreshold(20);
        config.SetCompactionRangeGarbageThreshold(999999);

        auto runtime = PrepareTestActorRuntime(config, 2048);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        ui64 compactionByGarbageBlocksPerRange = 0;
        ui64 compactionByGarbageBlocksPerDisk = 0;
        bool compactionRequestObserved = false;
        runtime->SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) {
                switch (event->GetTypeRewrite()) {
                    case TEvPartitionPrivate::EvCompactionRequest: {
                        auto* msg = event->Get<TEvPartitionPrivate::TEvCompactionRequest>();
                        if (msg->Mode == TEvPartitionPrivate::GarbageCompaction) {
                            compactionRequestObserved = true;
                        }
                        break;
                    }
                    case TEvStatsService::EvVolumePartCounters: {
                        auto* msg =
                            event->Get<TEvStatsService::TEvVolumePartCounters>();
                        const auto& cc = msg->DiskCounters->Cumulative;
                        compactionByGarbageBlocksPerRange =
                            cc.CompactionByGarbageBlocksPerRange.Value;
                        compactionByGarbageBlocksPerDisk =
                            cc.CompactionByGarbageBlocksPerDisk.Value;
                        break;
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            }
        );

        partition.WriteBlocks(TBlockRange32::WithLength(0, 1024));
        partition.WriteBlocks(TBlockRange32::WithLength(0, 301));

        // wait for background operations completion
        runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        UNIT_ASSERT(compactionRequestObserved);

        compactionRequestObserved = false;

        // marking range 0 as non-compacted
        partition.WriteBlocks(0);
        partition.Flush();

        partition.WriteBlocks(TBlockRange32::MakeClosedInterval(1024, 1400));
        // 50% garbage
        partition.WriteBlocks(TBlockRange32::MakeClosedInterval(1024, 1400));

        runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        UNIT_ASSERT(compactionRequestObserved);

        compactionRequestObserved = false;

        partition.CreateCheckpoint("c1");

        // writing lots of blocks into range 1
        partition.WriteBlocks(TBlockRange32::WithLength(1024, 1024));
        partition.WriteBlocks(TBlockRange32::WithLength(1024, 1024));

        runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        // there is a checkpoint => garbage-based compaction should not run
        UNIT_ASSERT(!compactionRequestObserved);

        partition.DeleteCheckpoint("c1");

        // triggering compaction attempt, block index does not matter
        partition.WriteBlocks(0);
        partition.Flush();

        runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));
        UNIT_ASSERT(compactionRequestObserved);

        partition.SendToPipe(
            std::make_unique<TEvPartitionPrivate::TEvUpdateCounters>());
        {
            TDispatchOptions options;
            options.FinalEvents.emplace_back(
                TEvStatsService::EvVolumePartCounters);
            runtime->DispatchEvents(options);
        }

        UNIT_ASSERT(compactionByGarbageBlocksPerDisk > 0);
        UNIT_ASSERT_EQUAL(0, compactionByGarbageBlocksPerRange);
    }

    Y_UNIT_TEST(ShouldAutomaticallyRunGarbageCompactionForSuperDirtyRanges)
    {
        auto config = DefaultConfig();
        config.SetHDDCompactionType(NProto::CT_LOAD);
        config.SetV1GarbageCompactionEnabled(true);
        config.SetCompactionGarbageThreshold(999999);
        config.SetCompactionRangeGarbageThreshold(200);

        auto runtime = PrepareTestActorRuntime(config);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        ui64 compactionByGarbageBlocksPerRange = 0;
        ui64 compactionByGarbageBlocksPerDisk = 0;
        bool compactionRequestObserved = false;
        runtime->SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) {
                switch (event->GetTypeRewrite()) {
                    case TEvPartitionPrivate::EvCompactionRequest: {
                        auto* msg = event->Get<TEvPartitionPrivate::TEvCompactionRequest>();
                        if (msg->Mode == TEvPartitionPrivate::GarbageCompaction) {
                            compactionRequestObserved = true;
                        }
                        break;
                    }
                    case TEvStatsService::EvVolumePartCounters: {
                        auto* msg =
                            event->Get<TEvStatsService::TEvVolumePartCounters>();
                        const auto& cc = msg->DiskCounters->Cumulative;
                        compactionByGarbageBlocksPerRange =
                            cc.CompactionByGarbageBlocksPerRange.Value;
                        compactionByGarbageBlocksPerDisk =
                            cc.CompactionByGarbageBlocksPerDisk.Value;
                        break;
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            }
        );

        partition.WriteBlocks(TBlockRange32::WithLength(0, 1024));
        partition.WriteBlocks(TBlockRange32::WithLength(0, 1024));
        partition.WriteBlocks(TBlockRange32::WithLength(0, 1024));

        // wait for background operations completion
        runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        // garbage == used x2 => compaction
        UNIT_ASSERT(compactionRequestObserved);

        compactionRequestObserved = false;

        partition.WriteBlocks(TBlockRange32::WithLength(0, 1024));

        // wait for background operations completion
        runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        // garbage == used x1 => no compaction
        UNIT_ASSERT(!compactionRequestObserved);

        partition.CreateCheckpoint("c1");

        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(1024, stats.GetCompactionGarbageScore());
        }

        partition.WriteBlocks(TBlockRange32::WithLength(0, 1024));

        // wait for background operations completion
        runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        // garbage == used x2 => still no compaction because there is a checkpoint
        UNIT_ASSERT(!compactionRequestObserved);

        partition.DeleteCheckpoint("c1");

        partition.WriteBlocks(TBlockRange32::MakeOneBlock(0));
        partition.Flush();

        // wait for background operations completion
        runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        // garbage == used x2 => compaction
        UNIT_ASSERT(compactionRequestObserved);

        compactionRequestObserved = false;

        partition.WriteBlocks(TBlockRange32::MakeOneBlock(0));
        partition.Flush();

        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(1, stats.GetCompactionGarbageScore());
        }

        // wait for background operations completion
        runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        // block count for this range should've been reset
        UNIT_ASSERT(!compactionRequestObserved);

        // a range with no used blocks
        partition.WriteBlocks(TBlockRange32::WithLength(0, 1024));
        partition.ZeroBlocks(TBlockRange32::WithLength(0, 1024));

        // wait for background operations completion
        runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        // only garbage => compaction
        UNIT_ASSERT(compactionRequestObserved);

        compactionRequestObserved = false;

        partition.WriteBlocks(TBlockRange32::WithLength(0, 101));
        partition.Flush();

        // wait for background operations completion
        runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        // no garbage (99% of the blocks belong to a deletion marker) => no compaction
        UNIT_ASSERT(!compactionRequestObserved);

        partition.WriteBlocks(TBlockRange32::WithLength(0, 101));
        partition.Flush();

        // wait for background operations completion
        runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        // garbage == used => no compaction
        UNIT_ASSERT(!compactionRequestObserved);

        partition.WriteBlocks(TBlockRange32::WithLength(0, 101));
        partition.Flush();

        // wait for background operations completion
        runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        // garbage == 2 * used => compaction
        UNIT_ASSERT(compactionRequestObserved);

        partition.SendToPipe(
            std::make_unique<TEvPartitionPrivate::TEvUpdateCounters>());
        {
            TDispatchOptions options;
            options.FinalEvents.emplace_back(
                TEvStatsService::EvVolumePartCounters);
            runtime->DispatchEvents(options);
        }

        UNIT_ASSERT_EQUAL(0, compactionByGarbageBlocksPerDisk);
        UNIT_ASSERT(compactionByGarbageBlocksPerRange > 0);
    }

    Y_UNIT_TEST(CompactionShouldTakeCareOfFreshBlocks)
    {
        auto runtime = PrepareTestActorRuntime();

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        partition.WriteBlocks(1, 1);
        partition.WriteBlocks(2, 2);
        partition.WriteBlocks(3, 3);
        partition.Flush();

        partition.WriteBlocks(1, 11);
        partition.Flush();

        partition.WriteBlocks(2, 22);
        partition.Flush();

        partition.WriteBlocks(3, 33);
        // partition.Flush();

        partition.Compaction();

        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(11),
            GetBlockContent(partition.ReadBlocks(1))
        );
        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(22),
            GetBlockContent(partition.ReadBlocks(2))
        );
        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(33),
            GetBlockContent(partition.ReadBlocks(3))
        );
    }

    Y_UNIT_TEST(ShouldZeroBlocks)
    {
        auto runtime = PrepareTestActorRuntime();

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        partition.WriteBlocks(1, 1);
        partition.WriteBlocks(2, 2);
        partition.WriteBlocks(3, 3);

        partition.Flush();
        partition.ZeroBlocks(2);

        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(1),
            GetBlockContent(partition.ReadBlocks(1))
        );
        UNIT_ASSERT_VALUES_EQUAL(
            "",
            GetBlockContent(partition.ReadBlocks(2))
        );
        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(3),
            GetBlockContent(partition.ReadBlocks(3))
        );

        partition.Flush();
        partition.ZeroBlocks(3);

        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(1),
            GetBlockContent(partition.ReadBlocks(1))
        );
        UNIT_ASSERT_VALUES_EQUAL(
            "",
            GetBlockContent(partition.ReadBlocks(2))
        );
        UNIT_ASSERT_VALUES_EQUAL(
            "",
            GetBlockContent(partition.ReadBlocks(3))
        );
    }

    Y_UNIT_TEST(ShouldZeroBlocksWrittenToFreshChannelAfterReboot)
    {
        auto config = DefaultConfig();
        config.SetFreshChannelCount(1);
        config.SetFreshChannelWriteRequestsEnabled(true);

        auto runtime = PrepareTestActorRuntime(config);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        partition.WriteBlocks(1, 1);
        partition.ZeroBlocks(1);

        partition.RebootTablet();

        UNIT_ASSERT_VALUES_EQUAL(
            "",
            GetBlockContent(partition.ReadBlocks(1))
        );
    }

    Y_UNIT_TEST(ShouldReadZeroFromUninitializedBlock)
    {
        auto runtime = PrepareTestActorRuntime();

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        partition.WriteBlocks(1, 1);
        // partition.WriteBlocks(2, 2);
        partition.WriteBlocks(3, 3);

        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(1),
            GetBlockContent(partition.ReadBlocks(1))
        );
        UNIT_ASSERT_VALUES_EQUAL(
            "",
            GetBlockContent(partition.ReadBlocks(2))
        );
        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(3),
            GetBlockContent(partition.ReadBlocks(3))
        );

        partition.Flush();

        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(1),
            GetBlockContent(partition.ReadBlocks(1))
        );
        UNIT_ASSERT_VALUES_EQUAL(
            "",
            GetBlockContent(partition.ReadBlocks(2))
        );
        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(3),
            GetBlockContent(partition.ReadBlocks(3))
        );
    }

    Y_UNIT_TEST(ShouldZeroLargeNumberOfBlocks)
    {
        auto runtime = PrepareTestActorRuntime();

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        partition.WriteBlocks(TBlockRange32::WithLength(0, MaxBlocksCount));
        partition.ZeroBlocks(TBlockRange32::WithLength(0, MaxBlocksCount));

        UNIT_ASSERT_VALUES_EQUAL(
            "",
            GetBlockContent(partition.ReadBlocks(0))
        );
        UNIT_ASSERT_VALUES_EQUAL(
            "",
            GetBlockContent(partition.ReadBlocks(MaxBlocksCount / 2))
        );
        UNIT_ASSERT_VALUES_EQUAL(
            "",
            GetBlockContent(partition.ReadBlocks(MaxBlocksCount - 1))
        );
    }

    Y_UNIT_TEST(ShouldHandleZeroedBlocksInCompaction)
    {
        auto runtime = PrepareTestActorRuntime();

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        partition.WriteBlocks(TBlockRange32::WithLength(0, MaxBlocksCount));
        partition.Flush();

        partition.ZeroBlocks(TBlockRange32::WithLength(0, MaxBlocksCount));
        partition.Flush();

        partition.Compaction();
    }

    Y_UNIT_TEST(ShouldCleanupBlobs)
    {
        auto runtime = PrepareTestActorRuntime();

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetMixedBlobsCount());
            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetMergedBlobsCount());
        }

        partition.WriteBlocks(TBlockRange32::WithLength(0, MaxBlocksCount), 1);

        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetMixedBlobsCount());
            UNIT_ASSERT_VALUES_EQUAL(1, stats.GetMergedBlobsCount());
        }

        partition.WriteBlocks(TBlockRange32::WithLength(0, MaxBlocksCount), 2);

        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetMixedBlobsCount());
            UNIT_ASSERT_VALUES_EQUAL(2, stats.GetMergedBlobsCount());
        }

        partition.Compaction();

        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetMixedBlobsCount());
            UNIT_ASSERT_VALUES_EQUAL(3, stats.GetMergedBlobsCount());
        }

        partition.Cleanup();

        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetMixedBlobsCount());
            UNIT_ASSERT_VALUES_EQUAL(1, stats.GetMergedBlobsCount());
        }
    }

    Y_UNIT_TEST(ShouldReadFromCheckpoint)
    {
        auto runtime = PrepareTestActorRuntime();

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        partition.WriteBlocks(1, 1);
        partition.CreateCheckpoint("checkpoint1");

        partition.Flush();
        partition.WriteBlocks(1, 2);
        partition.CreateCheckpoint("checkpoint2");

        partition.Flush();
        partition.WriteBlocks(1, 3);
        partition.CreateCheckpoint("checkpoint3");

        partition.WriteBlocks(1, 4);

        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(1),
            GetBlockContent(partition.ReadBlocks(1, "checkpoint1"))
        );
        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(2),
            GetBlockContent(partition.ReadBlocks(1, "checkpoint2"))
        );
        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(3),
            GetBlockContent(partition.ReadBlocks(1, "checkpoint3"))
        );
        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(4),
            GetBlockContent(partition.ReadBlocks(1))
        );
    }

    Y_UNIT_TEST(ShouldKillTabletIfCriticalFailureDuringWriteBlocks)
    {
        auto runtime = PrepareTestActorRuntime();

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        runtime->SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) {
                switch (event->GetTypeRewrite()) {
                    case TEvPartitionPrivate::EvWriteBlobResponse: {
                        auto* msg = event->Get<TEvPartitionPrivate::TEvWriteBlobResponse>();
                        auto& e = const_cast<NProto::TError&>(msg->Error);
                        e.SetCode(E_REJECTED);
                        break;
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        partition.SendWriteBlocksRequest(
            TBlockRange32::WithLength(0, MaxBlocksCount));
        auto response = partition.RecvWriteBlocksResponse();

        UNIT_ASSERT(FAILED(response->GetStatus()));
    }

    Y_UNIT_TEST(ShouldKillTabletIfCriticalFailureDuringFlush)
    {
        auto runtime = PrepareTestActorRuntime();

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        partition.WriteBlocks(TBlockRange32::WithLength(0, 10));
        partition.WriteBlocks(TBlockRange32::WithLength(10, 10));
        partition.WriteBlocks(TBlockRange32::WithLength(20, 10));

        int pillCount = 0;

        runtime->SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) {
                switch (event->GetTypeRewrite()) {
                    case TEvents::TSystem::PoisonPill: {
                        ++pillCount;
                        break;
                    }
                    case TEvPartitionPrivate::EvWriteBlobResponse: {
                        auto* msg = event->Get<TEvPartitionPrivate::TEvWriteBlobResponse>();
                        auto& e = const_cast<NProto::TError&>(msg->Error);
                        e.SetCode(E_REJECTED);
                        break;
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        partition.SendFlushRequest();
        auto response = partition.RecvFlushResponse();

        UNIT_ASSERT(FAILED(response->GetStatus()));
    }

    auto BuildEvGetBreaker(ui32 blockCount, bool& broken) {
        return [blockCount, &broken] (TAutoPtr<IEventHandle>& event) {
            switch (event->GetTypeRewrite()) {
                case TEvBlobStorage::EvGetResult: {
                    auto* msg = event->Get<TEvBlobStorage::TEvGetResult>();
                    ui32 totalSize = 0;
                    for (ui32 i = 0; i < msg->ResponseSz; ++i) {
                        totalSize += msg->Responses[i].Buffer.size();
                    }
                    if (totalSize == blockCount * DefaultBlockSize) {
                        // it's our blob
                        msg->Responses[0].Status = NKikimrProto::NODATA;
                        broken = true;
                    }

                    break;
                }
            }

            return TTestActorRuntime::DefaultObserverFunc(event);
        };
    }

    Y_UNIT_TEST(ShouldReturnErrorIfNODATABlocksAreDetectedInDefaultStorageAccessMode)
    {
        auto runtime = PrepareTestActorRuntime(
            DefaultConfig(),
            1024,
            {},
            TTestPartitionInfo(),
            {},
            EStorageAccessMode::Default
        );

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        bool broken = false;
        runtime->SetObserverFunc(BuildEvGetBreaker(777, broken));

        partition.WriteBlocks(TBlockRange32::WithLength(0, 777), 1);
        partition.SendReadBlocksRequest(TBlockRange32::WithLength(0, 777));
        auto response = partition.RecvReadBlocksResponse();
        UNIT_ASSERT(broken);
        UNIT_ASSERT_VALUES_EQUAL(E_REJECTED, response->GetError().GetCode());
    }

    Y_UNIT_TEST(ShouldMarkNODATABlocksInRepairStorageAccessMode)
    {
        auto runtime = PrepareTestActorRuntime(
            DefaultConfig(),
            1024,
            {},
            TTestPartitionInfo(),
            {},
            EStorageAccessMode::Repair
        );

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        bool broken = false;
        runtime->SetObserverFunc(BuildEvGetBreaker(777, broken));

        partition.WriteBlocks(TBlockRange32::WithLength(0, 777), 1);
        partition.SendReadBlocksRequest(TBlockRange32::WithLength(0, 777));
        auto response = partition.RecvReadBlocksResponse();
        Y_ABORT_UNLESS(broken);
        UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetError().GetCode());

        const auto& blocks = response->Record.GetBlocks();
        UNIT_ASSERT_EQUAL(777, blocks.BuffersSize());
        for (size_t i = 0; i < blocks.BuffersSize(); ++i) {
            UNIT_ASSERT_VALUES_EQUAL(
                GetBrokenDataMarker(),
                blocks.GetBuffers(i)
            );
        }
    }

    Y_UNIT_TEST(ShouldRejectRequestForBlockOutOfRange)
    {
        auto runtime = PrepareTestActorRuntime();

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        {
            partition.WriteBlocks(TBlockRange32::MakeOneBlock(1023));
        }

        {
            partition.SendWriteBlocksRequest(TBlockRange32::WithLength(1023, 2));
            auto response = partition.RecvWriteBlocksResponse();
            UNIT_ASSERT(FAILED(response->GetStatus()));
        }

        {
            partition.SendWriteBlocksRequest(TBlockRange32::MakeOneBlock(1024));
            auto response = partition.RecvWriteBlocksResponse();
            UNIT_ASSERT(FAILED(response->GetStatus()));
        }
    }

    Y_UNIT_TEST(ShouldNotCauseUI32IntergerOverflow)
    {
        auto runtime = PrepareTestActorRuntime(DefaultConfig(), Max<ui32>());

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        {
            auto range = TBlockRange32::MakeOneBlock(Max<ui32>() - 1);
            partition.WriteBlocks(range);
            partition.ReadBlocks(Max<ui32>() - 1);
        }

        {
            auto range =
                TBlockRange32::MakeClosedInterval(Max<ui32>() - 1, Max<ui32>());
            partition.SendWriteBlocksRequest(range);
            auto response = partition.RecvWriteBlocksResponse();
            UNIT_ASSERT(FAILED(response->GetStatus()));
        }

        {
            auto range = TBlockRange32::MakeOneBlock(Max<ui32>() - 1);
            auto blockContent = GetBlockContent(1);
            partition.WriteBlocksLocal(range, blockContent);
            partition.ReadBlocks(Max<ui32>() - 1);
        }

        {
            auto range =
                TBlockRange32::MakeClosedInterval(Max<ui32>() - 1, Max<ui32>());
            auto blockContent = GetBlockContent(1);
            partition.SendWriteBlocksLocalRequest(range, blockContent);
            auto response = partition.RecvWriteBlocksLocalResponse();
            UNIT_ASSERT(FAILED(response->GetStatus()));
        }

        {
            auto range =
                TBlockRange32::MakeClosedInterval(Max<ui32>() - 1, Max<ui32>());
            TVector<TString> blocks;
            auto sglist = ResizeBlocks(
                blocks,
                range.Size(),
                TString::TUninitialized(DefaultBlockSize));
            partition.SendReadBlocksLocalRequest(range, std::move(sglist));
            auto response = partition.RecvReadBlocksLocalResponse();
            UNIT_ASSERT(FAILED(response->GetStatus()));
        }
    }

    Y_UNIT_TEST(ShouldSupportCheckpointOperations)
    {
        auto runtime = PrepareTestActorRuntime(DefaultConfig());

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        partition.CreateCheckpoint("checkpoint1");
        partition.DeleteCheckpoint("checkpoint1");

        partition.CreateCheckpoint("checkpoint1");
        partition.DeleteCheckpoint("checkpoint1");
        partition.DeleteCheckpoint("checkpoint1");

        partition.CreateCheckpoint("checkpoint1", "id1");
        partition.DeleteCheckpoint("checkpoint1", "id1");

        partition.CreateCheckpoint("checkpoint1", "id1");
        partition.CreateCheckpoint("checkpoint1", "id1");
        partition.DeleteCheckpoint("checkpoint1", "id2");

        partition.CreateCheckpoint("checkpoint1", "id1");
        partition.DeleteCheckpoint("checkpoint1", "id2");
        partition.DeleteCheckpoint("checkpoint1", "id2");

        partition.CreateCheckpoint("checkpoint1", "id1");
        partition.DeleteCheckpoint("checkpoint1", "id2");
        partition.DeleteCheckpoint("checkpoint1", "id3");

        partition.CreateCheckpoint("checkpoint1", "id1");
        partition.CreateCheckpoint("checkpoint1", "id2");
        partition.DeleteCheckpoint("checkpoint1", "id2");

        partition.CreateCheckpoint("checkpoint1", "id1");
        partition.DeleteCheckpoint("checkpoint2", "id1");
    }

    Y_UNIT_TEST(ShouldDeleteCheckpointAfterDeleteCheckpointData)
    {
        auto runtime = PrepareTestActorRuntime(DefaultConfig());

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        partition.CreateCheckpoint("checkpoint1");
        partition.DeleteCheckpointData("checkpoint1");

        auto responseDelete = partition.DeleteCheckpoint("checkpoint1");
        UNIT_ASSERT_VALUES_EQUAL(S_OK, responseDelete->GetStatus());
    }

    Y_UNIT_TEST(ShouldDeleteCheckpointAfterDeleteCheckpointDataAndReboot)
    {
        auto runtime = PrepareTestActorRuntime(DefaultConfig());

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        partition.WriteBlocks(1, 1);
        partition.CreateCheckpoint("checkpoint1");
        partition.Flush();
        partition.WriteBlocks(1, 2);

        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(1),
            GetBlockContent(partition.ReadBlocks(1, "checkpoint1"))
        );
        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(2),
            GetBlockContent(partition.ReadBlocks(1))
        );

        partition.CreateCheckpoint("checkpoint1");
        partition.DeleteCheckpointData("checkpoint1");

        partition.SendReadBlocksRequest(1, "checkpoint1");
        auto response = partition.RecvReadBlocksResponse();
        UNIT_ASSERT_VALUES_EQUAL(E_NOT_FOUND, response->GetStatus());

        partition.RebootTablet();
        partition.WaitReady();

        partition.SendReadBlocksRequest(1, "checkpoint1");
        response = partition.RecvReadBlocksResponse();
        UNIT_ASSERT_VALUES_EQUAL(E_NOT_FOUND, response->GetStatus());

        {
            auto responseDelete = partition.DeleteCheckpoint("checkpoint1");
            UNIT_ASSERT_VALUES_EQUAL(S_OK, responseDelete->GetStatus());
        }
        {
            auto responseCreate = partition.CreateCheckpoint("checkpoint1");
            UNIT_ASSERT_VALUES_EQUAL(S_OK, responseCreate->GetStatus());
        }
        {
            auto responseDelete = partition.DeleteCheckpoint("checkpoint1");
            UNIT_ASSERT_VALUES_EQUAL(S_OK, responseDelete->GetStatus());
        }
        {
            partition.CreateCheckpoint("checkpoint1", "id1");
            partition.DeleteCheckpointData("checkpoint1");
            auto responseCreate = partition.CreateCheckpoint("checkpoint1", "id1");
            UNIT_ASSERT_VALUES_EQUAL(S_ALREADY, responseCreate->GetStatus());
        }
    }

    Y_UNIT_TEST(ShouldSupportCheckpointsWithoutData) {
        auto runtime = PrepareTestActorRuntime(DefaultConfig());

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        partition.WriteBlocks(1, 1);
        partition.CreateCheckpoint("checkpoint_without_data", "id1", true);
        partition.Flush();
        partition.WriteBlocks(1, 2);

        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(2),
            GetBlockContent(partition.ReadBlocks(1))
        );

        partition.SendReadBlocksRequest(1, "checkpoint_without_data");
        auto response = partition.RecvReadBlocksResponse();
        UNIT_ASSERT_VALUES_EQUAL(E_NOT_FOUND, response->GetStatus());

        {
            auto responseDelete = partition.DeleteCheckpoint("checkpoint_without_data");
            UNIT_ASSERT_VALUES_EQUAL(S_OK, responseDelete->GetStatus());
        }
        {
            auto responseCreate = partition.CreateCheckpoint("checkpoint1", "id2", true);
            UNIT_ASSERT_VALUES_EQUAL(S_OK, responseCreate->GetStatus());
        }
        {
            auto responseDeleteData =  partition.DeleteCheckpointData("checkpoint1");
            UNIT_ASSERT_VALUES_EQUAL(S_ALREADY, responseDeleteData->GetStatus());
        }
        {
            auto responseCreateSame = partition.CreateCheckpoint("checkpoint1", "id3");
            UNIT_ASSERT_VALUES_EQUAL(S_ALREADY, responseCreateSame->GetStatus());
        }
        {
            auto responseDelete =  partition.DeleteCheckpoint("checkpoint1");
            UNIT_ASSERT_VALUES_EQUAL(S_OK, responseDelete->GetStatus());
        }
    }

    void DoTestFullCompaction(bool forced)
    {
        constexpr ui32 rangesCount = 5;
        auto storageConfig = DefaultConfig();
        storageConfig.SetWriteBlobThreshold(1_MB);
        auto runtime = PrepareTestActorRuntime(storageConfig, rangesCount * 1024);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        for (ui32 range = 0; range < rangesCount; ++range) {
            partition.WriteBlocks(
                TBlockRange32::WithLength(range * 1024, 1024),
                1);
        }
        partition.Flush();

        auto response = partition.StatPartition();
        auto oldStats = response->Record.GetStats();

        TCompactionOptions options;
        options.set(ToBit(ECompactionOption::Full));
        if (forced) {
            options.set(ToBit(ECompactionOption::Forced));
        }
        for (ui32 range = 0; range < rangesCount; ++range) {
            partition.Compaction(range * 1024, options);
        }

        response = partition.StatPartition();
        auto newStats = response->Record.GetStats();
        UNIT_ASSERT_VALUES_EQUAL(
            oldStats.GetMixedBlobsCount() + oldStats.GetMergedBlobsCount() + rangesCount,
            newStats.GetMixedBlobsCount() + newStats.GetMergedBlobsCount()
        );
    }

    Y_UNIT_TEST(ShouldCreateBlobsForEveryWrittenRangeDuringFullCompaction)
    {
        DoTestFullCompaction(false);
    }

    Y_UNIT_TEST(ShouldCreateBlobsForEveryWrittenRangeDuringForcedFullCompaction)
    {
        DoTestFullCompaction(true);
    }

    void DoTestEmptyRangesFullCompaction(bool forced)
    {
        constexpr ui32 rangesCount = 5;
        constexpr ui32 emptyRange = 2;
        auto storageConfig = DefaultConfig();
        storageConfig.SetWriteBlobThreshold(1_MB);
        auto runtime = PrepareTestActorRuntime(storageConfig, rangesCount * 1024);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        for (ui32 range = 0; range < rangesCount; ++range) {
            if (range != emptyRange) {
                partition.WriteBlocks(
                    TBlockRange32::WithLength(range * 1024, 900),
                    1);
            }
        }
        partition.Flush();

        auto response = partition.StatPartition();
        auto oldStats = response->Record.GetStats();

        TCompactionOptions options;
        options.set(ToBit(ECompactionOption::Full));
        if (forced) {
            options.set(ToBit(ECompactionOption::Forced));
        }

        for (ui32 range = 0; range < rangesCount; ++range) {
            partition.Compaction(range * 1024, options);
        }

        response = partition.StatPartition();
        auto newStats = response->Record.GetStats();
        UNIT_ASSERT_VALUES_EQUAL(
            oldStats.GetMixedBlobsCount() + oldStats.GetMergedBlobsCount() + rangesCount - 1,
            newStats.GetMixedBlobsCount() + newStats.GetMergedBlobsCount()
        );
    }

    Y_UNIT_TEST(ShouldNotCreateBlobsForEmptyRangesDuringFullCompaction)
    {
        DoTestEmptyRangesFullCompaction(false);
    }

    Y_UNIT_TEST(ShouldNotCreateBlobsForEmptyRangesDuringForcedFullCompaction)
    {
        DoTestEmptyRangesFullCompaction(true);
    }

    Y_UNIT_TEST(ShouldCorrectlyMarkFirstBlockInBlobIfItIsTheSameAsLastBlockInPreviousBlob)
    {
        auto config = DefaultConfig();
        config.SetCleanupThreshold(1);
        config.SetCollectGarbageThreshold(1);
        config.SetFlushThreshold(8_MB);
        config.SetWriteBlobThreshold(4_MB);

        auto runtime = PrepareTestActorRuntime(config, 2048);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        partition.WriteBlocks(TBlockRange32::WithLength(0,1023), 1);
        for (ui32 i = 0; i < 1024; ++i) {
            partition.WriteBlocks(1023,1);
        }

        // here we expect that fresh contains 2048 blocks
        // flush will write two blobs but latest blob
        // will be immediately gc as it is completely overwritten

        partition.Flush();

        auto response = partition.StatPartition();
        const auto& stats = response->Record.GetStats();
        UNIT_ASSERT_VALUES_EQUAL(1, stats.GetMixedBlobsCount());
    }

    Y_UNIT_TEST(ShouldMakeUnderlyingBlobsEligibleForCleanupAfterCompaction)
    {
        auto config = DefaultConfig();
        config.SetCleanupThreshold(1);
        config.SetCollectGarbageThreshold(3);
        config.SetSSDMaxBlobsPerRange(4);
        config.SetHDDMaxBlobsPerRange(4);
        config.SetFlushThreshold(8_MB);
        config.SetWriteBlobThreshold(4_MB);

        auto runtime = PrepareTestActorRuntime(config, 2048);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        for (ui32 version = 0; version < 3; ++version) {
            for (ui32 pos = 0; pos < 2; ++pos) {
                partition.WriteBlocks(TBlockRange32::WithLength(512 * pos, 512));
            }
            partition.Flush();
        }

        partition.Compaction();
        partition.Cleanup();
        partition.CollectGarbage();

        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(1, stats.GetMergedBlobsCount());
        }
    }

    Y_UNIT_TEST(ShouldCorrectlyMarkCrossRangeBlocksDuringCompaction)
    {
        auto config = DefaultConfig();
        config.SetCleanupThreshold(1);
        config.SetCollectGarbageThreshold(3);
        config.SetSSDMaxBlobsPerRange(4);
        config.SetHDDMaxBlobsPerRange(4);
        config.SetFlushThreshold(8_MB);
        config.SetWriteBlobThreshold(4_MB);

        auto runtime = PrepareTestActorRuntime(config, 2048);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        for (ui32 version = 0; version < 2; ++version) {
            for (ui32 pos = 0; pos < 4; ++pos) {
                partition.WriteBlocks(TBlockRange32::WithLength(512 * pos, 512));
            }
            partition.Flush();
        }

        partition.WriteBlocks(TBlockRange32::MakeClosedInterval(1000, 1500));
        partition.Flush();

        partition.Compaction();
        partition.Compaction();
        partition.Cleanup();
        partition.CollectGarbage();

        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(2, stats.GetMergedBlobsCount());
        }
    }

    Y_UNIT_TEST(ShouldNotWriteBlobIfCompactionRangeWasOverwrittenWithZeroBlocks)
    {
        auto config = DefaultConfig();
        config.SetCleanupThreshold(1);
        config.SetSSDMaxBlobsPerRange(2);
        config.SetHDDMaxBlobsPerRange(2);
        config.SetCollectGarbageThreshold(1);
        config.SetFlushThreshold(8_MB);
        config.SetWriteBlobThreshold(4_MB);

        auto runtime = PrepareTestActorRuntime(config, 2048);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        for (ui32 write_step = 0; write_step < 2; ++write_step) {
            partition.WriteBlocks(TBlockRange32::WithLength(0,512));
        }

        partition.Flush();

        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(1, stats.GetMixedBlobsCount());
        }

        for (ui32 zero_step = 0; zero_step < 2; ++zero_step) {
            partition.ZeroBlocks(TBlockRange32::WithLength(0,512));
        }

        bool writeBlobSeen = false;
        runtime->SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) {
                switch (event->GetTypeRewrite()) {
                    case TEvPartitionPrivate::EvWriteBlobRequest: {
                        writeBlobSeen = true;
                        break;
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        partition.Compaction();
        partition.Cleanup();
        partition.CollectGarbage();

        UNIT_ASSERT(!writeBlobSeen);
        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetMixedBlobsCount());
        }
    }

    Y_UNIT_TEST(ShouldWriteBlobIfCompactionUsesOnlyFreshBlocks)
    {
        auto config = DefaultConfig();
        config.SetCleanupThreshold(3);
        config.SetHDDMaxBlobsPerRange(2);
        config.SetSSDMaxBlobsPerRange(2);
        config.SetCollectGarbageThreshold(3);
        config.SetFlushThreshold(8_MB);
        config.SetWriteBlobThreshold(4_MB);

        auto runtime = PrepareTestActorRuntime(config, 2048);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        for (ui32 write_step = 0; write_step < 2; ++write_step) {
            partition.WriteBlocks(TBlockRange32::WithLength(0,512));
        }

        partition.Flush();

        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(1, stats.GetMixedBlobsCount());
        }

        for (ui32 write_step = 0; write_step < 2; ++write_step) {
            partition.WriteBlocks(TBlockRange32::WithLength(0,512));
        }

        bool writeBlobSeen = false;
        runtime->SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) {
                switch (event->GetTypeRewrite()) {
                    case TEvPartitionPrivate::EvWriteBlobRequest: {
                        writeBlobSeen = true;
                        break;
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        partition.Compaction();

        UNIT_ASSERT(writeBlobSeen);
        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(1, stats.GetMixedBlobsCount());
            UNIT_ASSERT_VALUES_EQUAL(1, stats.GetMergedBlobsCount());
        }
    }

    Y_UNIT_TEST(ShouldCorrectlyHandleZeroAndNonZeroFreshBlobsDuringFlush)
    {
        auto config = DefaultConfig();
        config.SetFlushThreshold(12_KB);
        config.SetFlushBlobSizeThreshold(8_KB);

        auto runtime = PrepareTestActorRuntime(config, 2048);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        partition.WriteBlocks(0, 1);
        partition.ZeroBlocks(0);
        partition.WriteBlocks(0, 1);

        partition.Flush();

        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(1, stats.GetMixedBlobsCount());
        }
    }

    Y_UNIT_TEST(ShouldRejectWriteRequestsIfDataChannelsAreYellow)
    {
        auto config = DefaultConfig();

        auto runtime = PrepareTestActorRuntime(config, 2048);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        runtime->SetObserverFunc(StorageStateChanger(
            NKikimrBlobStorage::StatusDiskSpaceLightYellowMove |
            NKikimrBlobStorage::StatusDiskSpaceYellowStop));

        partition.WriteBlocks(TBlockRange32::WithLength(0, 1024));

        {
            partition.SendWriteBlocksRequest(TBlockRange32::WithLength(0, 1024));
            auto response = partition.RecvWriteBlocksResponse();
            UNIT_ASSERT(FAILED(response->GetStatus()));
        }
    }

    Y_UNIT_TEST(ShouldCorrectlyTrackYellowChannelsInBackground)
    {
        const auto channelCount = 6;
        const auto groupCount = channelCount - DataChannelOffset;

        TTestEnv env(0, 1, channelCount, groupCount);
        auto& runtime = env.GetRuntime();
        auto tabletId = InitTestActorRuntime(env, runtime, channelCount, channelCount);

        TPartitionClient partition(runtime, 0, tabletId);
        partition.WaitReady();

        // one data channel yellow => ok
        {
            auto request =
                std::make_unique<TEvTablet::TEvCheckBlobstorageStatusResult>(
                    TVector<ui32>({
                        env.GetGroupIds()[1],
                    }),
                    TVector<ui32>({
                        env.GetGroupIds()[1],
                    })
                );
            partition.SendToPipe(std::move(request));
        }

        runtime.DispatchEvents({},  TDuration::Seconds(1));

        {
            partition.SendWriteBlocksRequest(TBlockRange32::WithLength(0, 1024));
            auto response = partition.RecvWriteBlocksResponse();
            UNIT_ASSERT(SUCCEEDED(response->GetStatus()));
        }

        // all channels yellow => failure
        {
            auto request =
                std::make_unique<TEvTablet::TEvCheckBlobstorageStatusResult>(
                    TVector<ui32>({
                        env.GetGroupIds()[0],
                        env.GetGroupIds()[1],
                    }),
                    TVector<ui32>({
                        env.GetGroupIds()[0],
                        env.GetGroupIds()[1],
                    })
                );
            partition.SendToPipe(std::move(request));
        }

        runtime.DispatchEvents({},  TDuration::Seconds(1));

        {
            partition.SendWriteBlocksRequest(TBlockRange32::WithLength(0, 1024));
            auto response = partition.RecvWriteBlocksResponse();
            UNIT_ASSERT(FAILED(response->GetStatus()));
        }

        // channels returned to non-yellow state => ok
        {
            auto request =
                std::make_unique<TEvTablet::TEvCheckBlobstorageStatusResult>(
                    TVector<ui32>(),
                    TVector<ui32>()
                );
            partition.SendToPipe(std::move(request));
        }

        runtime.DispatchEvents({},  TDuration::Seconds(1));

        {
            partition.SendWriteBlocksRequest(TBlockRange32::WithLength(0, 1024));
            auto response = partition.RecvWriteBlocksResponse();
            UNIT_ASSERT(SUCCEEDED(response->GetStatus()));
        }

        // TODO: the state may be neither yellow nor green (e.g. orange) -
        // this case is not supported in the background check, but should be
    }

    Y_UNIT_TEST(ShouldValidateMaxChangedBlocksRangeBlocksCount)
    {
        auto runtime = PrepareTestActorRuntime(DefaultConfig(), 1 << 21);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        {
            partition.SendGetChangedBlocksRequest(
                TBlockRange32::WithLength(0, 1 << 21),
                "",
                "",
                false);

            UNIT_ASSERT_VALUES_EQUAL(
                E_ARGUMENT,
                partition.RecvGetChangedBlocksResponse()->GetError().GetCode());
        }

        {
            auto response = partition.GetChangedBlocks(
                TBlockRange32::WithLength(0, 1 << 20),
                "",
                "",
                false);

            UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());
        }
    }

    Y_UNIT_TEST(ShouldSeeChangedFreshBlocksInChangedBlocksRequest)
    {
        auto config = DefaultConfig();

        auto runtime = PrepareTestActorRuntime(config, 2048);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        partition.WriteBlocks(0,1);
        partition.WriteBlocks(1,1);
        partition.CreateCheckpoint("cp1");

        partition.WriteBlocks(1,2);
        partition.WriteBlocks(2,2);
        partition.CreateCheckpoint("cp2");

        auto response = partition.GetChangedBlocks(
            TBlockRange32::WithLength(0, 1024),
            "cp1",
            "cp2",
            false);

        UNIT_ASSERT_VALUES_EQUAL(128, response->Record.GetMask().size());
        UNIT_ASSERT_VALUES_EQUAL(6, response->Record.GetMask()[0]);
    }

    Y_UNIT_TEST(ShouldSeeChangedMergedBlocksInChangedBlocksRequest)
    {
        auto config = DefaultConfig();

        auto runtime = PrepareTestActorRuntime(config, 4096);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        partition.WriteBlocks(TBlockRange32::WithLength(0, 1024), 1);
        partition.WriteBlocks(TBlockRange32::WithLength(1024, 1024), 1);
        partition.CreateCheckpoint("cp1");

        partition.WriteBlocks(TBlockRange32::WithLength(1024,1024), 2);
        partition.WriteBlocks(TBlockRange32::WithLength(2048,1024), 2);
        partition.CreateCheckpoint("cp2");

        auto response = partition.GetChangedBlocks(
            TBlockRange32::WithLength(0, 3072),
            "cp1",
            "cp2",
            false);

        const auto& mask = response->Record.GetMask();
        UNIT_ASSERT_VALUES_EQUAL(384, mask.size());
        AssertEqual(
            TVector<ui8>(256, 255),
            TVector<ui8>(mask.begin() + 128, mask.end())
        );
    }

    Y_UNIT_TEST(ShouldUseZeroCommitIdWhenLowCheckpointIdIsNotSet)
    {
        auto config = DefaultConfig();

        auto runtime = PrepareTestActorRuntime(config, 4096);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        partition.WriteBlocks(0, 1);
        partition.CreateCheckpoint("cp1");
        partition.WriteBlocks(1, 1);
        partition.CreateCheckpoint("cp2");

        {
            auto response = partition.GetChangedBlocks(
                TBlockRange32::WithLength(0, 3072),
                "cp1",
                "cp2",
                false);

            const auto& mask = response->Record.GetMask();
            UNIT_ASSERT_VALUES_EQUAL(384, mask.size());
            UNIT_ASSERT_VALUES_EQUAL(2, response->Record.GetMask()[0]);
        }
        {
            auto response = partition.GetChangedBlocks(
                TBlockRange32::WithLength(0, 3072),
                "",
                "cp2",
                false);

            const auto& mask = response->Record.GetMask();
            UNIT_ASSERT_VALUES_EQUAL(384, mask.size());
            UNIT_ASSERT_VALUES_EQUAL(3, response->Record.GetMask()[0]);
        }
    }

    Y_UNIT_TEST(ShouldUseMostRecentCommitIdWhenHighCheckpointIdIsNotSet)
    {
        auto config = DefaultConfig();

        auto runtime = PrepareTestActorRuntime(config, 4096);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        partition.WriteBlocks(0, 1);
        partition.WriteBlocks(1, 1);
        partition.CreateCheckpoint("cp1");

        partition.WriteBlocks(1, 1);

        partition.CreateCheckpoint("cp2");

        partition.WriteBlocks(2, 1);

        {
            auto response = partition.GetChangedBlocks(
                TBlockRange32::WithLength(0, 3072),
                "cp1",
                "cp2",
                false);

            const auto& mask = response->Record.GetMask();
            UNIT_ASSERT_VALUES_EQUAL(384, mask.size());
            UNIT_ASSERT_VALUES_EQUAL(2, response->Record.GetMask()[0]);
        }

        {
            auto response = partition.GetChangedBlocks(
                TBlockRange32::WithLength(0, 3072),
                "cp1",
                "",
                false);

            const auto& mask = response->Record.GetMask();
            UNIT_ASSERT_VALUES_EQUAL(384, mask.size());
            UNIT_ASSERT_VALUES_EQUAL(6, response->Record.GetMask()[0]);
        }
    }

    Y_UNIT_TEST(ShouldCorrectlyHandleStartBlockInChangedBlocksRequest)
    {
        auto config = DefaultConfig();

        auto runtime = PrepareTestActorRuntime(config, 4096);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        partition.WriteBlocks(0,1);
        partition.CreateCheckpoint("cp1");

        partition.WriteBlocks(0,1);
        partition.WriteBlocks(1024,1);
        partition.CreateCheckpoint("cp2");

        {
            auto response = partition.GetChangedBlocks(
                TBlockRange32::WithLength(0, 1024),
                "cp1",
                "cp2",
                false);

            const auto& mask = response->Record.GetMask();
            UNIT_ASSERT_VALUES_EQUAL(128, mask.size());
            UNIT_ASSERT_VALUES_EQUAL(1, mask[0]);
            AssertEqual(
                TVector<ui8>(127, 0),
                TVector<ui8>(mask.begin() + 1, mask.end())
            );
        }

        {
            auto response = partition.GetChangedBlocks(
                TBlockRange32::WithLength(1024, 1024),
                "cp1",
                "cp2",
                false);

            const auto& mask = response->Record.GetMask();
            UNIT_ASSERT_VALUES_EQUAL(128, mask.size());
            UNIT_ASSERT_VALUES_EQUAL(1, mask[0]);
            AssertEqual(
                TVector<ui8>(127, 0),
                TVector<ui8>(mask.begin() + 1, mask.end())
            );
        }

        {
            auto response = partition.GetChangedBlocks(
                TBlockRange32::WithLength(0, 2048),
                "cp1",
                "cp2",
                false);

            const auto& mask = response->Record.GetMask();
            UNIT_ASSERT_VALUES_EQUAL(256, mask.size());

            UNIT_ASSERT_VALUES_EQUAL(mask[0], 1);
            AssertEqual(
                TVector<ui8>(127, 0),
                TVector<ui8>(mask.begin() + 1, mask.begin() + 128)
            );

            UNIT_ASSERT_VALUES_EQUAL(mask[128], 1);
            AssertEqual(
                TVector<ui8>(127, 0),
                TVector<ui8>(mask.begin() + 129, mask.end())
            );
        }
    }

    Y_UNIT_TEST(ShouldCorrectlyGetChangedBlocksForCheckpointWithoutData)
    {
        auto config = DefaultConfig();

        auto runtime = PrepareTestActorRuntime(config, 2048);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        auto checkChangedBlockMask = [&] (
            const TString& firstCheckpoint,
            const TString& secondCheckpoint,
            const ui8 maskValue)
        {
            auto response = partition.GetChangedBlocks(
                TBlockRange32::WithLength(0, 1024),
                firstCheckpoint,
                secondCheckpoint,
                false);

            UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());
            UNIT_ASSERT_VALUES_EQUAL(128, response->Record.GetMask().size());
            UNIT_ASSERT_VALUES_EQUAL(maskValue, response->Record.GetMask()[0]);
        };

        partition.WriteBlocks(0,1);
        partition.WriteBlocks(1,1);
        partition.CreateCheckpoint("cp1");

        partition.WriteBlocks(1,2);
        partition.WriteBlocks(2,2);

        partition.DeleteCheckpointData("cp1");
        partition.CreateCheckpoint("cp2");

        checkChangedBlockMask("cp1", "cp2", 0b00000110);

        partition.WriteBlocks(3, 3);
        partition.WriteBlocks(4, 3);
        partition.CreateCheckpoint("cp3", "", true);  // cp3 withoutData

        checkChangedBlockMask("cp1", "cp3", 0b00011110);
        checkChangedBlockMask("cp2", "cp3", 0b00011000);

        partition.WriteBlocks(4, 4);
        partition.WriteBlocks(5, 4);
        partition.WriteBlocks(2, 4);

        // block 4 has been overwritten (cp3 without data, don't hold block4)
        // block 2 also has been overwritten, but cp2 - with data, it holds block2
        checkChangedBlockMask("cp1", "cp3", 0b00001110);
        checkChangedBlockMask("cp2", "cp3", 0b00001000);

        partition.CreateCheckpoint("cp4", "", true);  // cp4 withoutData

        checkChangedBlockMask("cp1", "cp4", 0b00111110);
        checkChangedBlockMask("cp2", "cp4", 0b00111100);
        checkChangedBlockMask("cp3", "cp4", 0b00110100);
    }

    Y_UNIT_TEST(ShouldCorrectlyGetChangedBlocksForOverlayDisk)
    {
        TPartitionContent baseContent = {
        /*|      0      |     1     |     2 ... 5    |     6     |      7      |     8     |      9      |*/
            TBlob(1, 1) , TFresh(2) , TBlob(2, 3, 4) ,  TEmpty() , TBlob(1, 4) , TFresh(5) , TBlob(2, 6)
        };

        auto partitionWithRuntime =
            SetupOverlayPartition(TestTabletId, TestTabletId2, baseContent);

        auto& partition = *partitionWithRuntime.Partition;
        partition.WaitReady();

        {
            auto response = partition.GetChangedBlocks(
                TBlockRange32::WithLength(0, 1024),
                "",
                "",
                false);

            UNIT_ASSERT_VALUES_EQUAL(128, response->Record.GetMask().size());
            UNIT_ASSERT_VALUES_EQUAL(char(0b10111111), response->Record.GetMask()[0]);
            UNIT_ASSERT_VALUES_EQUAL(char(0b00000011), response->Record.GetMask()[1]);
        }
        {
            auto response = partition.GetChangedBlocks(
                TBlockRange32::WithLength(0, 1024),
                "",
                "",
                true);

            UNIT_ASSERT_VALUES_EQUAL(128, response->Record.GetMask().size());
            UNIT_ASSERT_VALUES_EQUAL(0, response->Record.GetMask()[0]);
            UNIT_ASSERT_VALUES_EQUAL(0, response->Record.GetMask()[1]);
        }

        partition.WriteBlocks(0, 1);
        partition.WriteBlocks(1, 1);
        partition.CreateCheckpoint("cp1");

        partition.WriteBlocks(1, 2);
        partition.WriteBlocks(2, 2);
        partition.CreateCheckpoint("cp2");

        {
            auto response = partition.GetChangedBlocks(
                TBlockRange32::WithLength(0, 1024),
                "cp1",
                "cp2",
                false);

            UNIT_ASSERT_VALUES_EQUAL(128, response->Record.GetMask().size());
            UNIT_ASSERT_VALUES_EQUAL(char(0b00000110), response->Record.GetMask()[0]);
            UNIT_ASSERT_VALUES_EQUAL(0, response->Record.GetMask()[1]);
        }
        {
            auto response = partition.GetChangedBlocks(
                TBlockRange32::WithLength(0, 1024),
                "",
                "cp2",
                false);

            UNIT_ASSERT_VALUES_EQUAL(128, response->Record.GetMask().size());
            UNIT_ASSERT_VALUES_EQUAL(
                char(0b10111111),
                response->Record.GetMask()[0]
            );
            UNIT_ASSERT_VALUES_EQUAL(
                char(0b00000011),
                response->Record.GetMask()[1]
            );
        }
        {
            auto response = partition.GetChangedBlocks(
                TBlockRange32::WithLength(0, 1024),
                "",
                "cp1",
                true);

            UNIT_ASSERT_VALUES_EQUAL(128, response->Record.GetMask().size());
            UNIT_ASSERT_VALUES_EQUAL(char(0b00000011), response->Record.GetMask()[0]);
            UNIT_ASSERT_VALUES_EQUAL(0, response->Record.GetMask()[1]);
        }

    }

    Y_UNIT_TEST(ShouldCorrectlyGetChangedBlocksWhenRangeIsOutOfBounds)
    {
        auto runtime = PrepareTestActorRuntime(DefaultConfig(), 10);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        partition.WriteBlocks(1, 1);
        partition.WriteBlocks(2, 1);
        partition.CreateCheckpoint("cp");

        {
            auto response = partition.GetChangedBlocks(
                TBlockRange32::MakeClosedInterval(10, 1023),
                "",
                "cp",
                false);
            UNIT_ASSERT_VALUES_EQUAL(0, response->Record.GetMask().size());
        }

        {
            auto response = partition.GetChangedBlocks(
                TBlockRange32::WithLength(0, 1024),
                "",
                "cp",
                false);

            UNIT_ASSERT_VALUES_EQUAL(2, response->Record.GetMask().size());
            UNIT_ASSERT_VALUES_EQUAL(
                char(0b00000110),
                response->Record.GetMask()[0]
            );
            UNIT_ASSERT_VALUES_EQUAL(
                char(0b00000000),
                response->Record.GetMask()[1]
            );
        }
    }

    Y_UNIT_TEST(ShouldConsolidateZeroedBlocksOnCompaction)
    {
        auto config = DefaultConfig();
        config.SetWriteBlobThreshold(1);   // disable FreshBlocks

        auto runtime = PrepareTestActorRuntime(config);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        partition.WriteBlocks(TBlockRange32::WithLength(0, 501));
        partition.WriteBlocks(TBlockRange32::MakeClosedInterval(501, 1000));
        partition.ZeroBlocks(TBlockRange32::MakeClosedInterval(501, 1000));

        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetMixedBlobsCount());
            UNIT_ASSERT_VALUES_EQUAL(3, stats.GetMergedBlobsCount());
        }

        partition.Compaction();
        partition.Cleanup();
        partition.CollectGarbage();

        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetMixedBlobsCount());
            UNIT_ASSERT_VALUES_EQUAL(2, stats.GetMergedBlobsCount());
        }

        partition.ZeroBlocks(TBlockRange32::WithLength(0, 1001));

        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetMixedBlobsCount());
            UNIT_ASSERT_VALUES_EQUAL(3, stats.GetMergedBlobsCount());
        }

        partition.Compaction();
        partition.Cleanup();
        partition.CollectGarbage();

        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetMixedBlobsCount());
            UNIT_ASSERT_VALUES_EQUAL(1, stats.GetMergedBlobsCount());
        }
    }

    Y_UNIT_TEST(ShouldProduceSparseMergedBlobsUponCompaction)
    {
        auto config = DefaultConfig();
        config.SetWriteBlobThreshold(1);   // disable FreshBlocks
        config.SetSSDMaxBlobsPerRange(999);
        config.SetHDDMaxBlobsPerRange(999);

        auto runtime = PrepareTestActorRuntime(config);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        partition.WriteBlocks(5, 1);
        partition.WriteBlocks(10, 2);
        partition.WriteBlocks(20, 3);
        partition.WriteBlocks(30, 4);
        partition.ZeroBlocks(30);
        partition.ZeroBlocks(40);

        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetMixedBlobsCount());
            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetMixedBlocksCount());
            UNIT_ASSERT_VALUES_EQUAL(6, stats.GetMergedBlobsCount());
            UNIT_ASSERT_VALUES_EQUAL(4, stats.GetMergedBlocksCount());
        }

        partition.Compaction();
        partition.Cleanup();
        partition.CollectGarbage();

        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetMixedBlobsCount());
            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetMixedBlocksCount());
            UNIT_ASSERT_VALUES_EQUAL(2, stats.GetMergedBlobsCount());
            UNIT_ASSERT_VALUES_EQUAL(3, stats.GetMergedBlocksCount());
        }

        UNIT_ASSERT_VALUES_EQUAL(
            TStringBuf(),
            GetBlocksContent(partition.ReadBlocks(4))
        );
        UNIT_ASSERT_VALUES_EQUAL(
            GetBlocksContent(1),
            GetBlocksContent(partition.ReadBlocks(5))
        );
        UNIT_ASSERT_VALUES_EQUAL(
            TStringBuf(),
            GetBlocksContent(partition.ReadBlocks(6))
        );

        UNIT_ASSERT_VALUES_EQUAL(
            TStringBuf(),
            GetBlocksContent(partition.ReadBlocks(9))
        );
        UNIT_ASSERT_VALUES_EQUAL(
            GetBlocksContent(2),
            GetBlocksContent(partition.ReadBlocks(10))
        );
        UNIT_ASSERT_VALUES_EQUAL(
            TStringBuf(),
            GetBlocksContent(partition.ReadBlocks(11))
        );

        UNIT_ASSERT_VALUES_EQUAL(
            TStringBuf(),
            GetBlocksContent(partition.ReadBlocks(19))
        );
        UNIT_ASSERT_VALUES_EQUAL(
            GetBlocksContent(3),
            GetBlocksContent(partition.ReadBlocks(20))
        );
        UNIT_ASSERT_VALUES_EQUAL(
            TStringBuf(),
            GetBlocksContent(partition.ReadBlocks(21))
        );

        UNIT_ASSERT_VALUES_EQUAL(
            TStringBuf(),
            GetBlocksContent(partition.ReadBlocks(30))
        );

        UNIT_ASSERT_VALUES_EQUAL(
            TStringBuf(),
            GetBlocksContent(partition.ReadBlocks(40))
        );
    }

    void DoTestIncrementalCompaction(
        NProto::TStorageServiceConfig config,
        bool incrementalCompactionExpected)
    {
        config.SetWriteBlobThreshold(1);   // disable FreshBlocks
        config.SetIncrementalCompactionEnabled(true);
        config.SetHDDMaxBlobsPerRange(4);
        config.SetSSDMaxBlobsPerRange(4);
        config.SetMaxSkippedBlobsDuringCompaction(1);
        config.SetTargetCompactionBytesPerOp(64_KB);

        auto runtime = PrepareTestActorRuntime(config);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        std::unique_ptr<IEventHandle> compactionRequest;
        bool intercept = true;

        runtime->SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) {
                switch (event->GetTypeRewrite()) {
                    case TEvPartitionPrivate::EvCompactionRequest: {
                        if (intercept) {
                            auto request =
                                event->Get<TEvPartitionPrivate::TEvCompactionRequest>();
                            UNIT_ASSERT_VALUES_EQUAL(
                                incrementalCompactionExpected,
                                !request->CompactionOptions.test(
                                    ToBit(ECompactionOption::Full))
                            );

                            compactionRequest.reset(event.Release());
                            intercept = false;

                            return TTestActorRuntime::EEventAction::DROP;
                        }

                        break;
                    }
                }

                return TTestActorRuntime::DefaultObserverFunc(event);
            }
        );

        partition.WriteBlocks(TBlockRange32::WithLength(0, 1024), 1);
        partition.WriteBlocks(TBlockRange32::WithLength(0, 5), 2);
        partition.WriteBlocks(TBlockRange32::MakeClosedInterval(10, 14), 3);
        partition.WriteBlocks(TBlockRange32::MakeClosedInterval(20, 25), 4);

        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetMixedBlobsCount());
            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetMixedBlocksCount());
            UNIT_ASSERT_VALUES_EQUAL(4, stats.GetMergedBlobsCount());
            UNIT_ASSERT_VALUES_EQUAL(1040, stats.GetMergedBlocksCount());
        }

        runtime->DispatchEvents({}, TDuration::Seconds(1));
        UNIT_ASSERT(compactionRequest);
        runtime->Send(compactionRequest.release());
        runtime->DispatchEvents({}, TDuration::Seconds(1));
        partition.Cleanup();
        partition.CollectGarbage();

        {
            ui32 blobs = incrementalCompactionExpected ? 2 : 1;
            ui32 blocks = incrementalCompactionExpected ? 1040 : 1024;

            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetMixedBlobsCount());
            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetMixedBlocksCount());
            UNIT_ASSERT_VALUES_EQUAL(blobs, stats.GetMergedBlobsCount());
            UNIT_ASSERT_VALUES_EQUAL(blocks, stats.GetMergedBlocksCount());
        }

        for (ui32 i = 0; i <= 4; ++i) {
            UNIT_ASSERT_VALUES_EQUAL(
                GetBlocksContent(2),
                GetBlocksContent(partition.ReadBlocks(i))
            );
        }

        for (ui32 i = 5; i < 10; ++i) {
            UNIT_ASSERT_VALUES_EQUAL(
                GetBlocksContent(1),
                GetBlocksContent(partition.ReadBlocks(i))
            );
        }

        for (ui32 i = 10; i <= 14; ++i) {
            UNIT_ASSERT_VALUES_EQUAL(
                GetBlocksContent(3),
                GetBlocksContent(partition.ReadBlocks(i))
            );
        }

        for (ui32 i = 15; i < 20; ++i) {
            UNIT_ASSERT_VALUES_EQUAL(
                GetBlocksContent(1),
                GetBlocksContent(partition.ReadBlocks(i))
            );
        }

        for (ui32 i = 20; i <= 25; ++i) {
            UNIT_ASSERT_VALUES_EQUAL(
                GetBlocksContent(4),
                GetBlocksContent(partition.ReadBlocks(i))
            );
        }

        for (ui32 i = 26; i < 1023; ++i) {
            UNIT_ASSERT_VALUES_EQUAL(
                GetBlocksContent(1),
                GetBlocksContent(partition.ReadBlocks(i))
            );
        }
    }

    Y_UNIT_TEST(ShouldCompactIncrementally)
    {
        auto config = DefaultConfig();
        config.SetCompactionGarbageThreshold(20);
        DoTestIncrementalCompaction(std::move(config), true);
    }

    Y_UNIT_TEST(ShouldNotCompactIncrementallyIfDiskGarbageLevelIsTooHigh)
    {
        auto config = DefaultConfig();
        config.SetCompactionGarbageThreshold(1);
        DoTestIncrementalCompaction(std::move(config), false);
    }

    Y_UNIT_TEST(ShouldNotEraseBlocksForSkippedBlobsFromIndexDuringIncrementalCompaction)
    {
        auto config = DefaultConfig();
        config.SetWriteBlobThreshold(1_GB);   // everything goes to fresh
        config.SetIncrementalCompactionEnabled(true);
        config.SetHDDMaxBlobsPerRange(999);
        config.SetSSDMaxBlobsPerRange(999);
        config.SetMaxSkippedBlobsDuringCompaction(1);
        config.SetTargetCompactionBytesPerOp(1);

        auto runtime = PrepareTestActorRuntime(config);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        // blob 1 needs to eventually have more live blocks than blob 2 in order
        // not to be compacted
        partition.WriteBlocks(TBlockRange32::MakeClosedInterval(2, 22), 1);
        partition.Flush();
        partition.WriteBlocks(TBlockRange32::MakeClosedInterval(1, 10), 2);
        partition.Flush();

        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(2, stats.GetMixedBlobsCount());
            UNIT_ASSERT_VALUES_EQUAL(31, stats.GetMixedBlocksCount());
            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetMergedBlobsCount());
            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetMergedBlocksCount());
        }

        partition.Compaction();
        partition.Cleanup();
        partition.CollectGarbage();

        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(1, stats.GetMixedBlobsCount());
            UNIT_ASSERT_VALUES_EQUAL(21, stats.GetMixedBlocksCount());
            UNIT_ASSERT_VALUES_EQUAL(1, stats.GetMergedBlobsCount());
            UNIT_ASSERT_VALUES_EQUAL(10, stats.GetMergedBlocksCount());
        }

        for (ui32 i = 11; i <= 22; ++i) {
            UNIT_ASSERT_VALUES_EQUAL(
                GetBlocksContent(1),
                GetBlocksContent(partition.ReadBlocks(i))
            );
        }

        for (ui32 i = 1; i <= 10; ++i) {
            UNIT_ASSERT_VALUES_EQUAL(
                GetBlocksContent(2),
                GetBlocksContent(partition.ReadBlocks(i))
            );
        }
    }

    Y_UNIT_TEST(ShouldProperlyProcessZeroesDuringIncrementalCompaction)
    {
        auto config = DefaultConfig();
        config.SetWriteBlobThreshold(1);    // disabling fresh
        config.SetIncrementalCompactionEnabled(true);
        config.SetHDDMaxBlobsPerRange(999);
        config.SetSSDMaxBlobsPerRange(999);
        config.SetMaxSkippedBlobsDuringCompaction(1);
        config.SetTargetCompactionBytesPerOp(1);

        auto runtime = PrepareTestActorRuntime(config);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        // writing some data
        partition.WriteBlocks(TBlockRange32::WithLength(0, 11), 1);
        // zeroing it
        partition.ZeroBlocks(TBlockRange32::WithLength(0, 11));
        // writing some more data above
        partition.WriteBlocks(TBlockRange32::MakeClosedInterval(1, 12), 2);

        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetMixedBlobsCount());
            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetMixedBlocksCount());
            UNIT_ASSERT_VALUES_EQUAL(3, stats.GetMergedBlobsCount());
            UNIT_ASSERT_VALUES_EQUAL(23, stats.GetMergedBlocksCount());
        }

        partition.Compaction();
        partition.Cleanup();
        partition.CollectGarbage();

        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetMixedBlobsCount());
            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetMixedBlocksCount());
            UNIT_ASSERT_VALUES_EQUAL(2, stats.GetMergedBlobsCount());
            UNIT_ASSERT_VALUES_EQUAL(12, stats.GetMergedBlocksCount());
        }

        UNIT_ASSERT_VALUES_EQUAL(
            TStringBuf(),
            GetBlocksContent(partition.ReadBlocks(0))
        );

        for (ui32 i = 1; i <= 12; ++i) {
            UNIT_ASSERT_VALUES_EQUAL(
                GetBlocksContent(2),
                GetBlocksContent(partition.ReadBlocks(i))
            );
        }
    }

    Y_UNIT_TEST(ShouldRejectCompactionRequestsIfDataChannelsAreAlmostFull)
    {
        // smoke test: tests that the compaction mechanism checks partition state
        // detailed tests are located in part_state_ut.cpp
        auto runtime = PrepareTestActorRuntime();

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        runtime->SetObserverFunc(
            StorageStateChanger(NKikimrBlobStorage::StatusDiskSpaceLightYellowMove));
        partition.WriteBlocks(TBlockRange32::WithLength(0, 1024));

        {
            auto response = partition.Compaction();
            UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetError().GetCode());
        }

        const auto badFlags = {
            NKikimrBlobStorage::StatusDiskSpaceOrange,
            NKikimrBlobStorage::StatusDiskSpaceRed,
        };

        for (const auto flag: badFlags) {
            partition.RebootTablet();

            runtime->SetObserverFunc(StorageStateChanger(flag));
            partition.WriteBlocks(TBlockRange32::WithLength(0, 1024));

            auto request = partition.CreateCompactionRequest();
            partition.SendToPipe(std::move(request));

            auto response = partition.RecvResponse<TEvPartitionPrivate::TEvCompactionResponse>();
            UNIT_ASSERT_VALUES_EQUAL(E_BS_OUT_OF_SPACE, response->GetError().GetCode());
        }
    }

    Y_UNIT_TEST(ShouldReassignNonwritableTabletChannels)
    {
        const auto channelCount = 6;
        const auto groupCount = channelCount - DataChannelOffset;
        const auto reassignTimeout = TDuration::Minutes(1);

        TTestEnv env(0, 1, channelCount, groupCount);
        auto& runtime = env.GetRuntime();

        NMonitoring::TDynamicCountersPtr counters
            = new NMonitoring::TDynamicCounters();
        InitCriticalEventsCounter(counters);
        auto reassignCounter =
            counters->GetCounter("AppCriticalEvents/ReassignTablet", true);

        NProto::TStorageServiceConfig config;
        config.SetReassignRequestRetryTimeout(reassignTimeout.MilliSeconds());
        const auto tabletId = InitTestActorRuntime(
            env,
            runtime,
            channelCount,
            channelCount,
            config);

        TPartitionClient partition(runtime, 0, tabletId);
        partition.WaitReady();

        ui64 reassignedTabletId = 0;
        TVector<ui32> channels;
        ui32 ssflags = ui32(
            NKikimrBlobStorage::StatusDiskSpaceYellowStop |
            NKikimrBlobStorage::StatusDiskSpaceLightYellowMove);

        runtime.SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) {
                switch (event->GetTypeRewrite()) {
                    case TEvHiveProxy::EvReassignTabletRequest: {
                        auto* msg = event->Get<TEvHiveProxy::TEvReassignTabletRequest>();
                        reassignedTabletId = msg->TabletId;
                        channels = msg->Channels;

                        break;
                    }
                }

                return StorageStateChanger(ssflags, env.GetGroupIds()[1])(event);
            }
        );

        // first request does not trigger a reassign event - data is written to
        // the first group
        partition.WriteBlocks(TBlockRange32::WithLength(0, 1024));
        // second request succeeds but triggers a reassign event - yellow flag
        // is received in the response for this request
        partition.WriteBlocks(TBlockRange32::WithLength(0, 1024));

        UNIT_ASSERT_VALUES_EQUAL(1, reassignCounter->Val());

        UNIT_ASSERT_VALUES_EQUAL(tabletId, reassignedTabletId);
        UNIT_ASSERT_VALUES_EQUAL(1, channels.size());
        UNIT_ASSERT_VALUES_EQUAL(4, channels.front());

        reassignedTabletId = 0;
        channels.clear();

        {
            // third request doesn't trigger a reassign event because we can
            // still write (some data channels are not yellow yet)
            auto request = partition.CreateWriteBlocksRequest(
                TBlockRange32::WithLength(0, 1024));
            partition.SendToPipe(std::move(request));

            auto response =
                partition.RecvResponse<TEvService::TEvWriteBlocksResponse>();
            UNIT_ASSERT_VALUES_EQUAL(
                S_OK,
                response->GetError().GetCode()
            );
        }

        // checking that a reassign request has not been sent
        UNIT_ASSERT_VALUES_EQUAL(1, reassignCounter->Val());

        UNIT_ASSERT_VALUES_EQUAL(0, reassignedTabletId);
        UNIT_ASSERT_VALUES_EQUAL(0, channels.size());

        reassignedTabletId = 0;
        channels.clear();

        {
            // informing partition tablet that the first group is yellow
            auto request =
                std::make_unique<TEvTablet::TEvCheckBlobstorageStatusResult>(
                    TVector<ui32>({env.GetGroupIds()[0], env.GetGroupIds()[1]}),
                    TVector<ui32>({env.GetGroupIds()[0], env.GetGroupIds()[1]})
                );
            partition.SendToPipe(std::move(request));
        }

        {
            auto request = partition.CreateWriteBlocksRequest(
                TBlockRange32::WithLength(0, 1024));
            partition.SendToPipe(std::move(request));

            auto response =
                partition.RecvResponse<TEvService::TEvWriteBlocksResponse>();
            UNIT_ASSERT_VALUES_EQUAL(
                E_BS_OUT_OF_SPACE,
                response->GetError().GetCode()
            );
        }

        // this time no reassign request should have been sent because of the
        // ReassignRequestSentTs check in partition actor
        UNIT_ASSERT_VALUES_EQUAL(1, reassignCounter->Val());

        UNIT_ASSERT_VALUES_EQUAL(0, reassignedTabletId);
        UNIT_ASSERT_VALUES_EQUAL(0, channels.size());

        runtime.AdvanceCurrentTime(reassignTimeout);

        {
            auto request = partition.CreateWriteBlocksRequest(
                TBlockRange32::WithLength(0, 1024));
            partition.SendToPipe(std::move(request));

            auto response =
                partition.RecvResponse<TEvService::TEvWriteBlocksResponse>();
            UNIT_ASSERT_VALUES_EQUAL(
                E_BS_OUT_OF_SPACE,
                response->GetError().GetCode()
            );
        }

        // ReassignRequestRetryTimeout has passed - another reassign request
        // should've been sent
        UNIT_ASSERT_VALUES_EQUAL(2, reassignCounter->Val());

        UNIT_ASSERT_VALUES_EQUAL(tabletId, reassignedTabletId);
        UNIT_ASSERT_VALUES_EQUAL(5, channels.size());
        for (ui32 i = 0; i < channels.size(); ++i) {
            UNIT_ASSERT_VALUES_EQUAL(i, channels[i]);
        }

        partition.RebootTablet();
        ssflags = {};

        // no yellow channels => write request should succeed
        partition.WriteBlocks(TBlockRange32::WithLength(0, 1024));

        partition.RebootTablet();

        {
            // informing partition tablet that the first group is yellow
            auto request =
                std::make_unique<TEvTablet::TEvCheckBlobstorageStatusResult>(
                    TVector<ui32>({env.GetGroupIds()[0]}),
                    TVector<ui32>({env.GetGroupIds()[0]})
                );
            partition.SendToPipe(std::move(request));
        }

        runtime.DispatchEvents({});

        {
            auto request = partition.CreateWriteBlocksRequest(
                TBlockRange32::WithLength(0, 1024));
            partition.SendToPipe(std::move(request));

            auto response =
                partition.RecvResponse<TEvService::TEvWriteBlocksResponse>();
            UNIT_ASSERT_VALUES_EQUAL(
                E_BS_OUT_OF_SPACE,
                response->GetError().GetCode()
            );
        }

        // checking that a reassign request has been sent
        UNIT_ASSERT_VALUES_EQUAL(3, reassignCounter->Val());

        UNIT_ASSERT_VALUES_EQUAL(tabletId, reassignedTabletId);
        UNIT_ASSERT_VALUES_EQUAL(4, channels.size());
        UNIT_ASSERT_VALUES_EQUAL(0, channels[0]);
        UNIT_ASSERT_VALUES_EQUAL(1, channels[1]);
        UNIT_ASSERT_VALUES_EQUAL(2, channels[2]);
        UNIT_ASSERT_VALUES_EQUAL(3, channels[3]);
    }

    Y_UNIT_TEST(ShouldReassignNonwritableTabletChannelsAgainAfterErrorFromHiveProxy)
    {
        auto runtime = PrepareTestActorRuntime();

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        ui64 reassignedTabletId = 0;
        TVector<ui32> channels;
        ui32 ssflags = ui32(
            NKikimrBlobStorage::StatusDiskSpaceYellowStop |
            NKikimrBlobStorage::StatusDiskSpaceLightYellowMove);

        runtime->SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) {
                switch (event->GetTypeRewrite()) {
                    case TEvHiveProxy::EvReassignTabletRequest: {
                        auto* msg = event->Get<TEvHiveProxy::TEvReassignTabletRequest>();
                        reassignedTabletId = msg->TabletId;
                        channels = msg->Channels;
                        auto response =
                            std::make_unique<TEvHiveProxy::TEvReassignTabletResponse>(
                                MakeError(E_REJECTED, "error")
                            );
                        runtime->Send(new IEventHandle(
                            event->Sender,
                            event->Recipient,
                            response.release(),
                            0, // flags
                            0
                        ), 0);

                        return TTestActorRuntime::EEventAction::DROP;
                    }
                }

                return StorageStateChanger(ssflags)(event);
            }
        );

        // first request is successful since we don't know that our channels
        // are yellow yet
        partition.WriteBlocks(TBlockRange32::WithLength(0, 1024));

        // but upon EvPutResult we should find out that our channels are yellow
        // and should send a reassign request
        UNIT_ASSERT_VALUES_EQUAL(TestTabletId, reassignedTabletId);
        UNIT_ASSERT_VALUES_EQUAL(1, channels.size());
        UNIT_ASSERT_VALUES_EQUAL(3, channels.front());

        reassignedTabletId = 0;
        channels.clear();

        for (ui32 i = 0; i < 3; ++i) {
            {
                // other requests should fail
                auto request = partition.CreateWriteBlocksRequest(
                    TBlockRange32::WithLength(0, 1024));
                partition.SendToPipe(std::move(request));

                auto response =
                    partition.RecvResponse<TEvService::TEvWriteBlocksResponse>();
                UNIT_ASSERT_VALUES_EQUAL(
                    E_BS_OUT_OF_SPACE,
                    response->GetError().GetCode()
                );
            }

            // checking that a reassign request has been sent
            UNIT_ASSERT_VALUES_EQUAL(TestTabletId, reassignedTabletId);
            UNIT_ASSERT_VALUES_EQUAL(1, channels.size());
            UNIT_ASSERT_VALUES_EQUAL(3, channels.front());

            reassignedTabletId = 0;
            channels.clear();
        }
    }

    Y_UNIT_TEST(ShouldSendBackpressureReportsUponChannelColorChange)
    {
        auto runtime = PrepareTestActorRuntime();

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        TBackpressureReport report;

        runtime->SetObserverFunc(
            [&report] (TAutoPtr<IEventHandle>& event) {
                switch (event->GetTypeRewrite()) {
                    case TEvPartition::EvBackpressureReport: {
                        report = *event->Get<TEvPartition::TEvBackpressureReport>();
                        break;
                    }
                }

                return StorageStateChanger(
                    NKikimrBlobStorage::StatusDiskSpaceLightYellowMove |
                    NKikimrBlobStorage::StatusDiskSpaceYellowStop)(event);
            }
        );

        partition.WriteBlocks(TBlockRange32::WithLength(0, 1024));

        UNIT_ASSERT(report.DiskSpaceScore > 0);
    }

    Y_UNIT_TEST(ShouldSendBackpressureReportsRegularly)
    {
        const auto blockCount = 1000;
        auto runtime = PrepareTestActorRuntime(DefaultConfig(), blockCount);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        TBackpressureReport report;
        bool reportUpdated = false;

        runtime->SetObserverFunc(
            [&report, &reportUpdated] (TAutoPtr<IEventHandle>& event) {
                switch (event->GetTypeRewrite()) {
                    case TEvPartition::EvBackpressureReport: {
                        report = *event->Get<TEvPartition::TEvBackpressureReport>();
                        reportUpdated = true;
                        break;
                    }
                }

                return TTestActorRuntime::DefaultObserverFunc(event);
            }
        );

        for (int i = 0; i < 200; ++i) {
            partition.WriteBlocks(i, i);
        }

        // Manually sending TEvSendBackpressureReport here
        // In real scenarios TPartitionActor will schedule this event
        // upon executor activation
        reportUpdated = false;
        partition.SendToPipe(
            std::make_unique<TEvPartitionPrivate::TEvSendBackpressureReport>()
        );

        runtime->DispatchEvents({}, TDuration::Seconds(5));

        UNIT_ASSERT_VALUES_EQUAL(true, reportUpdated);
        UNIT_ASSERT_DOUBLES_EQUAL(3.5, report.FreshIndexScore, 1e-5);
    }

    Y_UNIT_TEST(ShouldRejectRequestsInProgressOnTabletDeath)
    {
        auto runtime = PrepareTestActorRuntime();

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        runtime->SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) {
                if (event->GetTypeRewrite() == TEvPartitionPrivate::EvWriteBlobResponse) {
                    return TTestActorRuntime::EEventAction::DROP;
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        partition.SendWriteBlocksRequest(TBlockRange32::WithLength(0, 1024));

        // wait for background operations completion
        runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        // kill tablet
        partition.RebootTablet();

        auto response = partition.RecvWriteBlocksResponse();
        UNIT_ASSERT_C(response->GetStatus() == E_REJECTED, response->GetErrorReason());
    }

    Y_UNIT_TEST(ShouldReportNumberOfNonEmptyRangesInVolumeStats)
    {
        auto config = DefaultConfig();

        auto runtime = PrepareTestActorRuntime(config, 4096 * MaxBlocksCount);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        for (ui32 g = 0; g < 4; ++g) {
            partition.WriteBlocks(g * MaxBlocksCount * MaxBlocksCount, 1);
        }

        partition.Flush();

        auto response = partition.StatPartition();
        const auto& stats = response->Record.GetStats();
        UNIT_ASSERT_VALUES_EQUAL(4, stats.GetNonEmptyRangeCount());
    }

    Y_UNIT_TEST(ShouldHandleDescribeBlocksRequestWithInvalidCheckpointId)
    {
        auto runtime = PrepareTestActorRuntime();
        TPartitionClient partition(*runtime);
        partition.WaitReady();

        const TString validCheckpoint = "0";
        partition.CreateCheckpoint(validCheckpoint);

        const auto range = TBlockRange32::WithLength(0, 1);
        partition.DescribeBlocks(range, validCheckpoint);

        {
            const TString invalidCheckpoint = "1";
            auto request =
                partition.CreateDescribeBlocksRequest(range, invalidCheckpoint);
            partition.SendToPipe(std::move(request));
            auto response =
                partition.RecvResponse<TEvVolume::TEvDescribeBlocksResponse>();
            UNIT_ASSERT(response->GetStatus() == E_NOT_FOUND);
        }
    }

    Y_UNIT_TEST(ShouldForbidDescribeBlocksWithEmptyRange)
    {
        auto runtime = PrepareTestActorRuntime();
        TPartitionClient partition(*runtime);
        partition.WaitReady();

        {
            auto request = partition.CreateDescribeBlocksRequest(0, 0);
            partition.SendToPipe(std::move(request));
            auto response =
                partition.RecvResponse<TEvVolume::TEvDescribeBlocksResponse>();
            UNIT_ASSERT(response->GetStatus() == E_ARGUMENT);
        }
    }

    Y_UNIT_TEST(DescribeBlocksIsNotImplementedForOverlayDisks)
    {
        auto partitionWithRuntime =
            SetupOverlayPartition(TestTabletId, TestTabletId2);

        auto& partition = *partitionWithRuntime.Partition;
        partition.WaitReady();

        {
            auto request = partition.CreateDescribeBlocksRequest(0, 0);
            partition.SendToPipe(std::move(request));
            auto response =
                partition.RecvResponse<TEvVolume::TEvDescribeBlocksResponse>();
            UNIT_ASSERT(response->GetStatus() == E_NOT_IMPLEMENTED);
        }
    }

    Y_UNIT_TEST(ShouldHandleDescribeBlocksRequestWithOutOfBoundsRange)
    {
        auto runtime = PrepareTestActorRuntime();
        TPartitionClient partition(*runtime);
        partition.WaitReady();

        partition.WriteBlocks(TBlockRange32::WithLength(0, 11), char(1));

        {
            const auto range =
                TBlockRange32::MakeClosedInterval(Max<ui32>() - 1, Max<ui32>());
            auto request = partition.CreateDescribeBlocksRequest(range);
            partition.SendToPipe(std::move(request));
            auto response =
                partition.RecvResponse<TEvVolume::TEvDescribeBlocksResponse>();
            UNIT_ASSERT(response->GetStatus() == S_OK);
            UNIT_ASSERT(response->Record.FreshBlockRangesSize() == 0);
            UNIT_ASSERT(response->Record.BlobPiecesSize() == 0);
        }
    }

    Y_UNIT_TEST(ShouldHandleDescribeBlocksRequestWhenBlocksAreFresh)
    {
        auto runtime = PrepareTestActorRuntime();
        TPartitionClient partition(*runtime);
        partition.WaitReady();

        const auto range = TBlockRange32::WithLength(1, 11);
        partition.WriteBlocks(range, char(1));

        const TString checkpoint = "0";
        partition.CreateCheckpoint(checkpoint);

        {
            auto response = partition.DescribeBlocks(range, checkpoint);
            TString actualContent;
            TVector<TBlockRange32> actualRanges;

            auto extractContent = [&]() {
                const auto& message = response->Record;
                for (size_t i = 0; i < message.FreshBlockRangesSize(); ++i) {
                    const auto& freshRange = message.GetFreshBlockRanges(i);
                    actualContent += freshRange.GetBlocksContent();
                    actualRanges.push_back(
                        TBlockRange32::WithLength(
                            freshRange.GetStartIndex(),
                            freshRange.GetBlocksCount()));
                 }
            };

            extractContent();
            TString expectedContent = GetBlocksContent(char(1), range.Size());
            UNIT_ASSERT_VALUES_EQUAL(expectedContent, actualContent);
            CheckRangesArePartition(actualRanges, range);

            response = partition.DescribeBlocks(0, Max<ui32>(), checkpoint);
            actualContent = {};
            actualRanges = {};

            extractContent();
            expectedContent = GetBlocksContent(char(1), range.Size());
            UNIT_ASSERT_VALUES_EQUAL(expectedContent, actualContent);
            CheckRangesArePartition(actualRanges, range);
        }
    }

    Y_UNIT_TEST(ShouldHandleDescribeBlocksRequestWithOneBlob)
    {
        auto runtime = PrepareTestActorRuntime();
        TPartitionClient partition(*runtime);
        partition.WaitReady();

        const auto range1 = TBlockRange32::WithLength(11, 11);
        partition.WriteBlocks(range1, char(1));
        const auto range2 = TBlockRange32::WithLength(33, 22);
        partition.WriteBlocks(range2, char(1));
        partition.Flush();

        {
            const auto response =
                partition.DescribeBlocks(TBlockRange32::WithLength(0, 101));
            const auto& message = response->Record;
            // Expect that all data is contained in one blob.
            UNIT_ASSERT_VALUES_EQUAL(1, message.BlobPiecesSize());

            const auto& blobPiece = message.GetBlobPieces(0);
            UNIT_ASSERT_VALUES_EQUAL(2, blobPiece.RangesSize());
            const auto& r1 = blobPiece.GetRanges(0);
            const auto& r2 = blobPiece.GetRanges(1);

            UNIT_ASSERT_VALUES_EQUAL(0, r1.GetBlobOffset());
            UNIT_ASSERT_VALUES_EQUAL(range1.Start, r1.GetBlockIndex());
            UNIT_ASSERT_VALUES_EQUAL(range1.Size(), r1.GetBlocksCount());

            UNIT_ASSERT_VALUES_EQUAL(range1.Size(), r2.GetBlobOffset());
            UNIT_ASSERT_VALUES_EQUAL(range2.Start, r2.GetBlockIndex());
            UNIT_ASSERT_VALUES_EQUAL(range2.Size(), r2.GetBlocksCount());

            TVector<ui16> blobOffsets;
            for (size_t i = 0; i < r1.GetBlocksCount(); ++i) {
                blobOffsets.push_back(r1.GetBlobOffset() + i);
            }
            for (size_t i = 0; i < r2.GetBlocksCount(); ++i) {
                blobOffsets.push_back(r2.GetBlobOffset() + i);
            }

            {
                TVector<TString> blocks;
                auto sglist = ResizeBlocks(
                    blocks,
                    range1.Size() + range2.Size(),
                    TString(DefaultBlockSize, char(0)));

                const auto blobId =
                    LogoBlobIDFromLogoBlobID(blobPiece.GetBlobId());
                const auto group = blobPiece.GetBSGroupId();
                const auto response =
                    partition.ReadBlob(blobId, group, blobOffsets, sglist);

                for (size_t i = 0; i < sglist.size(); ++i) {
                    const auto block = sglist[i].AsStringBuf();
                    UNIT_ASSERT_VALUES_EQUAL_C(
                        GetBlockContent(
                            char(1)),
                            block,
                            "during iteration #" << i);
                }
            }
        }
    }

    Y_UNIT_TEST(ShouldRespondToDescribeBlocksRequestWithDenseBlobs)
    {
        auto runtime = PrepareTestActorRuntime();
        TPartitionClient partition(*runtime);
        partition.WaitReady();

        partition.WriteBlocks(0, char(1));
        partition.WriteBlocks(1, char(1));
        partition.WriteBlocks(2, char(1));
        partition.Flush();
        partition.WriteBlocks(3, char(2));
        partition.WriteBlocks(4, char(2));
        partition.WriteBlocks(5, char(2));
        partition.Flush();

        {
            const auto response = partition.DescribeBlocks(
                TBlockRange32::MakeClosedInterval(1, 4));
            const auto& message = response->Record;
            UNIT_ASSERT_VALUES_EQUAL(2, message.BlobPiecesSize());

            const auto& blobPiece1 = message.GetBlobPieces(0);
            UNIT_ASSERT_VALUES_EQUAL(1, blobPiece1.RangesSize());
            const auto& r1_2 = blobPiece1.GetRanges(0);
            UNIT_ASSERT_VALUES_EQUAL(1, r1_2.GetBlobOffset());
            UNIT_ASSERT_VALUES_EQUAL(1, r1_2.GetBlockIndex());
            UNIT_ASSERT_VALUES_EQUAL(2, r1_2.GetBlocksCount());
            const auto blobId1 =
                LogoBlobIDFromLogoBlobID(blobPiece1.GetBlobId());
            const auto group1 = blobPiece1.GetBSGroupId();

            const auto& blobPiece2 = message.GetBlobPieces(1);
            UNIT_ASSERT_VALUES_EQUAL(1, blobPiece2.RangesSize());
            const auto& r3_5 = blobPiece2.GetRanges(0);
            UNIT_ASSERT_VALUES_EQUAL(0, r3_5.GetBlobOffset());
            UNIT_ASSERT_VALUES_EQUAL(3, r3_5.GetBlockIndex());
            UNIT_ASSERT_VALUES_EQUAL(2, r3_5.GetBlocksCount());
            const auto blobId2 =
                LogoBlobIDFromLogoBlobID(blobPiece2.GetBlobId());
            const auto group2 = blobPiece2.GetBSGroupId();

            for (ui16 i = 0; i < 2; ++i) {
                const TVector<ui16> offsets = {i};
                {
                    TVector<TString> blocks;
                    auto sglist = ResizeBlocks(blocks, 1, TString(DefaultBlockSize, char(0)));
                    const auto response =
                        partition.ReadBlob(blobId1, group1, offsets, sglist);
                    UNIT_ASSERT_VALUES_EQUAL(
                        GetBlockContent(char(1)),
                        sglist[0].AsStringBuf()
                    );
                }

                {
                    TVector<TString> blocks;
                    auto sglist = ResizeBlocks(blocks, 1, TString(DefaultBlockSize, char(0)));
                    const auto response =
                        partition.ReadBlob(blobId2, group2, offsets, sglist);
                    UNIT_ASSERT_VALUES_EQUAL(
                        GetBlockContent(char(2)),
                        sglist[0].AsStringBuf()
                    );
                }
            }
        }
    }

    Y_UNIT_TEST(ShouldRespondToDescribeBlocksRequestWithSparseBlobs)
    {
        auto runtime = PrepareTestActorRuntime();
        TPartitionClient partition(*runtime);
        partition.WaitReady();

        partition.WriteBlocks(0, char(1));
        partition.WriteBlocks(2, char(1));
        partition.Flush();
        partition.WriteBlocks(4, char(2));
        partition.WriteBlocks(6, char(2));
        partition.Flush();

        {
            const auto response =
                partition.DescribeBlocks(TBlockRange32::WithLength(0, 7));
            const auto& message = response->Record;
            UNIT_ASSERT_VALUES_EQUAL(2, message.BlobPiecesSize());

            const auto& blobPiece1 = message.GetBlobPieces(0);
            UNIT_ASSERT_VALUES_EQUAL(2, blobPiece1.RangesSize());
            const auto& r0 = blobPiece1.GetRanges(0);
            UNIT_ASSERT_VALUES_EQUAL(0, r0.GetBlobOffset());
            UNIT_ASSERT_VALUES_EQUAL(0, r0.GetBlockIndex());
            UNIT_ASSERT_VALUES_EQUAL(1, r0.GetBlocksCount());
            const auto& r1 = blobPiece1.GetRanges(1);
            UNIT_ASSERT_VALUES_EQUAL(1, r1.GetBlobOffset());
            UNIT_ASSERT_VALUES_EQUAL(2, r1.GetBlockIndex());
            UNIT_ASSERT_VALUES_EQUAL(1, r1.GetBlocksCount());
            const auto blobId1 =
                LogoBlobIDFromLogoBlobID(blobPiece1.GetBlobId());
            const auto group1 = blobPiece1.GetBSGroupId();

            const auto& blobPiece2 = message.GetBlobPieces(1);
            UNIT_ASSERT_VALUES_EQUAL(2, blobPiece2.RangesSize());
            const auto& r4 = blobPiece2.GetRanges(0);
            UNIT_ASSERT_VALUES_EQUAL(0, r4.GetBlobOffset());
            UNIT_ASSERT_VALUES_EQUAL(4, r4.GetBlockIndex());
            UNIT_ASSERT_VALUES_EQUAL(1, r4.GetBlocksCount());
            const auto& r6 = blobPiece2.GetRanges(1);
            UNIT_ASSERT_VALUES_EQUAL(1, r6.GetBlobOffset());
            UNIT_ASSERT_VALUES_EQUAL(6, r6.GetBlockIndex());
            UNIT_ASSERT_VALUES_EQUAL(1, r6.GetBlocksCount());
            const auto blobId2 =
                LogoBlobIDFromLogoBlobID(blobPiece2.GetBlobId());
            const auto group2 = blobPiece2.GetBSGroupId();

            for (ui16 i = 0; i < 1; ++i) {
                const TVector<ui16> offsets = {i};
                {
                    TVector<TString> blocks;
                    auto sglist = ResizeBlocks(blocks, 1, TString(DefaultBlockSize, char(0)));
                    const auto response =
                        partition.ReadBlob(blobId1, group1, offsets, sglist);
                    UNIT_ASSERT_VALUES_EQUAL(
                        GetBlockContent(char(1)),
                        sglist[0].AsStringBuf()
                    );
                }

                {
                    TVector<TString> blocks;
                    auto sglist = ResizeBlocks(blocks, 1, TString(DefaultBlockSize, char(0)));
                    const auto response =
                        partition.ReadBlob(blobId2, group2, offsets, sglist);
                    UNIT_ASSERT_VALUES_EQUAL(
                        GetBlockContent(char(2)),
                        sglist[0].AsStringBuf()
                    );
                }
            }
        }
    }

    Y_UNIT_TEST(ShouldReturnExactlyOneVersionOfEachBlockInDescribeBlocksResponse) {
        auto runtime = PrepareTestActorRuntime();
        TPartitionClient partition(*runtime);
        partition.WaitReady();

        // Without fresh blocks.
        partition.WriteBlocks(0, char(1));
        partition.Flush();
        partition.CreateCheckpoint("checkpoint1");
        partition.WriteBlocks(0, char(2));
        partition.Flush();
        {
            const auto response =
                partition.DescribeBlocks(TBlockRange32::MakeOneBlock(0));
            UNIT_ASSERT_VALUES_EQUAL(0, response->Record.FreshBlockRangesSize());

            UNIT_ASSERT_VALUES_EQUAL(1, response->Record.BlobPiecesSize());
            const auto& blobPiece = response->Record.GetBlobPieces(0);
            UNIT_ASSERT_VALUES_EQUAL(1, blobPiece.RangesSize());
            UNIT_ASSERT_VALUES_EQUAL(0, blobPiece.GetRanges(0).GetBlobOffset());
            UNIT_ASSERT_VALUES_EQUAL(0, blobPiece.GetRanges(0).GetBlockIndex());

            const auto blobId = LogoBlobIDFromLogoBlobID(blobPiece.GetBlobId());
            const auto group = blobPiece.GetBSGroupId();
            {
                TVector<TString> blocks;
                auto sglist = ResizeBlocks(blocks, 1, TString(DefaultBlockSize, char(0)));
                const auto response = partition.ReadBlob(blobId, group, TVector<ui16>{0}, sglist);
                UNIT_ASSERT_VALUES_EQUAL(
                    GetBlockContent(char(2)),
                    sglist[0].AsStringBuf()
                );
            }
        }

        // With fresh blocks.
        partition.WriteBlocks(0, char(3));
        partition.CreateCheckpoint("checkpoint2");
        partition.WriteBlocks(0, char(4));
        {
            const auto response =
                partition.DescribeBlocks(TBlockRange32::MakeOneBlock(0));
            UNIT_ASSERT_VALUES_EQUAL(0, response->Record.BlobPiecesSize());

            UNIT_ASSERT_VALUES_EQUAL(1, response->Record.FreshBlockRangesSize());
            const auto& freshBlockRange = response->Record.GetFreshBlockRanges(0);
            UNIT_ASSERT_VALUES_EQUAL(1, freshBlockRange.GetBlocksCount());

            UNIT_ASSERT_VALUES_EQUAL(
                GetBlockContent(char(4)),
                freshBlockRange.GetBlocksContent()
            );
        }
    }

    Y_UNIT_TEST(ShouldCorrectlyCalculateUsedBlocksCount)
    {
        constexpr ui32 blockCount = 1024 * 1024;
        auto runtime = PrepareTestActorRuntime(DefaultConfig(), blockCount);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        partition.WriteBlocks(TBlockRange32::WithLength(0, 1024 * 10 + 1), 1);
        partition.WriteBlocks(
            TBlockRange32::MakeClosedInterval(1024 * 5, 1024 * 11),
            1);

        const auto step = 16;
        for (ui32 i = 1024 * 10; i < 1024 * 12; i += step) {
            partition.WriteBlocks(TBlockRange32::WithLength(i, step), 1);
        }

        for (ui32 i = 1024 * 20; i < 1024 * 21; i += step) {
            partition.WriteBlocks(TBlockRange32::WithLength(i, step + 1), 1);
        }

        partition.WriteBlocks(TBlockRange32::MakeClosedInterval(1001111, 1001210), 1);

        partition.ZeroBlocks(TBlockRange32::MakeClosedInterval(1024, 3023));
        partition.ZeroBlocks(TBlockRange32::MakeClosedInterval(5024, 5033));

        const auto expected = 1024 * 12 + 1024 + 1 + 100 - 2000 - 10;

        /*
        partition.WriteBlocks(TBlockRange32(5024, 5043), 1);
        partition.ZeroBlocks(TBlockRange32::MakeClosedInterval(5024, 5033));
        const auto expected = 10;
        */

        auto response = partition.StatPartition();
        auto stats = response->Record.GetStats();
        UNIT_ASSERT_VALUES_EQUAL(expected, stats.GetUsedBlocksCount());
        UNIT_ASSERT_VALUES_EQUAL(expected, stats.GetLogicalUsedBlocksCount());

        partition.RebootTablet();

        response = partition.StatPartition();
        stats = response->Record.GetStats();
        UNIT_ASSERT_VALUES_EQUAL(expected, stats.GetUsedBlocksCount());
        UNIT_ASSERT_VALUES_EQUAL(expected, stats.GetLogicalUsedBlocksCount());

        ui32 completionStatus = -1;
        runtime->SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) {
                switch (event->GetTypeRewrite()) {
                    case TEvPartitionPrivate::EvMetadataRebuildCompleted: {
                        using TEv =
                            TEvPartitionPrivate::TEvMetadataRebuildCompleted;
                        auto* msg = event->Get<TEv>();
                        completionStatus = msg->GetStatus();
                        break;
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            }
        );

        const auto rangesPerBatch = 100;
        partition.RebuildMetadata(NProto::ERebuildMetadataType::USED_BLOCKS, rangesPerBatch);

        UNIT_ASSERT_VALUES_EQUAL(S_OK, completionStatus);

        response = partition.StatPartition();
        stats = response->Record.GetStats();
        UNIT_ASSERT_VALUES_EQUAL(expected, stats.GetUsedBlocksCount());
        UNIT_ASSERT_VALUES_EQUAL(expected, stats.GetLogicalUsedBlocksCount());

        partition.RebootTablet();

        response = partition.StatPartition();
        stats = response->Record.GetStats();
        UNIT_ASSERT_VALUES_EQUAL(expected, stats.GetUsedBlocksCount());
        UNIT_ASSERT_VALUES_EQUAL(expected, stats.GetLogicalUsedBlocksCount());
    }

    Y_UNIT_TEST(ShouldCorrectlyCopyUsedBlocksCountForOverlayDisk)
    {
        ui32 blockCount =  1024 * 1024 * 1024;
        ui32 usedBlocksCount = 0;

        TPartitionContent baseContent;
        for (size_t i = 0; i < blockCount/4; i += 50) {
            baseContent.push_back(TBlob(i, 0, 49));
            usedBlocksCount += 49;
            baseContent.push_back(TEmpty());
        }

        auto partitionWithRuntime = SetupOverlayPartition(
            TestTabletId,
            TestTabletId2,
            baseContent,
            {},
            DefaultBlockSize,
            blockCount,
            DefaultConfig());

        auto& partition = *partitionWithRuntime.Partition;

        auto response = partition.StatPartition();
        auto stats = response->Record.GetStats();

        UNIT_ASSERT_VALUES_EQUAL(0, stats.GetUsedBlocksCount());
        UNIT_ASSERT_VALUES_EQUAL(
            usedBlocksCount,
            stats.GetLogicalUsedBlocksCount()
        );
    }

    Y_UNIT_TEST(ShouldCorrectlyCalculateLogicalUsedBlocksCountForOverlayDisk)
    {
        ui32 blockCount = 1024 * 128;
        TPartitionContent baseContent;
        for (size_t i = 0; i < 100; ++i) {
            baseContent.push_back(TEmpty());
        }
        for (size_t i = 100; i < 2048 + 100; ++i) {
            baseContent.push_back(TBlob(i, 1));
        }
        for (size_t i = 0; i < 100; ++i) {
            baseContent.push_back(TEmpty());
        }

        auto partitionWithRuntime = SetupOverlayPartition(
            TestTabletId,
            TestTabletId2,
            baseContent,
            {},
            DefaultBlockSize,
            blockCount,
            DefaultConfig());

        auto& partition = *partitionWithRuntime.Partition;

        partition.WriteBlocks(
            TBlockRange32::WithLength(1024 * 4, 2049),
            1);   // +2049
        partition.ZeroBlocks(TBlockRange32::WithLength(1024 * 5, 11));   // -11
        partition.ZeroBlocks(
            TBlockRange32::WithLength(1024 * 10, 1025));   // -0
        partition.WriteBlocks(
            TBlockRange32::WithLength(1024 * 4, 11),
            1);   // +0
        partition.WriteBlocks(
            TBlockRange32::WithLength(1024 * 3, 11),
            1);   // +11

        ui64 expectedUsedBlocksCount = 2048 + 1 - 11 + 11;
        ui64 expectedLogicalUsedBlocksCount = 2048 + expectedUsedBlocksCount;

        auto response = partition.StatPartition();
        auto stats = response->Record.GetStats();

        UNIT_ASSERT_VALUES_EQUAL(
            expectedUsedBlocksCount,
            stats.GetUsedBlocksCount()
        );
        UNIT_ASSERT_VALUES_EQUAL(
            expectedLogicalUsedBlocksCount,
            stats.GetLogicalUsedBlocksCount()
        );

        partition.RebootTablet();

        response = partition.StatPartition();
        stats = response->Record.GetStats();

        UNIT_ASSERT_VALUES_EQUAL(
            expectedUsedBlocksCount,
            stats.GetUsedBlocksCount()
        );
        UNIT_ASSERT_VALUES_EQUAL(
            expectedLogicalUsedBlocksCount,
            stats.GetLogicalUsedBlocksCount()
        );

        // range [100-200) is zero on overlay disk, but non zero on base disk
        partition.ZeroBlocks(TBlockRange32::WithLength(100, 100));

        response = partition.StatPartition();
        stats = response->Record.GetStats();

        UNIT_ASSERT_VALUES_EQUAL(
            expectedUsedBlocksCount,
            stats.GetUsedBlocksCount()
        );
        UNIT_ASSERT_VALUES_EQUAL(
            expectedLogicalUsedBlocksCount - 100,
            stats.GetLogicalUsedBlocksCount()
        );
    }

    // NBS-3934
    Y_UNIT_TEST(ShouldNotCrashWithMalformedUnicodeCharacterInGetUsedBlocksResponse)
    {
        TPartitionContent baseContent;

        // malformed utf-8 character
        auto c = 0xa9;
        while (c != 0) {
            if (c & 1) {
                baseContent.push_back(TBlob(0, 0));
            } else {
                baseContent.push_back(TEmpty());
            }

            c >>= 1;
        }

        auto partitionWithRuntime = SetupOverlayPartition(
            TestTabletId,
            TestTabletId2,
            baseContent);

        auto& partition = *partitionWithRuntime.Partition;
        auto response = partition.StatPartition();
        auto stats = response->Record.GetStats();

        UNIT_ASSERT_VALUES_EQUAL(4, stats.GetLogicalUsedBlocksCount());
    }

    Y_UNIT_TEST(ShouldReadBlocksFromBaseDisk)
    {
        TPartitionContent baseContent = {
        /*|      0      |     1     |     2 ... 5    |     6     |      7      |     8     |      9      |*/
            TBlob(1, 1) , TFresh(2) , TBlob(2, 3, 4) ,  TEmpty() , TBlob(1, 4) , TFresh(5) , TBlob(2, 6)
        };
        auto bitmap = CreateBitmap(10);

        auto partitionWithRuntime =
            SetupOverlayPartition(TestTabletId, TestTabletId2, baseContent);
        auto& partition = *partitionWithRuntime.Partition;
        auto& runtime = *partitionWithRuntime.Runtime;

        auto response = partition.ReadBlocks(TBlockRange32::WithLength(0, 10));

        UNIT_ASSERT_VALUES_EQUAL(
            GetBlocksContent(baseContent), GetBlocksContent(response));
        UNIT_ASSERT(bitmap == GetUnencryptedBlockMask(response));

        const auto evStats = TEvStatsService::EvVolumePartCounters;

        ui32 externalBlobReads = 0;
        ui32 externalBlobBytes = 0;
        auto obs = [&] (TAutoPtr<IEventHandle>& event) {
                if (event->GetTypeRewrite() == evStats) {
                    auto* msg =
                        event->Get<TEvStatsService::TEvVolumePartCounters>();

                    const auto& readBlobCounters =
                        msg->DiskCounters->RequestCounters.ReadBlob;
                    externalBlobReads = readBlobCounters.ExternalCount;
                    externalBlobBytes = readBlobCounters.ExternalRequestBytes;
                }

                return TTestActorRuntime::DefaultObserverFunc(event);
            };

        runtime.SetObserverFunc(obs);

        partition.SendToPipe(
            std::make_unique<TEvPartitionPrivate::TEvUpdateCounters>());
        {
            TDispatchOptions options;
            options.FinalEvents.emplace_back(evStats);
            runtime.DispatchEvents(options);
        }

        UNIT_ASSERT_VALUES_EQUAL(2, externalBlobReads);
        UNIT_ASSERT_VALUES_EQUAL(7 * DefaultBlockSize, externalBlobBytes);
    }

    Y_UNIT_TEST(ShouldReadBlocksFromOverlayDisk)
    {
        TPartitionContent baseContent = {
            TBlob(1, 1), TFresh(2), TBlob(2, 3)
        };
        auto bitmap = CreateBitmap(3);

        auto partitionWithRuntime =
            SetupOverlayPartition(TestTabletId, TestTabletId2, baseContent);
        auto& partition = *partitionWithRuntime.Partition;

        const auto range = TBlockRange32::WithLength(0, baseContent.size());

        partition.WriteBlocks(range, char(2));
        MarkWrittenBlocks(bitmap, range);

        partition.Flush();

        auto response = partition.ReadBlocks(range);

        UNIT_ASSERT_VALUES_EQUAL(
            GetBlocksContent(char(2), range.Size()),
            GetBlocksContent(response));
        UNIT_ASSERT(bitmap == GetUnencryptedBlockMask(response));
    }

    Y_UNIT_TEST(ShouldReadBlocksFromOverlayDiskWhenRangeOverlapsWithBaseDisk)
    {
        TPartitionContent baseContent = {
        /*|      0      |     1     |      2      |     3     |      4      |     5     |      6      |*/
            TBlob(1, 1) , TFresh(1) , TBlob(2, 2) ,  TEmpty() , TBlob(3, 3) , TFresh(3) , TBlob(4, 3)
        };
        auto bitmap = CreateBitmap(7);

        auto partitionWithRuntime =
            SetupOverlayPartition(TestTabletId, TestTabletId2, baseContent);
        auto& partition = *partitionWithRuntime.Partition;

        auto writeRange = TBlockRange32::MakeClosedInterval(2, 4);
        partition.WriteBlocks(writeRange, char(4));
        MarkWrittenBlocks(bitmap, writeRange);

        partition.Flush();

        auto response = partition.ReadBlocks(TBlockRange32::WithLength(0, 7));

        UNIT_ASSERT_VALUES_EQUAL(
            GetBlocksContent(char(1), 2) +
            GetBlocksContent(char(4), 3) +
            GetBlocksContent(char(3), 2),
            GetBlocksContent(response));
        UNIT_ASSERT(bitmap == GetUnencryptedBlockMask(response));
    }

    Y_UNIT_TEST(ShouldNotReadBlocksFromBaseDiskWhenTheyAreZeroedInOverlayDisk)
    {
        TPartitionContent baseContent = {
            TBlob(1, 1), TBlob(1, 2), TBlob(1, 3)
        };
        auto bitmap = CreateBitmap(3);

        auto partitionWithRuntime =
            SetupOverlayPartition(TestTabletId, TestTabletId2, baseContent);
        auto& partition = *partitionWithRuntime.Partition;

        const auto range = TBlockRange32::WithLength(0, baseContent.size());

        partition.ZeroBlocks(range);

        partition.Flush();

        auto response = partition.ReadBlocks(range);

        UNIT_ASSERT_VALUES_EQUAL(TString(), GetBlocksContent(response));
        UNIT_ASSERT(bitmap == GetUnencryptedBlockMask(response));
    }

    Y_UNIT_TEST(ShouldReadBlocksWithBaseDiskAndComplexRanges)
    {
        TPartitionContent baseContent = {
        /*|     0    |      1      |     2     |     3     |     4    |     5    |      6      |     7     |     8    |*/
            TEmpty() , TBlob(1, 1) , TFresh(2) ,  TEmpty() , TEmpty() , TEmpty() , TBlob(2, 3) , TFresh(4) , TEmpty()
        };
        auto bitmap = CreateBitmap(9);

        auto partitionWithRuntime =
            SetupOverlayPartition(TestTabletId, TestTabletId2, baseContent);
        auto& partition = *partitionWithRuntime.Partition;

        auto writeRange1 = TBlockRange32::MakeClosedInterval(2, 3);
        partition.WriteBlocks(writeRange1, char(5));
        MarkWrittenBlocks(bitmap, writeRange1);

        auto writeRange2 = TBlockRange32::MakeClosedInterval(5, 6);
        partition.WriteBlocks(writeRange2, char(6));
        MarkWrittenBlocks(bitmap, writeRange2);

        partition.Flush();

        auto response = partition.ReadBlocks(TBlockRange32::WithLength(0, 9));

        UNIT_ASSERT_VALUES_EQUAL(
            GetBlocksContent(char(0), 1) +
            GetBlocksContent(char(1), 1) +
            GetBlocksContent(char(5), 2) +
            GetBlocksContent(char(0), 1) +
            GetBlocksContent(char(6), 2) +
            GetBlocksContent(char(4), 1) +
            GetBlocksContent(char(0), 1),
            GetBlocksContent(response));
        UNIT_ASSERT(bitmap == GetUnencryptedBlockMask(response));
    }

    Y_UNIT_TEST(ShouldReadBlocksAfterCompactionOfOverlayDisk1)
    {
        TPartitionContent baseContent = { TBlob(1, 1) };
        auto bitmap = CreateBitmap(1);

        auto partitionWithRuntime =
            SetupOverlayPartition(TestTabletId, TestTabletId2, baseContent);
        auto& partition = *partitionWithRuntime.Partition;

        // Write 1023 blocks (4MB minus 4KB). After compaction, we have one
        // merged blob written.
        // It's a tricky situation because one block (at 0 index) is missing in
        // overlay disk and therefore this block should be read from base disk.
        auto writeRange = TBlockRange32::MakeClosedInterval(1, 1023);
        partition.WriteBlocks(writeRange, char(2));
        MarkWrittenBlocks(bitmap, writeRange);

        partition.Compaction();

        auto response = partition.ReadBlocks(TBlockRange32::WithLength(0, 1024));

        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(char(1)) +
            GetBlocksContent(char(2), 1023),
            GetBlocksContent(response));
        UNIT_ASSERT(bitmap == GetUnencryptedBlockMask(response));
    }

    Y_UNIT_TEST(ShouldReadBlocksAfterCompactionOfOverlayDisk2)
    {
        TPartitionContent baseContent;
        for (size_t i = 0; i < 1000; ++i) {
            baseContent.push_back(TEmpty());
        }
        for (size_t i = 1000; i < 1024; ++i) {
            baseContent.push_back(TBlob(i, 1));
        }
        auto bitmap = CreateBitmap(1024);

        auto partitionWithRuntime =
            SetupOverlayPartition(TestTabletId, TestTabletId2, baseContent);
        auto& partition = *partitionWithRuntime.Partition;

        // Write 1000 blocks. After compaction, we have one merged blob written.
        // It's a tricky situation because some blocks (in [1000..1023] range)
        // are missing in overlay disk and therefore these blocks should be read
        // from base disk.
        auto writeRange = TBlockRange32::WithLength(0, 1000);
        partition.WriteBlocks(writeRange, char(2));
        MarkWrittenBlocks(bitmap, writeRange);

        partition.Compaction();

        auto response = partition.ReadBlocks(TBlockRange32::WithLength(0, 1024));

        UNIT_ASSERT_VALUES_EQUAL(
            GetBlocksContent(char(2), 1000) +
            GetBlocksContent(char(1), 24),
            GetBlocksContent(response));
        UNIT_ASSERT(bitmap == GetUnencryptedBlockMask(response));

        partition.WriteBlocks(TBlockRange32::WithLength(0, 1024), char(3));
        partition.Compaction();
        partition.Cleanup();
        partition.CollectGarbage();

        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            // Other blobs should be collected.
            UNIT_ASSERT_VALUES_EQUAL(1, stats.GetMergedBlobsCount());
        }
    }

    Y_UNIT_TEST(ShouldReadBlocksAfterZeroBlocksAndCompactionOfOverlayDisk)
    {
        TPartitionContent baseContent = { TBlob(1, 1) };
        auto bitmap = CreateBitmap(1);

        auto partitionWithRuntime =
            SetupOverlayPartition(TestTabletId, TestTabletId2, baseContent);
        auto& partition = *partitionWithRuntime.Partition;

        // Zero 1023 blocks. After compaction, we have one merged zero blob
        // written. It's a tricky situation because one block (at 0 index) is
        // missed in overlay disk and therefore this block should be read from
        // base disk.
        auto zeroRange = TBlockRange32::MakeClosedInterval(1, 1023);
        partition.ZeroBlocks(zeroRange);
        MarkZeroedBlocks(bitmap, zeroRange);

        partition.Compaction();

        auto response =
            partition.ReadBlocks(TBlockRange32::WithLength(0, 1024));

        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(char(1)) +
            GetBlocksContent(char(0), 1023),
            GetBlocksContent(response));
        UNIT_ASSERT(bitmap == GetUnencryptedBlockMask(response));
    }

    Y_UNIT_TEST(ShouldNotKillTabletWhenFailedToReadBlobFromBaseDisk)
    {
        TPartitionContent baseContent = { TBlob(1, 1) };

        const auto baseTabletId = TestTabletId2;

        auto partitionWithRuntime =
            SetupOverlayPartition(TestTabletId, baseTabletId, baseContent);
        auto& partition = *partitionWithRuntime.Partition;
        auto& runtime = *partitionWithRuntime.Runtime;

        int pillCount = 0;

        const auto eventHandler =
            [&] (const TEvBlobStorage::TEvGet::TPtr& ev) {
                bool result = false;

                auto& msg = *ev->Get();

                auto response = std::make_unique<TEvBlobStorage::TEvGetResult>(
                    NKikimrProto::ERROR,
                    msg.QuerySize,
                    0);  // groupId

                for (ui32 i = 0; i < msg.QuerySize; ++i) {
                    const auto& q = msg.Queries[i];
                    const auto& blobId = q.Id;

                    if (blobId.TabletID() == baseTabletId) {
                        result = true;
                    }
                }

                if (result) {
                    runtime.Schedule(
                        new IEventHandle(
                            ev->Sender,
                            ev->Recipient,
                            response.release(),
                            0,
                            ev->Cookie),
                        TDuration());
                }

                return result;
            };

        runtime.SetEventFilter(
            [eventHandler] (TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& ev) {
                bool handled = false;

                const auto wrapped =
                    [&] (const auto& ev) {
                        handled = eventHandler(ev);
                    };

                switch (ev->GetTypeRewrite()) {
                    hFunc(TEvBlobStorage::TEvGet, wrapped);
                }
                return handled;
           });

        runtime.SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) {
                switch (event->GetTypeRewrite()) {
                    case TEvents::TSystem::PoisonPill: {
                        ++pillCount;
                        break;
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        partition.SendReadBlocksRequest(0);

        auto response = partition.RecvReadBlocksResponse();
        UNIT_ASSERT(FAILED(response->GetStatus()));

        UNIT_ASSERT_VALUES_EQUAL(0, pillCount);
    }

    Y_UNIT_TEST(ShouldReadBlocksWhenBaseBlobHasGreaterChannel)
    {
        const ui32 overlayTabletChannelsCount = 254;
        const ui32 baseBlobChannel = overlayTabletChannelsCount + 1;

        TPartitionContent baseContent = {
            TBlob(1, 1, 1, baseBlobChannel)
        };
        auto bitmap = CreateBitmap(1);

        auto partitionWithRuntime =
            SetupOverlayPartition(
                TestTabletId,
                TestTabletId2,
                baseContent,
                overlayTabletChannelsCount);
        auto& partition = *partitionWithRuntime.Partition;

        auto response = partition.ReadBlocks(0);

        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(char(1)),
            GetBlocksContent(response));
        UNIT_ASSERT(bitmap == GetUnencryptedBlockMask(response));
    }

    Y_UNIT_TEST(ShouldSendBlocksCountToReadInDescribeBlocksRequest)
    {
        auto partitionWithRuntime =
            SetupOverlayPartition(TestTabletId, TestTabletId2, {});
        auto& partition = *partitionWithRuntime.Partition;
        auto& runtime = *partitionWithRuntime.Runtime;

        partition.WriteBlocks(4, 1);
        partition.WriteBlocks(5, 1);

        int describeBlocksCount = 0;

        runtime.SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) {
                switch (event->GetTypeRewrite()) {
                    case TEvVolume::EvDescribeBlocksRequest: {
                        auto* msg = event->Get<TEvVolume::TEvDescribeBlocksRequest>();
                        auto& record = msg->Record;
                        UNIT_ASSERT_VALUES_EQUAL(
                            7,
                            record.GetBlocksCountToRead()
                        );
                        ++describeBlocksCount;
                        break;
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        partition.ReadBlocks(TBlockRange32::WithLength(0, 9));
        UNIT_ASSERT_VALUES_EQUAL(1, describeBlocksCount);
    }

    Y_UNIT_TEST(ShouldGetUsedBlocks)
    {
        constexpr ui32 blockCount = 1024 * 1024;
        auto runtime = PrepareTestActorRuntime(DefaultConfig(), blockCount);
        TPartitionClient partition(*runtime);
        partition.WaitReady();

        partition.WriteBlocks(TBlockRange32::WithLength(0, 2), 1);
        partition.WriteBlocks(TBlockRange32::WithLength(0, 1024), 1);
        partition.WriteBlocks(TBlockRange32::WithLength(2048, 2), 1);
        partition.WriteBlocks(TBlockRange32::WithLength(1024 * 10, 1025), 1);
        partition.WriteBlocks(TBlockRange32::WithLength(1024 * 512, 1025), 1);

        ui64 expected = 1024 + 2 + 1024 + 1 + 1024 + 1;

        auto response1 = partition.GetUsedBlocks();
        UNIT_ASSERT(SUCCEEDED(response1->GetStatus()));

        TCompressedBitmap bitmap(blockCount);

        for (const auto& block : response1->Record.GetUsedBlocks()) {
            bitmap.Merge(TCompressedBitmap::TSerializedChunk{
                block.GetChunkIdx(),
                block.GetData()
            });
        }

        UNIT_ASSERT_EQUAL(expected, bitmap.Count());
    }

    Y_UNIT_TEST(ShouldFailReadBlocksWhenSglistHolderIsDestroyed) {
        auto runtime = PrepareTestActorRuntime();
        TPartitionClient partition(*runtime);
        partition.WaitReady();
        partition.WriteBlocks(0, 1);

        // Test with fresh blocks.
        {
            TGuardedSgList sglist(TSgList{{}});
            sglist.Close();
            partition.SendReadBlocksLocalRequest(0, sglist);

            auto response = partition.RecvReadBlocksLocalResponse();
            UNIT_ASSERT(FAILED(response->GetStatus()));
        }

        partition.Flush();

        // Test with blob.
        {
            TGuardedSgList sglist(TSgList{{}});
            sglist.Close();
            partition.SendReadBlocksLocalRequest(0, sglist);

            auto response = partition.RecvReadBlocksLocalResponse();
            UNIT_ASSERT(FAILED(response->GetStatus()));
        }
    }

    Y_UNIT_TEST(ShouldReturnErrorWhenReadingFromUnknownCheckpoint)
    {
        auto runtime = PrepareTestActorRuntime();

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        partition.SendReadBlocksRequest(TBlockRange32::MakeOneBlock(0), "unknown");

        auto response = partition.RecvReadBlocksResponse();
        UNIT_ASSERT(FAILED(response->GetStatus()));
    }

    Y_UNIT_TEST(ShouldReturnErrorIfCompactionIsAlreadyRunning)
    {
        auto runtime = PrepareTestActorRuntime(DefaultConfig(), 2048);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        partition.WriteBlocks(TBlockRange32::WithLength(0, 1024));
        partition.WriteBlocks(TBlockRange32::WithLength(1024,1024));

        TAutoPtr<IEventHandle> addBlobs;
        runtime->SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) {
                switch (event->GetTypeRewrite()) {
                    case TEvPartitionPrivate::EvAddBlobsRequest: {
                        if (!addBlobs) {
                            addBlobs = event.Release();
                            return TTestActorRuntime::EEventAction::DROP;
                        }
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            });


        partition.SendCompactionRequest(0);

        partition.SendCompactionRequest(1024);

        {
            auto compactResponse = partition.RecvCompactionResponse();
            UNIT_ASSERT(FAILED(compactResponse->GetStatus()));
            UNIT_ASSERT_VALUES_EQUAL(E_TRY_AGAIN, compactResponse->GetStatus());
        }

        runtime->SetObserverFunc(&TTestActorRuntimeBase::DefaultObserverFunc);
        runtime->Send(addBlobs.Release());

        {
            auto compactResponse = partition.RecvCompactionResponse();
            UNIT_ASSERT(SUCCEEDED(compactResponse->GetStatus()));
        }

        {
            partition.SendCompactionRequest(0);
            auto compactResponse = partition.RecvCompactionResponse();
            UNIT_ASSERT_VALUES_EQUAL(S_OK, compactResponse->GetStatus());
        }
    }

    Y_UNIT_TEST(ShouldStartCompactionOnCompactRagesRequest)
    {
        auto runtime = PrepareTestActorRuntime();

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        partition.WriteBlocks(0, 100);
        partition.SendCompactRangeRequest(0, 100);

        auto response = partition.RecvCompactRangeResponse();
        UNIT_ASSERT(SUCCEEDED(response->GetStatus()));
        UNIT_ASSERT_VALUES_EQUAL(
            false,
            response->Record.GetOperationId().empty()
        );
    }

    Y_UNIT_TEST(ShouldReturnErrorForUnknownIdInCompactionStatusRequest)
    {
        auto runtime = PrepareTestActorRuntime();

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        partition.WriteBlocks(0, 100);
        partition.SendGetCompactionStatusRequest("xxx");

        auto response = partition.RecvGetCompactionStatusResponse();
        UNIT_ASSERT(FAILED(response->GetStatus()));
    }

    Y_UNIT_TEST(ShouldReturnCompactionProgress)
    {
        auto runtime = PrepareTestActorRuntime();

        TActorId rangeActor;
        TActorId partActor;
        runtime->SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) {
                switch (event->GetTypeRewrite()) {
                    case TEvPartitionPrivate::EvCompactionRequest: {
                        rangeActor = event->Sender;
                        partActor = event->Recipient;
                        return TTestActorRuntime::EEventAction::DROP ;
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            });


        TPartitionClient partition(*runtime);
        partition.WaitReady();

        partition.WriteBlocks(0, 100);
        auto compResponse = partition.CompactRange(0, 100);

        partition.SendGetCompactionStatusRequest(compResponse->Record.GetOperationId());

        auto statusResponse1 = partition.RecvGetCompactionStatusResponse();
        UNIT_ASSERT(SUCCEEDED(statusResponse1->GetStatus()));
        UNIT_ASSERT_VALUES_EQUAL(
            false,
            statusResponse1->Record.GetIsCompleted()
        );
        UNIT_ASSERT_VALUES_EQUAL(1, statusResponse1->Record.GetTotal());

        auto compactionResponse = std::make_unique<TEvPartitionPrivate::TEvCompactionResponse>();
        runtime->Send(
            new IEventHandle(
                rangeActor,
                partActor,
                compactionResponse.release(),
                0, // flags
                0),
            0);

        partition.SendGetCompactionStatusRequest(compResponse->Record.GetOperationId());

        auto statusResponse2 = partition.RecvGetCompactionStatusResponse();
        UNIT_ASSERT(SUCCEEDED(statusResponse2->GetStatus()));
        UNIT_ASSERT_VALUES_EQUAL(true, statusResponse2->Record.GetIsCompleted());
        UNIT_ASSERT_VALUES_EQUAL(1, statusResponse2->Record.GetTotal());
    }

    Y_UNIT_TEST(ShouldReturnCompactionStatusForLastCompletedCompaction)
    {
        auto runtime = PrepareTestActorRuntime();

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        partition.WriteBlocks(0, 100);
        auto compResponse = partition.CompactRange(0, 100);

        auto statusResponse = partition.GetCompactionStatus(
            compResponse->Record.GetOperationId());

        UNIT_ASSERT_VALUES_EQUAL(true, statusResponse->Record.GetIsCompleted());
        UNIT_ASSERT_VALUES_EQUAL(1, statusResponse->Record.GetTotal());
    }

    Y_UNIT_TEST(ShouldUseWriteBlobThreshold)
    {
        auto config = DefaultConfig();
        config.SetWriteBlobThreshold(1_MB);
        config.SetWriteBlobThresholdSSD(128_KB);
        TTestPartitionInfo testPartitionInfo;

        const auto mediaKinds = {
            NCloud::NProto::STORAGE_MEDIA_HDD,
            NCloud::NProto::STORAGE_MEDIA_HYBRID
        };

        for (auto mediaKind: mediaKinds) {
            testPartitionInfo.MediaKind = mediaKind;
            auto runtime = PrepareTestActorRuntime(
                config,
                1024,
                {},
                testPartitionInfo,
                {},
                EStorageAccessMode::Default
            );

            TPartitionClient partition(*runtime);
            partition.WaitReady();

            partition.WriteBlocks(TBlockRange32::WithLength(0, 255));

            {
                auto response = partition.StatPartition();
                const auto& stats = response->Record.GetStats();
                UNIT_ASSERT_VALUES_EQUAL(255, stats.GetFreshBlocksCount());
                UNIT_ASSERT_VALUES_EQUAL(0, stats.GetMixedBlocksCount());
                UNIT_ASSERT_VALUES_EQUAL(0, stats.GetMergedBlocksCount());
            }

            partition.WriteBlocks(TBlockRange32::WithLength(0, 256));

            {
                auto response = partition.StatPartition();
                const auto& stats = response->Record.GetStats();
                UNIT_ASSERT_VALUES_EQUAL(255, stats.GetFreshBlocksCount());
                UNIT_ASSERT_VALUES_EQUAL(0, stats.GetMixedBlocksCount());
                UNIT_ASSERT_VALUES_EQUAL(256, stats.GetMergedBlocksCount());
            }

            partition.ZeroBlocks(TBlockRange32::WithLength(0, 255));

            {
                auto response = partition.StatPartition();
                const auto& stats = response->Record.GetStats();
                UNIT_ASSERT_VALUES_EQUAL(255, stats.GetFreshBlocksCount());
                UNIT_ASSERT_VALUES_EQUAL(0, stats.GetMixedBlocksCount());
                UNIT_ASSERT_VALUES_EQUAL(256, stats.GetMergedBlocksCount());
            }

            partition.ZeroBlocks(TBlockRange32::WithLength(0, 256));

            {
                auto response = partition.StatPartition();
                const auto& stats = response->Record.GetStats();
                UNIT_ASSERT_VALUES_EQUAL(255, stats.GetFreshBlocksCount());
                UNIT_ASSERT_VALUES_EQUAL(0, stats.GetMixedBlocksCount());
                UNIT_ASSERT_VALUES_EQUAL(256, stats.GetMergedBlocksCount());
            }
        }

        {
            testPartitionInfo.MediaKind = NCloud::NProto::STORAGE_MEDIA_SSD;
            auto runtime = PrepareTestActorRuntime(
                config,
                1024,
                {},
                testPartitionInfo,
                {},
                EStorageAccessMode::Default
            );

            TPartitionClient partition(*runtime);
            partition.WaitReady();

            partition.WriteBlocks(TBlockRange32::WithLength(0, 31));

            {
                auto response = partition.StatPartition();
                const auto& stats = response->Record.GetStats();
                UNIT_ASSERT_VALUES_EQUAL(31, stats.GetFreshBlocksCount());
                UNIT_ASSERT_VALUES_EQUAL(0, stats.GetMixedBlocksCount());
                UNIT_ASSERT_VALUES_EQUAL(0, stats.GetMergedBlocksCount());
            }

            partition.WriteBlocks(TBlockRange32::WithLength(0, 32));

            {
                auto response = partition.StatPartition();
                const auto& stats = response->Record.GetStats();
                UNIT_ASSERT_VALUES_EQUAL(31, stats.GetFreshBlocksCount());
                UNIT_ASSERT_VALUES_EQUAL(0, stats.GetMixedBlocksCount());
                UNIT_ASSERT_VALUES_EQUAL(32, stats.GetMergedBlocksCount());
            }

            partition.ZeroBlocks(TBlockRange32::WithLength(0, 31));

            {
                auto response = partition.StatPartition();
                const auto& stats = response->Record.GetStats();
                UNIT_ASSERT_VALUES_EQUAL(31, stats.GetFreshBlocksCount());
                UNIT_ASSERT_VALUES_EQUAL(0, stats.GetMixedBlocksCount());
                UNIT_ASSERT_VALUES_EQUAL(32, stats.GetMergedBlocksCount());
            }

            partition.ZeroBlocks(TBlockRange32::WithLength(0, 32));

            {
                auto response = partition.StatPartition();
                const auto& stats = response->Record.GetStats();
                UNIT_ASSERT_VALUES_EQUAL(31, stats.GetFreshBlocksCount());
                UNIT_ASSERT_VALUES_EQUAL(0, stats.GetMixedBlocksCount());
                UNIT_ASSERT_VALUES_EQUAL(32, stats.GetMergedBlocksCount());
            }
        }
    }

    Y_UNIT_TEST(ShouldProperlyProcessDeletionMarkers)
    {
        auto config = DefaultConfig();
        config.SetWriteBlobThreshold(1_MB);

        auto runtime = PrepareTestActorRuntime(config);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        partition.WriteBlocks(TBlockRange32::WithLength(0, 1024));
        partition.ZeroBlocks(TBlockRange32::WithLength(0, 1024));

        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetMixedBlocksCount());
            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetMixedBlobsCount());

            UNIT_ASSERT_VALUES_EQUAL(1024, stats.GetMergedBlocksCount());
            UNIT_ASSERT_VALUES_EQUAL(2, stats.GetMergedBlobsCount());
        }

        partition.Compaction();
        partition.Cleanup();

        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetMixedBlocksCount());
            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetMixedBlobsCount());

            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetMergedBlocksCount());
            UNIT_ASSERT_VALUES_EQUAL(1, stats.GetMergedBlobsCount());
        }

        partition.WriteBlocks(TBlockRange32::WithLength(0, 100));
        partition.ZeroBlocks(TBlockRange32::WithLength(0, 100));
        partition.Flush();
        partition.Flush();

        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();

            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetMixedBlocksCount());
            UNIT_ASSERT_VALUES_EQUAL(1, stats.GetMixedBlobsCount());

            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetMergedBlocksCount());
            UNIT_ASSERT_VALUES_EQUAL(1, stats.GetMergedBlobsCount());
        }

        partition.Compaction();
        partition.Cleanup();

        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetMixedBlocksCount());
            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetMixedBlobsCount());

            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetMergedBlocksCount());
            UNIT_ASSERT_VALUES_EQUAL(1, stats.GetMergedBlobsCount());
        }
    }

    Y_UNIT_TEST(ShouldHandleHttpCollectGarbage)
    {
        auto runtime = PrepareTestActorRuntime();

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        auto createResponse = partition.RemoteHttpInfo(
            BuildRemoteHttpQuery(TestTabletId, {{"action","collectGarbage"}}),
            HTTP_METHOD::HTTP_METHOD_POST);

        UNIT_ASSERT_C(
            createResponse->Html.Contains("Operation successfully completed"),
            true
        );
    }

    Y_UNIT_TEST(ShouldFailsHttpGetCollectGarbage)
    {
        auto runtime = PrepareTestActorRuntime();

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        auto createResponse = partition.RemoteHttpInfo(
            BuildRemoteHttpQuery(TestTabletId, {{"action","collectGarbage"}}));

        UNIT_ASSERT_C(createResponse->Html.Contains("Wrong HTTP method"), true);
    }

    Y_UNIT_TEST(ShouldFailHttpCollectGarbageOnTabletRestart)
    {
        auto runtime = PrepareTestActorRuntime();

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        bool patchRequest = true;
        runtime->SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) {
                if (event->GetTypeRewrite() == TEvPartitionPrivate::EvCollectGarbageRequest) {
                    if (patchRequest) {
                        patchRequest = false;
                        auto request = std::make_unique<TEvPartitionPrivate::TEvCollectGarbageRequest>();
                        SendUndeliverableRequest(*runtime, event, std::move(request));
                        return TTestActorRuntime::EEventAction::DROP;
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        auto httpResponse = partition.RemoteHttpInfo(
            BuildRemoteHttpQuery(TestTabletId, {{"action","collectGarbage"}}),
            HTTP_METHOD::HTTP_METHOD_POST);

        UNIT_ASSERT_C(httpResponse->Html.Contains("tablet is shutting down"), true);
    }

    Y_UNIT_TEST(ShouldForgetTooOldCompactRangeOperations)
    {
        constexpr TDuration CompactOpHistoryDuration = TDuration::Days(1);

        auto runtime = PrepareTestActorRuntime();

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        TVector<TString> op;

        for (ui32 i = 0; i < 4; ++i)
        {
            partition.WriteBlocks(0, 100);
            partition.SendCompactRangeRequest(0, 100);

            {
                TDispatchOptions options;
                options.FinalEvents.emplace_back(
                    TEvPartitionPrivate::EvForcedCompactionCompleted,
                    1);
                runtime->DispatchEvents(options);
            }

            auto response = partition.RecvCompactRangeResponse();
            UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());
            op.push_back(response->Record.GetOperationId());
        }

        runtime->AdvanceCurrentTime(CompactOpHistoryDuration + TDuration::Seconds(1));

        for (ui32 i = 0; i < 4; ++i)
        {
            partition.SendGetCompactionStatusRequest(op[i]);
            auto response = partition.RecvGetCompactionStatusResponse();
            UNIT_ASSERT_VALUES_EQUAL(E_NOT_FOUND, response->GetStatus());
        }
    }

    Y_UNIT_TEST(ShouldDrain)
    {
        auto config = DefaultConfig();
        config.SetWriteBlobThreshold(1_MB);
        auto runtime = PrepareTestActorRuntime(config);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        // drain before any requests should work
        partition.Drain();

        partition.WriteBlocks(TBlockRange32::MakeOneBlock(0));       // fresh
        partition.WriteBlocks(TBlockRange32::WithLength(0, 1024));   // blob
        partition.ZeroBlocks(TBlockRange32::MakeOneBlock(0));        // fresh
        partition.ZeroBlocks(TBlockRange32::WithLength(0, 1024));    // blob

        // drain after some requests have completed should work
        partition.Drain();

        TDeque<std::unique_ptr<IEventHandle>> evPutRequests;
        bool intercept = true;
        runtime->SetObserverFunc([&] (TAutoPtr<IEventHandle>& event)
            {
                if (intercept) {
                    switch (event->GetTypeRewrite()) {
                        case TEvBlobStorage::EvPutResult: {
                            evPutRequests.emplace_back(event.Release());
                            return TTestActorRuntime::EEventAction::DROP;
                        }
                    }
                }

                return TTestActorRuntime::DefaultObserverFunc(event);
            }
        );

        auto test = [&] (TString testName, bool isWrite)
        {
            runtime->DispatchEvents({}, TDuration::Seconds(1));

            // sending this request after DispatchEvents because our pipe client
            // reconnects from time to time and thus fifo guarantee for
            // zero/write -> drain is broken => we need to ensure the correct
            // order (zero/write, then drain) in our test scenario
            partition.SendDrainRequest();

            UNIT_ASSERT_C(
                evPutRequests.size(),
                TStringBuilder() << testName << ": intercepted EvPut requests"
            );

            auto evList = runtime->CaptureEvents();
            for (auto& ev: evList) {
                UNIT_ASSERT_C(
                    ev->GetTypeRewrite() != TEvPartition::EvDrainResponse,
                    TStringBuilder() << testName << ": check no drain response"
                );
            }
            runtime->PushEventsFront(evList);

            intercept = false;

            for (auto& request: evPutRequests) {
                runtime->Send(request.release());
            }

            evPutRequests.clear();

            runtime->DispatchEvents({}, TDuration::Seconds(1));

            if (isWrite) {
                {
                    auto response = partition.RecvWriteBlocksResponse();
                    UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());
                }
            } else {
                {
                    auto response = partition.RecvZeroBlocksResponse();
                    UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());
                }
            }

            {
                auto response = partition.RecvDrainResponse();
                UNIT_ASSERT_VALUES_EQUAL_C(
                    S_OK,
                    response->GetStatus(),
                    TStringBuilder() << testName << ": check drain response"
                );
            }

            intercept = true;
        };

        partition.SendWriteBlocksRequest(TBlockRange32::MakeOneBlock(0));
        test("write fresh", true);

        partition.SendWriteBlocksRequest(TBlockRange32::WithLength(0, 1024));
        test("write blob", true);

        partition.SendZeroBlocksRequest(TBlockRange32::MakeOneBlock(0));
        test("zero fresh", false);

        partition.SendZeroBlocksRequest(TBlockRange32::WithLength(0, 1024));
        test("zero blob", false);
    }

    Y_UNIT_TEST(ShouldProperlyHandleCollectGarbageErrors)
    {
        const auto channelCount = 6;
        const auto groupCount = channelCount - DataChannelOffset;

        TTestEnv env(0, 1, channelCount, groupCount);
        auto& runtime = env.GetRuntime();
        auto tabletId = InitTestActorRuntime(env, runtime, channelCount, channelCount);

        TPartitionClient partition(runtime, 0, tabletId);
        partition.WaitReady();

        bool channel3requestObserved = false;
        bool channel4requestObserved = false;
        bool channel4responseObserved = false;
        bool deleteGarbageObserved = false;
        bool sendError = true;

        runtime.SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) {
                switch (event->GetTypeRewrite()) {
                    case TEvBlobStorage::EvCollectGarbage: {
                        auto* msg = event->Get<TEvBlobStorage::TEvCollectGarbage>();
                        if (3 == msg->Channel) {
                            channel3requestObserved = true;
                            if (sendError) {
                                auto response =
                                    std::make_unique<TEvBlobStorage::TEvCollectGarbageResult>(
                                        NKikimrProto::ERROR,
                                        0,  // doesn't matter
                                        0,  // doesn't matter
                                        0,  // doesn't matter
                                        msg->Channel
                                    );

                                runtime.Send(new IEventHandle(
                                    event->Sender,
                                    event->Recipient,
                                    response.release(),
                                    0, // flags
                                    0
                                ), 0);

                                return TTestActorRuntime::EEventAction::DROP;
                            }
                        } else if (4 == msg->Channel) {
                            channel4requestObserved = true;
                        }

                        break;
                    }

                    case TEvBlobStorage::EvCollectGarbageResult: {
                        auto* msg = event->Get<TEvBlobStorage::TEvCollectGarbageResult>();
                        if (4 == msg->Channel) {
                            channel4responseObserved = true;
                        }

                        break;
                    }

                    case TEvPartitionPrivate::EvDeleteGarbageRequest: {
                        deleteGarbageObserved = true;

                        break;
                    }
                }

                return TTestActorRuntime::DefaultObserverFunc(event);
            }
        );

        // 10 blobs needed to trigger automatic collect
        for (ui32 i = 0; i < 9; ++i) {
            partition.WriteBlocks(TBlockRange32::WithLength(0, 1024), 1);
        }
        partition.Compaction();
        partition.Cleanup();
        partition.SendCollectGarbageRequest();
        {
            auto response = partition.RecvCollectGarbageResponse();
            UNIT_ASSERT_VALUES_EQUAL(
                MAKE_KIKIMR_ERROR(NKikimrProto::ERROR),
                response->GetStatus()
            );
        }
        UNIT_ASSERT(channel3requestObserved);
        UNIT_ASSERT(channel4requestObserved);
        UNIT_ASSERT(channel4responseObserved);
        UNIT_ASSERT(!deleteGarbageObserved);
        channel3requestObserved = false;
        channel4requestObserved = false;
        channel4responseObserved = false;
        sendError = false;

        runtime.AdvanceCurrentTime(TDuration::Seconds(10));
        runtime.DispatchEvents(TDispatchOptions(), TDuration::MilliSeconds(10));

        UNIT_ASSERT(channel3requestObserved);
        UNIT_ASSERT(channel4requestObserved);
        UNIT_ASSERT(channel4responseObserved);
        UNIT_ASSERT(deleteGarbageObserved);
    }

    Y_UNIT_TEST(ShouldExecuteCollectGarbageAtStartup)
    {
        const auto channelCount = 7;
        const auto groupCount = channelCount - DataChannelOffset;

        auto isDataChannel = [&] (ui64 ch) {
            return (ch >= DataChannelOffset) && (ch < channelCount);
        };

        NProto::TStorageServiceConfig config = DefaultConfig();

        TTestEnv env(0, 1, channelCount, groupCount);
        auto& runtime = env.GetRuntime();
        const auto tabletId = InitTestActorRuntime(env, runtime, channelCount, channelCount, config);

        TPartitionClient partition(runtime, 0, tabletId);

        ui32 gcRequests = 0;
        bool deleteGarbageSeen = false;

        NActors::TActorId gcActor = {};

        runtime.SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) {
                switch (event->GetTypeRewrite()) {
                    case TEvBlobStorage::EvCollectGarbage: {
                        auto* msg = event->Get<TEvBlobStorage::TEvCollectGarbage>();
                        if (msg->TabletId == tabletId &&
                            isDataChannel(msg->Channel))
                        {
                            if (!gcActor || gcActor == event->Sender) {
                                gcActor = event->Sender;
                                ++gcRequests;
                            }
                        }
                        break;
                    }
                    case TEvPartitionPrivate::EvDeleteGarbageRequest: {
                        deleteGarbageSeen = true;
                        break;
                    }
                }

                return TTestActorRuntime::DefaultObserverFunc(event);
            }
        );

        {
            TDispatchOptions options;
            options.FinalEvents.emplace_back(TEvPartitionPrivate::EvCollectGarbageResponse);
            runtime.DispatchEvents(options);
        }

        UNIT_ASSERT_VALUES_EQUAL(3, gcRequests);
        UNIT_ASSERT_VALUES_EQUAL(false, deleteGarbageSeen);
    }

    Y_UNIT_TEST(ShouldNotTrimInFlightBlocks)
    {
        auto config = DefaultConfig();
        config.SetFreshChannelCount(1);
        config.SetFreshChannelWriteRequestsEnabled(true);

        auto runtime = PrepareTestActorRuntime(config);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        partition.WriteBlocks(1, 1);

        TAutoPtr<IEventHandle> addFreshBlocks;

        runtime->SetObserverFunc([&] (TAutoPtr<IEventHandle>& event)
            {
                switch (event->GetTypeRewrite()) {
                    case TEvPartitionPrivate::EvAddFreshBlocksRequest: {
                        if (!addFreshBlocks) {
                            addFreshBlocks = event.Release();
                            return TTestActorRuntime::EEventAction::DROP;
                        }
                        break;
                    }
                }

                return TTestActorRuntime::DefaultObserverFunc(event);
            }
        );

        partition.SendWriteBlocksRequest(2, 2);

        runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        partition.WriteBlocks(3, 3);

        partition.Flush();
        partition.TrimFreshLog();

        UNIT_ASSERT(addFreshBlocks);
        runtime->Send(addFreshBlocks.Release());

        runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        partition.RebootTablet();

        auto response = partition.ReadBlocks(1);
        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(char(1)),
            GetBlocksContent(response)
        );

        response = partition.ReadBlocks(2);
        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(char(2)),
            GetBlocksContent(response)
        );

        response = partition.ReadBlocks(3);
        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(char(3)),
            GetBlocksContent(response)
        );
    }

    Y_UNIT_TEST(ShouldNotTrimUnflushedBlocksWhileThereIsFlushedBlockWithLargerCommitId)
    {
        auto config = DefaultConfig();
        config.SetFreshChannelCount(1);
        config.SetFreshChannelWriteRequestsEnabled(true);

        auto runtime = PrepareTestActorRuntime(config);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        partition.WriteBlocks(1, 1);

        TAutoPtr<IEventHandle> addFreshBlocks;

        runtime->SetObserverFunc([&] (TAutoPtr<IEventHandle>& event)
            {
                switch (event->GetTypeRewrite()) {
                    case TEvPartitionPrivate::EvAddFreshBlocksRequest: {
                        if (!addFreshBlocks) {
                            addFreshBlocks = event.Release();
                            return TTestActorRuntime::EEventAction::DROP;
                        }
                        break;
                    }
                }

                return TTestActorRuntime::DefaultObserverFunc(event);
            }
        );

        partition.SendWriteBlocksRequest(2, 2);

        runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        partition.WriteBlocks(3, 3);

        partition.Flush();

        UNIT_ASSERT(addFreshBlocks);
        runtime->Send(addFreshBlocks.Release());

        runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        partition.TrimFreshLog();

        partition.RebootTablet();

        auto response = partition.ReadBlocks(1);
        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(char(1)),
            GetBlocksContent(response)
        );

        response = partition.ReadBlocks(2);
        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(char(2)),
            GetBlocksContent(response)
        );

        response = partition.ReadBlocks(3);
        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(char(3)),
            GetBlocksContent(response)
        );
    }

    Y_UNIT_TEST(ShouldReleaseTrimBarrierOnBlockDeletion)
    {
        auto config = DefaultConfig();
        config.SetFreshChannelCount(1);
        config.SetFreshChannelWriteRequestsEnabled(true);

        auto runtime = PrepareTestActorRuntime(config);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        partition.WriteBlocks(TBlockRange32::WithLength(1, 2));
        partition.WriteBlocks(TBlockRange32::WithLength(1, 2));

        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(2, stats.GetFreshBlocksCount());
        }

        partition.Flush();
        partition.TrimFreshLog();

        partition.RebootTablet();

        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetFreshBlocksCount());
        }
    }

    Y_UNIT_TEST(ShouldHandleFlushCorrectlyWhileBlockFromFreshChannelIsBeingDeleted)
    {
        auto config = DefaultConfig();
        config.SetFreshChannelCount(1);
        config.SetFreshChannelWriteRequestsEnabled(true);

        auto runtime = PrepareTestActorRuntime(config);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        partition.WriteBlocks(TBlockRange32::WithLength(1, 5));

        TAutoPtr<IEventHandle> addBlobsRequest;

        runtime->SetObserverFunc([&] (TAutoPtr<IEventHandle>& event)
            {
                if (event->GetTypeRewrite() == TEvPartitionPrivate::EvAddBlobsRequest) {
                    addBlobsRequest = event.Release();
                    return TTestActorRuntime::EEventAction::DROP;
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            }
        );

        partition.SendFlushRequest();

        runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        partition.WriteBlocks(TBlockRange32::WithLength(1, 5));

        UNIT_ASSERT(addBlobsRequest);
        runtime->Send(addBlobsRequest.Release());

        runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        auto response = partition.RecvFlushResponse();
        UNIT_ASSERT(SUCCEEDED(response->GetStatus()));
    }

    Y_UNIT_TEST(ShouldHandleFlushCorrectlyWhileBlockFromDbIsBeingDeleted)
    {
        auto config = DefaultConfig();
        config.SetFreshChannelCount(0);
        config.SetFreshChannelWriteRequestsEnabled(false);

        auto runtime = PrepareTestActorRuntime(config);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        partition.WriteBlocks(TBlockRange32::WithLength(1, 5));

        TAutoPtr<IEventHandle> addBlobsRequest;

        runtime->SetObserverFunc([&] (TAutoPtr<IEventHandle>& event)
            {
                if (event->GetTypeRewrite() == TEvPartitionPrivate::EvAddBlobsRequest) {
                    addBlobsRequest = event.Release();
                    return TTestActorRuntime::EEventAction::DROP;
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            }
        );

        partition.SendFlushRequest();

        runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        partition.WriteBlocks(TBlockRange32::WithLength(1, 5));

        UNIT_ASSERT(addBlobsRequest);
        runtime->Send(addBlobsRequest.Release());

        runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        auto response = partition.RecvFlushResponse();
        UNIT_ASSERT(SUCCEEDED(response->GetStatus()));
    }

    Y_UNIT_TEST(ShouldCorrectlyHandleTabletInfoChannelCountMoreThanConfigChannelCount)
    {
        constexpr ui32 channelCount = 7;
        constexpr ui32 tabletInfoChannelCount = 11;

        TTestEnv env(0, 1, tabletInfoChannelCount, 4);
        auto& runtime = env.GetRuntime();

        const auto tabletId = InitTestActorRuntime(
            env,
            runtime,
            channelCount,
            tabletInfoChannelCount);

        TPartitionClient partition(runtime, 0, tabletId);
        partition.WaitReady();

        for (ui32 i = 0; i < 30; ++i) {
            partition.WriteBlocks(TBlockRange32::WithLength(0, 1024), 1);
        }

        partition.Compaction();
        partition.CollectGarbage();
    }

    Y_UNIT_TEST(ShouldCorrectlyHandleTabletInfoChannelCountLessThanConfigChannelCount)
    {
        constexpr ui32 channelCount = 11;
        constexpr ui32 tabletInfoChannelCount = 7;

        TTestEnv env(0, 1, tabletInfoChannelCount, 4);
        auto& runtime = env.GetRuntime();

        const auto tabletId = InitTestActorRuntime(
            env,
            runtime,
            channelCount,
            tabletInfoChannelCount);

        TPartitionClient partition(runtime, 0, tabletId);
        partition.WaitReady();

        for (ui32 i = 0; i < 30; ++i) {
            partition.WriteBlocks(TBlockRange32::WithLength(0, 1024), 1);
        }

        partition.Compaction();
        partition.CollectGarbage();
    }

    Y_UNIT_TEST(ShouldHandleBSErrorsOnInitFreshBlocksFromChannel)
    {
        auto config = DefaultConfig();
        config.SetFreshChannelCount(1);
        config.SetFreshChannelWriteRequestsEnabled(true);

        auto runtime = PrepareTestActorRuntime(config);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        partition.WriteBlocks(TBlockRange32::MakeOneBlock(0), 1);
        partition.WriteBlocks(TBlockRange32::MakeOneBlock(1), 2);
        partition.WriteBlocks(TBlockRange32::MakeOneBlock(2), 3);

        bool evRangeResultSeen = false;

        ui32 evLoadFreshBlobsCompletedCount = 0;

        runtime->SetEventFilter(
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
                    default: {
                        return false;
                    }
                }
            }
        );

        partition.RebootTablet();
        partition.WaitReady();

        UNIT_ASSERT_VALUES_EQUAL(true, evRangeResultSeen);

        // tablet rebooted twice (after explicit RebootTablet() and on fail)
        UNIT_ASSERT_VALUES_EQUAL(2, evLoadFreshBlobsCompletedCount);

        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(1),
            GetBlockContent(partition.ReadBlocks(0)));
        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(2),
            GetBlockContent(partition.ReadBlocks(1)));
        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(3),
            GetBlockContent(partition.ReadBlocks(2)));
    }

    Y_UNIT_TEST(ShouldFillEnryptedBlockMaskWhenReadBlock)
    {
        auto range = TBlockRange32::WithLength(0, 16);
        auto emptyBlock = TString::Uninitialized(DefaultBlockSize);

        auto bitmap = CreateBitmap(16);

        auto runtime = PrepareTestActorRuntime();

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        {
            partition.Flush();

            auto response = partition.ReadBlocks(range);
            UNIT_ASSERT(bitmap == GetUnencryptedBlockMask(response));

            TVector<TString> blocks;
            auto sglist = ResizeBlocks(blocks, range.Size(), emptyBlock);
            auto localResponse = partition.ReadBlocksLocal(range, sglist);
            UNIT_ASSERT(bitmap == GetUnencryptedBlockMask(localResponse));
        }

        {
            auto writeRange = TBlockRange32::WithLength(1, 4);
            partition.WriteBlocks(writeRange, char(4));
            MarkWrittenBlocks(bitmap, writeRange);

            auto zeroRange = TBlockRange32::WithLength(5, 3);
            partition.ZeroBlocks(zeroRange);
            MarkZeroedBlocks(bitmap, zeroRange);

            auto writeRangeLocal = TBlockRange32::WithLength(8, 3);
            partition.WriteBlocksLocal(writeRangeLocal, GetBlockContent(4));
            MarkWrittenBlocks(bitmap, writeRangeLocal);

            auto zeroRangeLocal = TBlockRange32::WithLength(12, 3);
            partition.ZeroBlocks(zeroRangeLocal);
            MarkZeroedBlocks(bitmap, zeroRangeLocal);

            partition.Flush();

            auto response = partition.ReadBlocks(range);
            UNIT_ASSERT(bitmap == GetUnencryptedBlockMask(response));

            TVector<TString> blocks;
            auto sglist = ResizeBlocks(blocks, range.Size(), emptyBlock);
            auto localResponse = partition.ReadBlocksLocal(range, sglist);
            UNIT_ASSERT(bitmap == GetUnencryptedBlockMask(localResponse));
        }

        {
            auto zeroRange = TBlockRange32::WithLength(1, 3);
            partition.ZeroBlocks(zeroRange);
            MarkZeroedBlocks(bitmap, zeroRange);

            auto writeRange = TBlockRange32::WithLength(5, 3);
            partition.WriteBlocks(writeRange, char(4));
            MarkWrittenBlocks(bitmap, writeRange);

            auto zeroRangeLocal = TBlockRange32::WithLength(8, 3);
            partition.ZeroBlocks(zeroRangeLocal);
            MarkZeroedBlocks(bitmap, zeroRangeLocal);

            auto writeRangeLocal = TBlockRange32::WithLength(12, 3);
            partition.WriteBlocksLocal(writeRangeLocal, GetBlockContent(4));
            MarkWrittenBlocks(bitmap, writeRangeLocal);

            partition.Flush();

            auto response = partition.ReadBlocks(range);
            UNIT_ASSERT(bitmap == GetUnencryptedBlockMask(response));

            TVector<TString> blocks;
            auto sglist = ResizeBlocks(blocks, range.Size(), emptyBlock);
            auto localResponse = partition.ReadBlocksLocal(range, sglist);
            UNIT_ASSERT(bitmap == GetUnencryptedBlockMask(localResponse));
        }
    }

    Y_UNIT_TEST(ShouldFillEnryptedBlockMaskWhenReadBlockFromOverlayDisk)
    {
        TPartitionContent baseContent = {
        /*|    0     |    1    |    2     |    3 ... 5    |    6     |    7    |    8    |    9     |    10...12    |    13    |   14    |    15    |*/
            TFresh(1), TEmpty(), TFresh(2), TBlob(2, 3, 3), TFresh(4), TEmpty(), TEmpty(), TFresh(5), TBlob(3, 6, 3), TFresh(7), TEmpty(), TFresh(8)
        };

        auto range = TBlockRange32::WithLength(0, 16);
        auto emptyBlock = TString::Uninitialized(DefaultBlockSize);

        auto bitmap = CreateBitmap(16);

        auto partitionWithRuntime =
            SetupOverlayPartition(TestTabletId, TestTabletId2, baseContent);
        auto& partition = *partitionWithRuntime.Partition;

        {
            partition.Flush();

            auto response = partition.ReadBlocks(range);
            UNIT_ASSERT(bitmap == GetUnencryptedBlockMask(response));

            TVector<TString> blocks;
            auto sglist = ResizeBlocks(blocks, range.Size(), emptyBlock);
            auto localResponse = partition.ReadBlocksLocal(range, sglist);
            UNIT_ASSERT(bitmap == GetUnencryptedBlockMask(localResponse));
        }

        {
            auto writeRange = TBlockRange32::WithLength(1, 4);
            partition.WriteBlocks(writeRange, char(4));
            MarkWrittenBlocks(bitmap, writeRange);

            auto zeroRange = TBlockRange32::WithLength(5, 3);
            partition.ZeroBlocks(zeroRange);
            MarkZeroedBlocks(bitmap, zeroRange);

            auto writeRangeLocal = TBlockRange32::WithLength(8, 3);
            partition.WriteBlocksLocal(writeRangeLocal, GetBlockContent(4));
            MarkWrittenBlocks(bitmap, writeRangeLocal);

            auto zeroRangeLocal = TBlockRange32::WithLength(12, 3);
            partition.ZeroBlocks(zeroRangeLocal);
            MarkZeroedBlocks(bitmap, zeroRangeLocal);

            partition.Flush();

            auto response = partition.ReadBlocks(range);
            UNIT_ASSERT(bitmap == GetUnencryptedBlockMask(response));

            TVector<TString> blocks;
            auto sglist = ResizeBlocks(blocks, range.Size(), emptyBlock);
            auto localResponse = partition.ReadBlocksLocal(range, sglist);
            UNIT_ASSERT(bitmap == GetUnencryptedBlockMask(localResponse));
        }

        {
            auto zeroRange = TBlockRange32::WithLength(1, 3);
            partition.ZeroBlocks(zeroRange);
            MarkZeroedBlocks(bitmap, zeroRange);

            auto writeRange = TBlockRange32::WithLength(5, 3);
            partition.WriteBlocks(writeRange, char(4));
            MarkWrittenBlocks(bitmap, writeRange);

            auto zeroRangeLocal = TBlockRange32::WithLength(8, 3);
            partition.ZeroBlocks(zeroRangeLocal);
            MarkZeroedBlocks(bitmap, zeroRangeLocal);

            auto writeRangeLocal = TBlockRange32::WithLength(12, 3);
            partition.WriteBlocksLocal(writeRangeLocal, GetBlockContent(4));
            MarkWrittenBlocks(bitmap, writeRangeLocal);

            partition.Flush();

            auto response = partition.ReadBlocks(range);
            UNIT_ASSERT(bitmap == GetUnencryptedBlockMask(response));

            TVector<TString> blocks;
            auto sglist = ResizeBlocks(blocks, range.Size(), emptyBlock);
            auto localResponse = partition.ReadBlocksLocal(range, sglist);
            UNIT_ASSERT(bitmap == GetUnencryptedBlockMask(localResponse));
        }
    }

    Y_UNIT_TEST(ShouldHandleCorruptedFreshBlobOnInitFreshBlocks)
    {
        auto config = DefaultConfig();
        config.SetFreshChannelCount(1);
        config.SetFreshChannelWriteRequestsEnabled(true);

        auto runtime = PrepareTestActorRuntime(config);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        partition.WriteBlocks(TBlockRange32::MakeOneBlock(0), 1);
        partition.WriteBlocks(TBlockRange32::MakeOneBlock(1), 2);
        partition.WriteBlocks(TBlockRange32::MakeOneBlock(2), 3);

        bool evRangeResultSeen = false;

        ui32 evLoadFreshBlobsCompletedCount = 0;

        runtime->SetEventFilter(
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
                    default: {
                        return false;
                    }
                }
            }
        );

        partition.RebootTablet();
        partition.WaitReady();

        UNIT_ASSERT_VALUES_EQUAL(true, evRangeResultSeen);

        // tablet rebooted twice (after explicit RebootTablet() and on fail)
        UNIT_ASSERT_VALUES_EQUAL(2, evLoadFreshBlobsCompletedCount);

        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(1),
            GetBlockContent(partition.ReadBlocks(0)));
        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(2),
            GetBlockContent(partition.ReadBlocks(1)));
        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(3),
            GetBlockContent(partition.ReadBlocks(2)));
    }

    Y_UNIT_TEST(ShouldUpdateUsedBlocksMapWhenFlushingBlocksFromFreshChannel)
    {
        auto config = DefaultConfig();
        config.SetFreshChannelCount(1);
        config.SetFreshChannelWriteRequestsEnabled(true);

        auto runtime = PrepareTestActorRuntime(config);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        partition.WriteBlocks(1);
        partition.WriteBlocks(2);
        partition.WriteBlocks(3);

        partition.ZeroBlocks(2);

        {
            auto response = partition.StatPartition();
            auto stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetUsedBlocksCount());
        }

        partition.Flush();

        {
            auto response = partition.StatPartition();
            auto stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(2, stats.GetUsedBlocksCount());
        }
    }

    Y_UNIT_TEST(ShouldCorrectlyCalculateBlocksCount)
    {
        constexpr ui32 blockCount = 1024 * 1024;
        auto runtime = PrepareTestActorRuntime(DefaultConfig(), blockCount);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        partition.WriteBlocks(
            TBlockRange32::MakeClosedInterval(0, 1024 * 10),
            1);
        partition.WriteBlocks(
            TBlockRange32::MakeClosedInterval(1024 * 5, 1024 * 11),
            1);

        const auto step = 16;
        for (ui32 i = 1024 * 10; i < 1024 * 12; i += step) {
            partition.WriteBlocks(TBlockRange32::WithLength(i, step), 1);
        }

        for (ui32 i = 1024 * 20; i < 1024 * 21; i += step) {
            partition.WriteBlocks(TBlockRange32::WithLength(i, step + 1), 1);
        }

        partition.WriteBlocks(
            TBlockRange32::MakeClosedInterval(1001111, 1001210),
            1);

        partition.ZeroBlocks(TBlockRange32::MakeClosedInterval(1024, 3023));
        partition.ZeroBlocks(TBlockRange32::MakeClosedInterval(5024, 5033));

        const auto expected = 1024 * 12 + 1024 + 1 + 100 - 2000 - 10;

        /*
        partition.WriteBlocks(TBlockRange32(5024, 5043), 1);
        partition.ZeroBlocks(TBlockRange32::MakeClosedInterval(5024, 5033));
        const auto expected = 10;
        */

        auto response = partition.StatPartition();
        auto stats = response->Record.GetStats();
        UNIT_ASSERT_VALUES_EQUAL(expected, stats.GetUsedBlocksCount());

        partition.RebootTablet();

        response = partition.StatPartition();
        stats = response->Record.GetStats();
        UNIT_ASSERT_VALUES_EQUAL(expected, stats.GetUsedBlocksCount());

        ui32 completionStatus = -1;
        runtime->SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) {
                switch (event->GetTypeRewrite()) {
                    case TEvPartitionPrivate::EvMetadataRebuildCompleted: {
                        using TEv =
                            TEvPartitionPrivate::TEvMetadataRebuildCompleted;
                        auto* msg = event->Get<TEv>();
                        completionStatus = msg->GetStatus();
                        break;
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            }
        );

        const auto rangesPerBatch = 100;
        partition.RebuildMetadata(NProto::ERebuildMetadataType::USED_BLOCKS, rangesPerBatch);

        UNIT_ASSERT_VALUES_EQUAL(S_OK, completionStatus);

        response = partition.StatPartition();
        stats = response->Record.GetStats();
        UNIT_ASSERT_VALUES_EQUAL(expected, stats.GetUsedBlocksCount());

        partition.RebootTablet();

        response = partition.StatPartition();
        stats = response->Record.GetStats();
        UNIT_ASSERT_VALUES_EQUAL(expected, stats.GetUsedBlocksCount());
    }

    Y_UNIT_TEST(ShouldPostponeBlockCountCalculationAndScanDiskUntilInflightWriteRequestsAreCompleted)
    {
        constexpr ui32 blockCount = 1024 * 1024;
        auto runtime = PrepareTestActorRuntime(DefaultConfig(), blockCount);

        ui64 writeCommitId = 0;

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        runtime->SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) mutable {
                switch (event->GetTypeRewrite()) {
                    case TEvPartitionPrivate::EvAddBlobsRequest: {
                        writeCommitId =
                            event->Get<TEvPartitionPrivate::TEvAddBlobsRequest>()->CommitId;
                        return TTestActorRuntime::EEventAction::DROP;
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            }
        );

        partition.SendWriteBlocksRequest(TBlockRange32::WithLength(0, 1024 * 10), 1);
        runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        {
            partition.SendMetadataRebuildBlockCountRequest(
                MakePartialBlobId(writeCommitId + 1, 0),
                100,
                MakePartialBlobId(writeCommitId + 1, 0));

            const auto response =
                partition.RecvMetadataRebuildBlockCountResponse();
            UNIT_ASSERT(FAILED(response->GetStatus()));
        }

        {
            partition.SendScanDiskBatchRequest(
                MakePartialBlobId(writeCommitId, 0),
                100,
                MakePartialBlobId(writeCommitId, 0));

            const auto response = partition.RecvScanDiskBatchResponse();
            UNIT_ASSERT(FAILED(response->GetStatus()));
        }
    }

    Y_UNIT_TEST(ShouldRebuildBlockCountSensors)
    {
        constexpr ui32 blockCount = 1024 * 1024;
        auto runtime = PrepareTestActorRuntime(DefaultConfig(), blockCount);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        partition.WriteBlocks(
            TBlockRange32::MakeClosedInterval(0, 1024 * 10),
            1);

        auto response = partition.RebuildMetadata(NProto::ERebuildMetadataType::BLOCK_COUNT, 100);

        partition.WriteBlocks(TBlockRange32::WithLength(0, 1024), 1);

        auto stats = partition.StatPartition()->Record.GetStats();
        UNIT_ASSERT_VALUES_EQUAL(1024 * 11 + 1, stats.GetMergedBlocksCount());
    }

    Y_UNIT_TEST(ShouldFailGetMetadataRebuildStatusIfNoOperationRunning)
    {
        constexpr ui32 blockCount = 1024 * 1024;
        auto runtime = PrepareTestActorRuntime(DefaultConfig(), blockCount);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        partition.WriteBlocks(
            TBlockRange32::MakeClosedInterval(0, 1024 * 10),
            1);

        partition.SendGetRebuildMetadataStatusRequest();
        auto progress = partition.RecvGetRebuildMetadataStatusResponse();
        UNIT_ASSERT(FAILED(progress->GetStatus()));
    }

    Y_UNIT_TEST(ShouldSuccessfullyRunMetadataRebuildOnEmptyDisk)
    {
        constexpr ui32 blockCount = 1024 * 1024;
        auto runtime = PrepareTestActorRuntime(DefaultConfig(), blockCount);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        partition.RebuildMetadata(NProto::ERebuildMetadataType::BLOCK_COUNT, 10);
        TDispatchOptions options;
        options.FinalEvents.emplace_back(TEvPartitionPrivate::EvMetadataRebuildCompleted);
        runtime->DispatchEvents(options, TDuration::Seconds(1));

        auto progress = partition.GetRebuildMetadataStatus();
        UNIT_ASSERT_VALUES_EQUAL(
            0,
            progress->Record.GetProgress().GetProcessed()
        );
        UNIT_ASSERT_VALUES_EQUAL(
            0,
            progress->Record.GetProgress().GetTotal()
        );
        UNIT_ASSERT_VALUES_EQUAL(
            true,
            progress->Record.GetProgress().GetIsCompleted()
        );
    }

    Y_UNIT_TEST(ShouldReturnRebuildMetadataProgressDuringExecution)
    {
        constexpr ui32 blockCount = 1024 * 1024;
        auto runtime = PrepareTestActorRuntime(DefaultConfig(), blockCount);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        partition.WriteBlocks(TBlockRange32::WithLength(0, 1024 * 10), 1);
        partition.WriteBlocks(
            TBlockRange32::WithLength(1024 * 10, 1024 * 10),
            1);

        runtime->SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) mutable {
                switch (event->GetTypeRewrite()) {
                    case TEvPartitionPrivate::EvMetadataRebuildBlockCountResponse: {
                        const auto* msg =
                            event->Get<TEvPartitionPrivate::TEvMetadataRebuildBlockCountResponse>();
                        UNIT_ASSERT_VALUES_UNEQUAL(
                            0,
                            msg->RebuildState.MixedBlocks + msg->RebuildState.MergedBlocks);
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            }
        );

        auto response = partition.RebuildMetadata(NProto::ERebuildMetadataType::BLOCK_COUNT, 10);

        auto progress = partition.GetRebuildMetadataStatus();
        UNIT_ASSERT_VALUES_EQUAL(
            20,
            progress->Record.GetProgress().GetProcessed()
        );
        UNIT_ASSERT_VALUES_EQUAL(
            true,
            progress->Record.GetProgress().GetIsCompleted()
        );
    }

    Y_UNIT_TEST(ShouldBlockCleanupDuringMetadataRebuild)
    {
        constexpr ui32 blockCount = 1024 * 1024;
        auto runtime = PrepareTestActorRuntime(DefaultConfig(), blockCount);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        partition.WriteBlocks(TBlockRange32::WithLength(0, 1024), 1);
        partition.WriteBlocks(TBlockRange32::WithLength(0, 1024), 1);
        partition.WriteBlocks(TBlockRange32::WithLength(0, 1024), 1);
        partition.WriteBlocks(TBlockRange32::WithLength(0, 1024), 1);

        ui32 cnt = 0;

        TAutoPtr<IEventHandle> savedEvent;

        runtime->SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) mutable {
                switch (event->GetTypeRewrite()) {
                    case TEvPartitionPrivate::EvMetadataRebuildBlockCountRequest: {
                        if (++cnt == 1) {
                            savedEvent = event.Release();
                            return TTestActorRuntime::EEventAction::DROP;
                        }
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            }
        );

        auto response = partition.RebuildMetadata(NProto::ERebuildMetadataType::BLOCK_COUNT, 10);
        partition.Compaction();

        {
            auto stats = partition.StatPartition()->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(5, stats.GetMergedBlobsCount());
        }

        {
            partition.SendCleanupRequest();
            auto response = partition.RecvCleanupResponse();
            UNIT_ASSERT_VALUES_EQUAL(E_TRY_AGAIN, response->GetStatus());
        }

        runtime->Send(savedEvent.Release());

        TDispatchOptions options;
        options.FinalEvents.emplace_back(
            TEvPartitionPrivate::EvMetadataRebuildCompleted);
        runtime->DispatchEvents(options);

        partition.Cleanup();

        {
            auto stats = partition.StatPartition()->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(1, stats.GetMergedBlobsCount());
        }

        auto progress = partition.GetRebuildMetadataStatus();
        UNIT_ASSERT_VALUES_EQUAL(
            4,
            progress->Record.GetProgress().GetProcessed()
        );
        UNIT_ASSERT_VALUES_EQUAL(
            4,
            progress->Record.GetProgress().GetTotal()
        );
        UNIT_ASSERT_VALUES_EQUAL(
            true,
            progress->Record.GetProgress().GetIsCompleted()
        );
    }

    Y_UNIT_TEST(ShouldNotKillTabletBeforeMaxReadBlobErrorsHappen)
    {
        auto config = DefaultConfig();
        config.SetWriteBlobThreshold(1_MB);
        config.SetMaxReadBlobErrorsBeforeSuicide(5);

        auto r = PrepareTestActorRuntime(config);
        auto& runtime = *r;

        TPartitionClient partition(runtime);
        partition.WaitReady();
        partition.WriteBlocks(TBlockRange32::WithLength(0, 1001), 1);

        NKikimrProto::TLogoBlobID blobId;
        {
            auto response = partition.DescribeBlocks(
                TBlockRange32::WithLength(0, 1001),
                "");
            UNIT_ASSERT_VALUES_EQUAL(1, response->Record.BlobPiecesSize());
            blobId = response->Record.GetBlobPieces(0).GetBlobId();
        }

        ui32 readBlobCount = 0;
        bool readBlobShouldFail = true;

        const auto eventHandler = [&] (const TEvBlobStorage::TEvGet::TPtr& ev) {
            auto& msg = *ev->Get();

            for (ui32 i = 0; i < msg.QuerySize; i++) {
                NKikimr::TLogoBlobID expected = LogoBlobIDFromLogoBlobID(blobId);
                NKikimr::TLogoBlobID actual = msg.Queries[i].Id;

                if (expected.IsSameBlob(actual)) {
                    readBlobCount++;

                    if (readBlobShouldFail) {
                        auto response = std::make_unique<TEvBlobStorage::TEvGetResult>(
                            NKikimrProto::ERROR,
                            msg.QuerySize,
                            0);  // groupId

                        runtime.Schedule(
                            new IEventHandle(
                                ev->Sender,
                                ev->Recipient,
                                response.release(),
                                0,
                                ev->Cookie),
                            TDuration());
                        return true;
                    }
                }
            }

            return false;
        };

        runtime.SetEventFilter(
            [eventHandler] (TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& ev) {
                bool handled = false;

                const auto wrapped = [&] (const auto& ev) {
                    handled = eventHandler(ev);
                };

                switch (ev->GetTypeRewrite()) {
                    hFunc(TEvBlobStorage::TEvGet, wrapped);
                }
                return handled;
            }
        );

        bool suicideHappened = false;

        runtime.SetObserverFunc([&] (TAutoPtr<IEventHandle>& ev) {
                switch (ev->GetTypeRewrite()) {
                    case TEvTablet::EEv::EvTabletDead: {
                        suicideHappened = true;
                        break;
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(ev);
            }
        );

        for (ui32 i = 1; i < config.GetMaxReadBlobErrorsBeforeSuicide(); i++) {
            partition.SendReadBlocksRequest(0);
            auto response = partition.RecvReadBlocksResponse();
            UNIT_ASSERT(FAILED(response->GetStatus()));

            UNIT_ASSERT_VALUES_EQUAL(i, readBlobCount);
            UNIT_ASSERT(!suicideHappened);
        }

        partition.SendReadBlocksRequest(0);
        auto response = partition.RecvReadBlocksResponse();
        UNIT_ASSERT(FAILED(response->GetStatus()));

        UNIT_ASSERT_VALUES_EQUAL(
            config.GetMaxReadBlobErrorsBeforeSuicide(),
            readBlobCount);
        UNIT_ASSERT(suicideHappened);
        suicideHappened = false;

        {
            TPartitionClient partition(runtime);
            partition.WaitReady();

            readBlobShouldFail = false;
            UNIT_ASSERT_VALUES_EQUAL(
                GetBlockContent(1),
                GetBlockContent(partition.ReadBlocks(0))
            );
            UNIT_ASSERT(!suicideHappened);
        }
    }

    Y_UNIT_TEST(ShouldProcessMultipleRangesUponCompaction)
    {
        auto config = DefaultConfig();
        config.SetWriteBlobThreshold(1_MB);
        config.SetBatchCompactionEnabled(true);
        config.SetCompactionRangeCountPerRun(3);
        config.SetSSDMaxBlobsPerRange(999);
        config.SetHDDMaxBlobsPerRange(999);
        config.SetCompactionGarbageThreshold(999);
        config.SetCompactionRangeGarbageThreshold(999);
        auto runtime = PrepareTestActorRuntime(
            config,
            MaxPartitionBlocksCount
        );

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        const auto blockRange1 = TBlockRange32::WithLength(0, 1024);
        const auto blockRange2 = TBlockRange32::WithLength(1024 * 1024, 1024);
        const auto blockRange3 = TBlockRange32::WithLength(2 * 1024 * 1024, 1024);
        const auto blockRange4 = TBlockRange32::WithLength(3 * 1024 * 1024, 1024);

        partition.WriteBlocks(blockRange1, 1);
        partition.WriteBlocks(blockRange1, 2);
        partition.WriteBlocks(blockRange1, 3);

        partition.WriteBlocks(blockRange2, 4);
        partition.WriteBlocks(blockRange2, 5);
        partition.WriteBlocks(blockRange2, 6);

        partition.WriteBlocks(blockRange3, 7);
        partition.WriteBlocks(blockRange3, 8);
        partition.WriteBlocks(blockRange3, 9);

        partition.WriteBlocks(blockRange4, 10);
        partition.WriteBlocks(blockRange4, 11);

        // blockRange4 should not be compacted, other ranges - should
        partition.Compaction();
        partition.Cleanup();

        // checking that data wasn't corrupted
        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(3),
            GetBlockContent(partition.ReadBlocks(blockRange1.Start))
        );
        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(3),
            GetBlockContent(partition.ReadBlocks(blockRange1.End))
        );

        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(6),
            GetBlockContent(partition.ReadBlocks(blockRange2.Start))
        );
        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(6),
            GetBlockContent(partition.ReadBlocks(blockRange2.End))
        );

        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(9),
            GetBlockContent(partition.ReadBlocks(blockRange3.Start))
        );
        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(9),
            GetBlockContent(partition.ReadBlocks(blockRange3.End))
        );

        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(11),
            GetBlockContent(partition.ReadBlocks(blockRange4.Start))
        );
        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(11),
            GetBlockContent(partition.ReadBlocks(blockRange4.End))
        );

        // checking that we now have 1 blob in each of the first 3 ranges
        // and 2 blobs in the last range
        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(5, stats.GetMergedBlobsCount());
        }

        // blockRange4 and any other 2 ranges should be compacted
        partition.Compaction();
        partition.Cleanup();

        // all ranges should contain 1 blob now
        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(4, stats.GetMergedBlobsCount());
        }
    }

    Y_UNIT_TEST(ShouldPatchBlobsDuringCompaction)
    {
        auto config = DefaultConfig();
        config.SetWriteBlobThreshold(1_MB);
        config.SetBlobPatchingEnabled(true);
        config.SetHDDMaxBlobsPerRange(999);
        config.SetSSDMaxBlobsPerRange(999);
        config.SetCompactionGarbageThreshold(999);
        config.SetCompactionRangeGarbageThreshold(999);
        auto runtime = PrepareTestActorRuntime(
            config,
            MaxPartitionBlocksCount
        );

        bool evPatchObserved = false;
        runtime->SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) {
                switch (event->GetTypeRewrite()) {
                    case TEvBlobStorage::EvPatch: {
                        evPatchObserved = true;
                        break;
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            }
        );

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        const auto blockRange1 = TBlockRange32::WithLength(0, 1024);
        const auto blockRange2 = TBlockRange32::WithLength(0, 512);
        const auto blockRange3 = TBlockRange32::WithLength(0, 256);

        partition.WriteBlocks(blockRange1, 1);
        partition.WriteBlocks(blockRange2, 2);
        partition.WriteBlocks(blockRange3, 3);

        partition.Compaction();
        partition.Cleanup();

        // checking that data wasn't corrupted
        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(3),
            GetBlockContent(partition.ReadBlocks(0))
        );

        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(3),
            GetBlockContent(partition.ReadBlocks(255))
        );

        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(2),
            GetBlockContent(partition.ReadBlocks(256))
        );

        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(2),
            GetBlockContent(partition.ReadBlocks(511))
        );

        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(1),
            GetBlockContent(partition.ReadBlocks(512))
        );

        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(1),
            GetBlockContent(partition.ReadBlocks(1023))
        );

        UNIT_ASSERT_VALUES_EQUAL(
            TStringBuf(),
            GetBlockContent(partition.ReadBlocks(1024))
        );

        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(1, stats.GetMergedBlobsCount());
        }

        UNIT_ASSERT(evPatchObserved);
    }

    Y_UNIT_TEST(WritingBlobsInsteadOfPatchingIfDiffIsGreaterThanThreshold)
    {
        auto config = DefaultConfig();
        config.SetWriteBlobThreshold(1_MB);
        config.SetBlobPatchingEnabled(true);
        config.SetHDDMaxBlobsPerRange(999);
        config.SetSSDMaxBlobsPerRange(999);
        config.SetCompactionGarbageThreshold(999);
        config.SetCompactionRangeGarbageThreshold(999);
        config.SetMaxDiffPercentageForBlobPatching(75);
        auto runtime = PrepareTestActorRuntime(
            config,
            MaxPartitionBlocksCount
        );

        bool evPatchObserved = false;
        bool evWriteObserved = false;
        runtime->SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) {
                switch (event->GetTypeRewrite()) {
                    case TEvBlobStorage::EvPatch: {
                        evPatchObserved = true;
                        break;
                    }
                    case TEvPartitionPrivate::EvWriteBlobRequest: {
                        evWriteObserved = true;
                        break;
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            }
        );

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        const auto blockRange1 = TBlockRange32::WithLength(0, 1024);
        const auto blockRange2 = TBlockRange32::WithLength(0, 512 + 256 + 64);
        const auto blockRange3 = TBlockRange32::WithLength(0, 512);

        partition.WriteBlocks(blockRange1, 1);
        partition.WriteBlocks(blockRange2, 2);

        partition.Compaction();
        partition.Cleanup();

        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(2),
            GetBlockContent(partition.ReadBlocks(256))
        );

        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(2),
            GetBlockContent(partition.ReadBlocks(512 + 256 + 63))
        );

        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(1),
            GetBlockContent(partition.ReadBlocks(900))
        );

        UNIT_ASSERT(!evPatchObserved);
        UNIT_ASSERT(evWriteObserved);

        evWriteObserved = false;
        partition.WriteBlocks(blockRange3, 3);

        partition.Compaction();
        partition.Cleanup();

        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(3),
            GetBlockContent(partition.ReadBlocks(511))
        );

        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(2),
            GetBlockContent(partition.ReadBlocks(512))
        );

        UNIT_ASSERT(evPatchObserved);
        UNIT_ASSERT(evWriteObserved);
    }

    Y_UNIT_TEST(ShouldPatchBlobsDuringIncrementalCompaction)
    {
        auto config = DefaultConfig();
        config.SetBlobPatchingEnabled(true);
        DoTestIncrementalCompaction(std::move(config), true);
    }

    Y_UNIT_TEST(ShouldProperlyPatchBlobWithDifferentLayout)
    {
        auto config = DefaultConfig();
        config.SetWriteBlobThreshold(1_MB);
        config.SetBlobPatchingEnabled(true);
        config.SetHDDMaxBlobsPerRange(999);
        config.SetSSDMaxBlobsPerRange(999);
        config.SetCompactionGarbageThreshold(999);
        config.SetCompactionRangeGarbageThreshold(999);
        auto runtime = PrepareTestActorRuntime(
            config,
            MaxPartitionBlocksCount
        );

        bool evPatchObserved = false;
        runtime->SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) {
                switch (event->GetTypeRewrite()) {
                    case TEvBlobStorage::EvPatch: {
                        evPatchObserved = true;
                        break;
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            }
        );

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        partition.WriteBlocks(TBlockRange32::MakeOneBlock(1), 1);
        partition.WriteBlocks(TBlockRange32::MakeOneBlock(3), 3);
        partition.WriteBlocks(TBlockRange32::MakeOneBlock(4), 4);

        partition.Flush();
        partition.Compaction();
        partition.Cleanup();

        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(1, stats.GetMergedBlobsCount());
        }

        UNIT_ASSERT(evPatchObserved);
        evPatchObserved = false;

        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(1),
            GetBlockContent(partition.ReadBlocks(1))
        );

        UNIT_ASSERT_VALUES_EQUAL(
            TString(),
            GetBlockContent(partition.ReadBlocks(2))
        );

        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(3),
            GetBlockContent(partition.ReadBlocks(3))
        );

        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(4),
            GetBlockContent(partition.ReadBlocks(4))
        );

        partition.WriteBlocks(TBlockRange32::MakeOneBlock(2), 2);
        partition.ZeroBlocks(TBlockRange32::MakeOneBlock(4));

        partition.Flush();
        partition.Compaction();
        partition.Cleanup();

        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(1),
            GetBlockContent(partition.ReadBlocks(1))
        );

        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(2),
            GetBlockContent(partition.ReadBlocks(2))
        );

        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(3),
            GetBlockContent(partition.ReadBlocks(3))
        );

        UNIT_ASSERT_VALUES_EQUAL(
            TString(),
            GetBlockContent(partition.ReadBlocks(4))
        );

        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            // 2 == zeroBlob + dataBlob
            UNIT_ASSERT_VALUES_EQUAL(2, stats.GetMergedBlobsCount());
        }

        UNIT_ASSERT(evPatchObserved);
    }

    Y_UNIT_TEST(ShouldRejectSmallWritesAfterReachingFreshByteCountHardLimit)
    {
        NProto::TStorageServiceConfig config;
        config.SetFreshByteCountHardLimit(8_KB);
        config.SetFlushThreshold(4_MB);
        config.SetFreshChannelCount(1);
        config.SetFreshChannelWriteRequestsEnabled(true);
        auto runtime = PrepareTestActorRuntime(config);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        partition.WriteBlocks(TBlockRange32::MakeOneBlock(0), 1);
        partition.WriteBlocks(TBlockRange32::MakeOneBlock(0), 1);

        partition.SendWriteBlocksRequest(TBlockRange32::MakeOneBlock(0), 1);
        auto response = partition.RecvWriteBlocksResponse();
        UNIT_ASSERT_VALUES_EQUAL_C(
            E_REJECTED,
            response->GetStatus(),
            response->GetErrorReason());
        UNIT_ASSERT(
            HasProtoFlag(response->GetError().GetFlags(), NProto::EF_SILENT));

        partition.Flush();

        partition.SendWriteBlocksRequest(TBlockRange32::MakeOneBlock(0), 1);
        response = partition.RecvWriteBlocksResponse();
        UNIT_ASSERT_VALUES_EQUAL_C(
            S_OK,
            response->GetStatus(),
            response->GetErrorReason());
    }

    Y_UNIT_TEST(ShouldCorrectlyScanDiskWithoutBrokenBlobs)
    {
        constexpr ui32 blockCount = 1024 * 1024;
        auto runtime = PrepareTestActorRuntime(DefaultConfig(), blockCount);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        partition.WriteBlocks(
            TBlockRange32::MakeClosedInterval(0, 1024 * 10),
            1);
        partition.WriteBlocks(
            TBlockRange32::MakeClosedInterval(1024 * 5, 1024 * 11),
            1);

        const auto step = 16;
        for (ui32 i = 1024 * 10; i < 1024 * 12; i += step) {
            partition.WriteBlocks(TBlockRange32::WithLength(i, step), 1);
        }

        for (ui32 i = 1024 * 20; i < 1024 * 21; i += step) {
            partition.WriteBlocks(TBlockRange32::WithLength(i, step + 1), 1);
        }

        partition.WriteBlocks(
            TBlockRange32::MakeClosedInterval(1001111, 1001210),
            1);

        partition.ZeroBlocks(TBlockRange32::MakeClosedInterval(1024, 3023));
        partition.ZeroBlocks(TBlockRange32::MakeClosedInterval(5024, 5033));

        const ui64 mixedBlobsCount = 4;
        const ui64 mergedBlobsCount = 20;
        const ui64 blobsCount = mixedBlobsCount + mergedBlobsCount;
        const ui64 blobsWithoutDeletionMarkerCount = blobsCount - 2;

        {
            const auto stats = partition.StatPartition()->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(
                mixedBlobsCount,
                stats.GetMixedBlobsCount());
            UNIT_ASSERT_VALUES_EQUAL(
                mergedBlobsCount,
                stats.GetMergedBlobsCount());
        }

        ui32 completionStatus = -1;
        ui32 readBlobCount = 0;

        runtime->SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) {
                switch (event->GetTypeRewrite()) {
                    case TEvPartitionPrivate::EvScanDiskCompleted: {
                        using TEv =
                            TEvPartitionPrivate::TEvScanDiskCompleted;
                        const auto* msg = event->Get<TEv>();
                        completionStatus = msg->GetStatus();
                        break;
                    }
                    case TEvPartitionCommonPrivate::EvReadBlobResponse: {
                        using TEv =
                            TEvPartitionCommonPrivate::TEvReadBlobResponse;
                        const auto* msg = event->Get<TEv>();
                        UNIT_ASSERT(!FAILED(msg->GetStatus()));
                        ++readBlobCount;
                        break;
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            }
        );

        const auto checkScanDisk = [&] (ui32 blobsPerBatch) {
            completionStatus = -1;
            readBlobCount = 0;

            const auto response = partition.ScanDisk(blobsPerBatch);

            TDispatchOptions options;
            options.FinalEvents.emplace_back(
                TEvPartitionPrivate::EvScanDiskCompleted);
            runtime->DispatchEvents(options, TDuration::Seconds(1));

            UNIT_ASSERT_VALUES_EQUAL(S_OK, completionStatus);
            UNIT_ASSERT_VALUES_EQUAL(
                blobsWithoutDeletionMarkerCount,
                readBlobCount);

            const auto progress = partition.GetScanDiskStatus();
            UNIT_ASSERT_VALUES_EQUAL(
                true,
                progress->Record.GetProgress().GetIsCompleted());
            UNIT_ASSERT_VALUES_EQUAL(
                blobsCount,
                progress->Record.GetProgress().GetTotal());
            UNIT_ASSERT_VALUES_EQUAL(
                blobsCount,
                progress->Record.GetProgress().GetProcessed());
            UNIT_ASSERT_VALUES_EQUAL(
                0,
                progress->Record.GetProgress().GetBrokenBlobs().size());
        };

        checkScanDisk(100);
        checkScanDisk(2);
        checkScanDisk(1);
        checkScanDisk(10);
    }

    Y_UNIT_TEST(ShouldCorrectlyScanDiskWithBrokenBlobs)
    {
        constexpr ui32 blockCount = 1024 * 1024;
        auto runtime = PrepareTestActorRuntime(DefaultConfig(), blockCount);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        partition.WriteBlocks(TBlockRange32::MakeClosedInterval(0, 1024 * 10), 1);
        partition.WriteBlocks(TBlockRange32::MakeClosedInterval(1024 * 5, 1024 * 11), 1);

        const auto step = 16;
        for (ui32 i = 1024 * 10; i < 1024 * 12; i += step) {
            partition.WriteBlocks(TBlockRange32::WithLength(i, step), 1);
        }

        for (ui32 i = 1024 * 20; i < 1024 * 21; i += step) {
            partition.WriteBlocks(TBlockRange32::WithLength(i, step + 1), 1);
        }

        partition.WriteBlocks(
            TBlockRange32::MakeClosedInterval(1001111, 1001210),
            1);

        const ui64 mixedBlobsCount = 4;
        const ui64 mergedBlobsCount = 18;
        const ui64 blobsCount = mixedBlobsCount + mergedBlobsCount;

        {
            const auto stats = partition.StatPartition()->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(
                mixedBlobsCount,
                stats.GetMixedBlobsCount());
            UNIT_ASSERT_VALUES_EQUAL(
                mergedBlobsCount,
                stats.GetMergedBlobsCount());
        }

        ui32 completionStatus = -1;

        TVector<bool> brokenBlobsIndexes;
        ui32 index = 0;

        runtime->SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) {
                switch (event->GetTypeRewrite()) {
                    case TEvPartitionPrivate::EvScanDiskCompleted: {
                        using TEv =
                            TEvPartitionPrivate::TEvScanDiskCompleted;
                        const auto* msg = event->Get<TEv>();

                        completionStatus = msg->GetStatus();
                        break;
                    }
                    case TEvPartitionCommonPrivate::EvReadBlobResponse: {
                        using TEv =
                            TEvPartitionCommonPrivate::TEvReadBlobResponse;

                        UNIT_ASSERT(index < brokenBlobsIndexes.size());

                        if (brokenBlobsIndexes[index++]) {
                            auto response = std::make_unique<TEv>(
                                MakeError(
                                    E_REJECTED,
                                    "blob is broken"));

                            runtime->Send(new IEventHandle(
                                event->Recipient,
                                event->Sender,
                                response.release(),
                                0, // flags
                                event->Cookie
                            ), 0);

                            return TTestActorRuntime::EEventAction::DROP;
                        }
                        break;
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            }
        );

        const auto checkScanDisk = [&] (ui32 blobsPerBatch) {
            completionStatus = -1;
            index = 0;

            const auto response = partition.ScanDisk(blobsPerBatch);

            TDispatchOptions options;
            options.FinalEvents.emplace_back(
                TEvPartitionPrivate::EvScanDiskCompleted);
            runtime->DispatchEvents(options, TDuration::Seconds(1));

            UNIT_ASSERT_VALUES_EQUAL(S_OK, completionStatus);

            const auto progress = partition.GetScanDiskStatus();

            UNIT_ASSERT_VALUES_EQUAL(
                true,
                progress->Record.GetProgress().GetIsCompleted());
            UNIT_ASSERT_VALUES_EQUAL(
                blobsCount,
                progress->Record.GetProgress().GetTotal());
            UNIT_ASSERT_VALUES_EQUAL(
                blobsCount,
                progress->Record.GetProgress().GetProcessed());

            int brokenBlobsCountExpected = 0;
            for (bool isBroken : brokenBlobsIndexes) {
                if (isBroken) {
                    ++brokenBlobsCountExpected;
                }
            }
            UNIT_ASSERT_VALUES_EQUAL(
                brokenBlobsCountExpected,
                progress->Record.GetProgress().GetBrokenBlobs().size());
        };

        brokenBlobsIndexes = TVector<bool>(blobsCount);
        brokenBlobsIndexes[0] = true;
        brokenBlobsIndexes[1] = true;
        brokenBlobsIndexes[4] = true;

        checkScanDisk(100);
        checkScanDisk(2);

        brokenBlobsIndexes = TVector<bool>(blobsCount, true);

        checkScanDisk(1);
        checkScanDisk(10);
    }

    Y_UNIT_TEST(ShouldCorrectlyUpdatePartialScanDiskProgress)
    {
        constexpr ui32 blockCount = 1024 * 1024;
        auto runtime = PrepareTestActorRuntime(DefaultConfig(), blockCount);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        partition.WriteBlocks(
            TBlockRange32::MakeClosedInterval(0, 1024 * 10),
            1);
        partition.WriteBlocks(
            TBlockRange32::MakeClosedInterval(1024 * 5, 1024 * 11),
            1);

        const auto step = 16;
        for (ui32 i = 1024 * 10; i < 1024 * 12; i += step) {
            partition.WriteBlocks(TBlockRange32::WithLength(i, step), 1);
        }

        for (ui32 i = 1024 * 20; i < 1024 * 21; i += step) {
            partition.WriteBlocks(TBlockRange32::WithLength(i, step + 1), 1);
        }

        partition.WriteBlocks(
            TBlockRange32::MakeClosedInterval(1001111, 1001210),
            1);

        ui32 batchCount = 0;
        runtime->SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) {
                switch (event->GetTypeRewrite()) {
                    case TEvPartitionPrivate::EvScanDiskBatchResponse: {
                        if (++batchCount == 2) {
                            return TTestActorRuntime::EEventAction::DROP;
                        }
                        break;
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            }
        );

        const ui32 blobsPerBatch = 3;
        const auto response = partition.ScanDisk(blobsPerBatch);

        runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        const auto progress = partition.GetScanDiskStatus();

        UNIT_ASSERT_VALUES_EQUAL(
            false,
            progress->Record.GetProgress().GetIsCompleted());
        UNIT_ASSERT_VALUES_EQUAL(
            blobsPerBatch,
            progress->Record.GetProgress().GetProcessed());
        UNIT_ASSERT_LT(
            blobsPerBatch,
            progress->Record.GetProgress().GetTotal());
    }

    Y_UNIT_TEST(ShouldFailGetScanDiskStatusIfNoOperationRunning)
    {
        constexpr ui32 blockCount = 1024 * 1024;
        auto runtime = PrepareTestActorRuntime(DefaultConfig(), blockCount);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        partition.WriteBlocks(TBlockRange32::MakeClosedInterval(0, 1024 * 10), 1);

        partition.SendGetScanDiskStatusRequest();
        const auto progress = partition.RecvGetScanDiskStatusResponse();
        UNIT_ASSERT(FAILED(progress->GetStatus()));
    }

    Y_UNIT_TEST(ShouldSuccessfullyScanDiskIfDiskIsEmpty)
    {
        constexpr ui32 blockCount = 1024 * 1024;
        auto runtime = PrepareTestActorRuntime(DefaultConfig(), blockCount);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        const ui32 blobsPerBatch = 10;
        const auto response = partition.ScanDisk(blobsPerBatch);

        TDispatchOptions options;
        options.FinalEvents.emplace_back(
            TEvPartitionPrivate::EvScanDiskCompleted);
        runtime->DispatchEvents(options, TDuration::Seconds(1));

        const auto progress = partition.GetScanDiskStatus();
        UNIT_ASSERT_VALUES_EQUAL(
            true,
            progress->Record.GetProgress().GetIsCompleted());
        UNIT_ASSERT_VALUES_EQUAL(
            0,
            progress->Record.GetProgress().GetTotal());
        UNIT_ASSERT_VALUES_EQUAL(
            0,
            progress->Record.GetProgress().GetProcessed());
        UNIT_ASSERT_VALUES_EQUAL(
            0,
            progress->Record.GetProgress().GetBrokenBlobs().size());
    }

    Y_UNIT_TEST(ShouldBlockCleanupDuringScanDisk)
    {
        constexpr ui32 blockCount = 1024 * 1024;
        auto runtime = PrepareTestActorRuntime(DefaultConfig(), blockCount);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        partition.WriteBlocks(TBlockRange32::WithLength(0, 1024), 1);
        partition.WriteBlocks(TBlockRange32::WithLength(0, 1024), 1);
        partition.WriteBlocks(TBlockRange32::WithLength(0, 1024), 1);
        partition.WriteBlocks(TBlockRange32::WithLength(0, 1024), 1);

        ui32 cnt = 0;
        TAutoPtr<IEventHandle> savedEvent;

        runtime->SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) mutable {
                switch (event->GetTypeRewrite()) {
                    case TEvPartitionPrivate::EvScanDiskBatchRequest: {
                        if (++cnt == 1) {
                            savedEvent = event.Release();
                            return TTestActorRuntime::EEventAction::DROP;
                        }
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            }
        );

        partition.ScanDisk(10);
        partition.Compaction();

        {
            const auto stats = partition.StatPartition()->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(5, stats.GetMergedBlobsCount());
        }

        {
            partition.SendCleanupRequest();
            const auto response = partition.RecvCleanupResponse();
            UNIT_ASSERT_VALUES_EQUAL(E_TRY_AGAIN, response->GetStatus());
        }

        runtime->Send(savedEvent.Release());

        TDispatchOptions options;
        options.FinalEvents.emplace_back(
            TEvPartitionPrivate::EvScanDiskCompleted);
        runtime->DispatchEvents(options);

        partition.Cleanup();

        {
            const auto stats = partition.StatPartition()->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(1, stats.GetMergedBlobsCount());
        }

        const auto progress = partition.GetScanDiskStatus();
        UNIT_ASSERT_VALUES_EQUAL(
            true,
            progress->Record.GetProgress().GetIsCompleted());
        UNIT_ASSERT_VALUES_EQUAL(
            progress->Record.GetProgress().GetTotal(),
            progress->Record.GetProgress().GetProcessed());
        UNIT_ASSERT_VALUES_EQUAL(
            0,
            progress->Record.GetProgress().GetBrokenBlobs().size());
    }

    Y_UNIT_TEST(ShouldFailScanDiskIfAlreadyRunning)
    {
        constexpr ui32 blockCount = 1024 * 1024;
        auto runtime = PrepareTestActorRuntime(DefaultConfig(), blockCount);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        partition.WriteBlocks(TBlockRange32::WithLength(0, 1024), 1);
        partition.WriteBlocks(TBlockRange32::WithLength(0, 1024), 1);
        partition.WriteBlocks(TBlockRange32::WithLength(0, 1024), 1);
        partition.WriteBlocks(TBlockRange32::WithLength(0, 1024), 1);

        ui32 cnt = 0;
        TAutoPtr<IEventHandle> savedEvent;

        runtime->SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) mutable {
                switch (event->GetTypeRewrite()) {
                    case TEvPartitionPrivate::EvScanDiskBatchRequest: {
                        if (++cnt == 1) {
                            savedEvent = event.Release();
                            return TTestActorRuntime::EEventAction::DROP;
                        }
                        break;
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            }
        );

        const auto responseFirst = partition.ScanDisk(10);
        UNIT_ASSERT_VALUES_EQUAL(
            S_OK,
            responseFirst->Record.GetError().GetCode());

        const auto responseDuplicate = partition.ScanDisk(10);
        UNIT_ASSERT_VALUES_EQUAL(
            S_ALREADY,
            responseDuplicate->Record.GetError().GetCode());

        runtime->Send(savedEvent.Release());

        TDispatchOptions options;
        options.FinalEvents.emplace_back(
            TEvPartitionPrivate::EvScanDiskCompleted);
        runtime->DispatchEvents(options, TDuration::Seconds(1));

        const auto progress = partition.GetScanDiskStatus();
        UNIT_ASSERT_VALUES_EQUAL(
            true,
            progress->Record.GetProgress().GetIsCompleted());
        UNIT_ASSERT_VALUES_EQUAL(
            progress->Record.GetProgress().GetTotal(),
            progress->Record.GetProgress().GetProcessed());
        UNIT_ASSERT_VALUES_EQUAL(
            0,
            progress->Record.GetProgress().GetBrokenBlobs().size());
    }

    Y_UNIT_TEST(ShouldCorrectlyScanDiskAfterPartitionReboot)
    {
        constexpr ui32 blockCount = 1024 * 1024;
        auto runtime = PrepareTestActorRuntime(DefaultConfig(), blockCount);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        partition.WriteBlocks(
            TBlockRange32::MakeClosedInterval(0, 1024 * 10),
            1);
        partition.WriteBlocks(
            TBlockRange32::MakeClosedInterval(1024 * 5, 1024 * 11),
            1);

        const auto step = 16;
        for (ui32 i = 1024 * 10; i < 1024 * 12; i += step) {
            partition.WriteBlocks(TBlockRange32::WithLength(i, step), 1);
        }

        for (ui32 i = 1024 * 20; i < 1024 * 21; i += step) {
            partition.WriteBlocks(TBlockRange32::WithLength(i, step + 1), 1);
        }

        partition.WriteBlocks(
            TBlockRange32::MakeClosedInterval(1001111, 1001210),
            1);

        partition.ZeroBlocks(TBlockRange32::MakeClosedInterval(1024, 3023));
        partition.ZeroBlocks(TBlockRange32::MakeClosedInterval(5024, 5033));

        ui32 completionStatus = -1;

        runtime->SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) {
                switch (event->GetTypeRewrite()) {
                    case TEvPartitionPrivate::EvScanDiskCompleted: {
                        using TEv =
                            TEvPartitionPrivate::TEvScanDiskCompleted;
                        const auto* msg = event->Get<TEv>();
                        completionStatus = msg->GetStatus();
                        break;
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            }
        );

        const auto checkScanDisk = [&] (ui32 blobsPerBatch) {
            completionStatus = -1;

            const auto response = partition.ScanDisk(blobsPerBatch);

            TDispatchOptions options;
            options.FinalEvents.emplace_back(
                TEvPartitionPrivate::EvScanDiskCompleted);
            runtime->DispatchEvents(options, TDuration::Seconds(1));

            UNIT_ASSERT_VALUES_EQUAL(S_OK, completionStatus);

            const auto progress = partition.GetScanDiskStatus();
            UNIT_ASSERT_VALUES_EQUAL(
                true,
                progress->Record.GetProgress().GetIsCompleted());
            UNIT_ASSERT_VALUES_EQUAL(
                progress->Record.GetProgress().GetTotal(),
                progress->Record.GetProgress().GetProcessed());
            UNIT_ASSERT_VALUES_EQUAL(
                0,
                progress->Record.GetProgress().GetBrokenBlobs().size());
        };

        checkScanDisk(5);
        partition.RebootTablet();
        checkScanDisk(100);
        partition.RebootTablet();
        checkScanDisk(1);
    }

    Y_UNIT_TEST(ShouldFirstGarbageCollectionFlagReboot)
    {
        auto runtime = PrepareTestActorRuntime();

        TPartitionClient partition(*runtime);
        bool garbageCollectorFinished = false;

        runtime->SetEventFilter(
            [&] (TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& event) {
                switch (event->GetTypeRewrite()) {
                    case TEvPartition::EvGarbageCollectorCompleted: {
                        garbageCollectorFinished = true;
                        break;
                    }
                }
                return false;
            }
        );
        partition.WaitReady();

        UNIT_ASSERT(garbageCollectorFinished);

        garbageCollectorFinished = false;

        partition.CollectGarbage();
        UNIT_ASSERT(!garbageCollectorFinished);
    }

    Y_UNIT_TEST(ShouldWriteBlocksWhenAddingUnconfirmedBlobs)
    {
        auto config = DefaultConfig();
        config.SetWriteBlobThreshold(1);
        config.SetAddingUnconfirmedBlobsEnabled(true);
        auto runtime = PrepareTestActorRuntime(config);

        bool dropAddConfirmedBlobs = false;

        runtime->SetObserverFunc([&] (auto& event) {
            switch (event->GetTypeRewrite()) {
                case TEvPartitionPrivate::EvAddConfirmedBlobsRequest: {
                    if (dropAddConfirmedBlobs) {
                        return TTestActorRuntime::EEventAction::DROP;
                    }
                    break;
                }
            }

            return TTestActorRuntime::DefaultObserverFunc(event);
        });

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        partition.WriteBlocks(11, 0);
        partition.CreateCheckpoint("checkpoint");

        partition.WriteBlocks(10, 1);
        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(2, stats.GetMergedBlobsCount());
            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetUnconfirmedBlobCount());
            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetConfirmedBlobCount());
        }
        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(1),
            GetBlockContent(partition.ReadBlocks(10))
        );

        dropAddConfirmedBlobs = true;
        partition.WriteBlocks(11, 2);

        {
            // can't read unconfirmed range
            partition.SendReadBlocksRequest(11);
            auto response = partition.RecvReadBlocksResponse();
            UNIT_ASSERT_VALUES_EQUAL(E_REJECTED, response->GetError().GetCode());
        }

        {
            // can't describe unconfirmed range
            partition.SendDescribeBlocksRequest(TBlockRange32::WithLength(11, 1), "");
            auto response =
                partition.RecvResponse<TEvVolume::TEvDescribeBlocksResponse>();
            UNIT_ASSERT_VALUES_EQUAL(E_REJECTED, response->GetError().GetCode());
        }

        {
            // can't do GetChangedBlocks on unconfirmed range
            partition.SendGetChangedBlocksRequest(
                TBlockRange32::WithLength(11, 1),
                "checkpoint",
                "",
                false);
            auto response =
                partition.RecvResponse<TEvService::TEvGetChangedBlocksResponse>();
            UNIT_ASSERT_VALUES_EQUAL(E_REJECTED, response->GetError().GetCode());
        }

        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetUnconfirmedBlobCount());
            UNIT_ASSERT_VALUES_EQUAL(1, stats.GetConfirmedBlobCount());
        }

        // but we can work with other ranges
        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(1),
            GetBlockContent(partition.ReadBlocks(10))
        );
        partition.DescribeBlocks(TBlockRange32::WithLength(10, 1), "");
        partition.GetChangedBlocks(TBlockRange32::WithLength(10, 1), "checkpoint", "", false);

        // older commits are also available even when range overlaps with
        // unconfirmed blobs
        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(0),
            GetBlockContent(partition.ReadBlocks(11, "checkpoint"))
        );
        partition.DescribeBlocks(TBlockRange32::WithLength(11, 1), "checkpoint");
        partition.GetChangedBlocks(TBlockRange32::WithLength(11, 1), "", "checkpoint", false);

        dropAddConfirmedBlobs = false;
        partition.RebootTablet();
        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(2),
            GetBlockContent(partition.ReadBlocks(11))
        );

        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetUnconfirmedBlobCount());
            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetConfirmedBlobCount());
        }
    }

    Y_UNIT_TEST(ShouldCompactAfterAddingConfirmedBlobs)
    {
        auto config = DefaultConfig();
        config.SetAddingUnconfirmedBlobsEnabled(true);
        auto runtime = PrepareTestActorRuntime(config);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        partition.WriteBlocks(TBlockRange32::WithLength(0, 1024), 1);

        TAutoPtr<IEventHandle> addConfirmedBlobs;
        bool interceptAddConfirmedBlobs = true;

        runtime->SetEventFilter(
            [&] (TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& event)
            {
                switch (event->GetTypeRewrite()) {
                    case TEvPartitionPrivate::EvAddConfirmedBlobsRequest: {
                        if (interceptAddConfirmedBlobs) {
                            UNIT_ASSERT(!addConfirmedBlobs);
                            addConfirmedBlobs = event.Release();
                            return true;
                        }
                        break;
                    }
                }

                return false;
            }
        );

        partition.WriteBlocks(TBlockRange32::WithLength(0, 1024), 2);
        UNIT_ASSERT(addConfirmedBlobs);

        partition.SendCompactionRequest();
        // wait for compaction to be queued
        runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        interceptAddConfirmedBlobs = false;
        runtime->Send(addConfirmedBlobs.Release());

        {
            auto compactResponse = partition.RecvCompactionResponse();
            // should fail on S_ALREADY and S_FALSE to ensure that compaction
            // was really taken place here
            UNIT_ASSERT_VALUES_EQUAL(S_OK, compactResponse->GetStatus());
        }
    }

    Y_UNIT_TEST(ShouldConfirmBlobs)
    {
        auto config = DefaultConfig();
        config.SetWriteBlobThreshold(1);
        config.SetAddingUnconfirmedBlobsEnabled(true);
        auto runtime = PrepareTestActorRuntime(config);

        bool dropAddConfirmedBlobs = false;
        bool spoofWriteBlobs = false;

        runtime->SetObserverFunc([&] (auto& event) {
            switch (event->GetTypeRewrite()) {
                case TEvPartitionPrivate::EvAddConfirmedBlobsRequest: {
                    if (dropAddConfirmedBlobs) {
                        return TTestActorRuntime::EEventAction::DROP;
                    }
                    break;
                }

                case TEvPartitionPrivate::EvWriteBlobRequest: {
                    if (spoofWriteBlobs) {
                        auto response =
                            std::make_unique<TEvPartitionPrivate::TEvWriteBlobResponse>();
                        response->BlockChecksums.resize(1);
                        runtime->Send(new IEventHandle(
                            event->Sender,
                            event->Recipient,
                            response.release(),
                            0,  // flags
                            0
                        ), 0);
                        return TTestActorRuntime::EEventAction::DROP;
                    }
                    break;
                }
            }

            return TTestActorRuntime::DefaultObserverFunc(event);
        });

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        partition.WriteBlocks(11, 1);

        dropAddConfirmedBlobs = true;
        spoofWriteBlobs = true;
        partition.WriteBlocks(11, 2);
        partition.SendReadBlocksRequest(11);
        auto response = partition.RecvReadBlocksResponse();
        // can't read unconfirmed range
        UNIT_ASSERT_VALUES_EQUAL(E_REJECTED, response->GetError().GetCode());

        dropAddConfirmedBlobs = false;
        partition.RebootTablet();

        // check that we are not affected by previous write
        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(1),
            GetBlockContent(partition.ReadBlocks(11))
        );
    }

    Y_UNIT_TEST(ShouldLimitUnconfirmedBlobCount)
    {
        auto config = DefaultConfig();
        config.SetWriteBlobThreshold(1);
        config.SetAddingUnconfirmedBlobsEnabled(true);
        config.SetUnconfirmedBlobCountHardLimit(1);
        auto runtime = PrepareTestActorRuntime(config);

        runtime->SetObserverFunc([&] (auto& event) {
            switch (event->GetTypeRewrite()) {
                case TEvPartitionPrivate::EvAddConfirmedBlobsRequest: {
                    return TTestActorRuntime::EEventAction::DROP;
                }
            }

            return TTestActorRuntime::DefaultObserverFunc(event);
        });

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        partition.WriteBlocks(10, 1);
        {
            // can't read unconfirmed range
            partition.SendReadBlocksRequest(10);
            auto response = partition.RecvReadBlocksResponse();
            UNIT_ASSERT_VALUES_EQUAL(E_REJECTED, response->GetError().GetCode());
        }

        // but next blob should be added bypassing 'unconfirmed blobs' feature
        partition.WriteBlocks(11, 2);
        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(2),
            GetBlockContent(partition.ReadBlocks(11))
        );
    }

    void DoTestCompression(
        ui32 writeBlobThreshold,
        ui32 uncompressedExpected,
        ui32 compressedExpected)
    {
        auto config = DefaultConfig();
        config.SetBlobCompressionRate(1);
        config.SetWriteBlobThreshold(writeBlobThreshold);
        config.SetFreshChannelCount(1);
        config.SetFreshChannelWriteRequestsEnabled(true);

        auto runtime = PrepareTestActorRuntime(config);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        ui32 uncompressed = 0;
        ui32 compressed = 0;

        auto obs =
            [&] (TAutoPtr<IEventHandle>& event) {
                if (event->GetTypeRewrite()
                        == TEvStatsService::EvVolumePartCounters)
                {
                    auto* msg =
                        event->Get<TEvStatsService::TEvVolumePartCounters>();

                    const auto& cc = msg->DiskCounters->Cumulative;
                    uncompressed = cc.UncompressedBytesWritten.Value;
                    compressed = cc.CompressedBytesWritten.Value;
                }

                return TTestActorRuntime::DefaultObserverFunc(event);
            };

        runtime->SetObserverFunc(obs);

        partition.WriteBlocks(1, 1);

        partition.SendToPipe(
            std::make_unique<TEvPartitionPrivate::TEvUpdateCounters>());
        {
            TDispatchOptions options;
            options.FinalEvents.emplace_back(
                TEvStatsService::EvVolumePartCounters);
            runtime->DispatchEvents(options);
        }

        UNIT_ASSERT_VALUES_EQUAL(uncompressedExpected, uncompressed);
        UNIT_ASSERT_VALUES_EQUAL(compressedExpected, compressed);
    }

    Y_UNIT_TEST(ShouldCompressBlobs)
    {
        DoTestCompression(1, DefaultBlockSize, 34);
    }

    Y_UNIT_TEST(ShouldCompressFreshBlocks)
    {
        DoTestCompression(Max<ui32>(), 4107, 43);
    }

    class TStatsChecker
    {
    private:
        ui64 RealWriteBlocksCount = 0;
        ui64 RealReadBlocksCount = 0;
        ui64 WriteBlocksCount = 0;
        ui64 ReadBlocksCount = 0;

    public:
        TStatsChecker() = default;

        void CheckStats(
            const NProto::TVolumeStats& stats,
            const ui64 realReadBlocksCount,
            const ui64 realWriteBlocksCount,
            const ui64 readBlocksCount,
            const ui64 writeBlocksCount)
        {
            RealReadBlocksCount += realReadBlocksCount;
            RealWriteBlocksCount += realWriteBlocksCount;
            ReadBlocksCount += readBlocksCount;
            WriteBlocksCount += writeBlocksCount;
            UNIT_ASSERT_VALUES_EQUAL(
                ReadBlocksCount, stats.GetSysReadCounters().GetBlocksCount());
            UNIT_ASSERT_VALUES_EQUAL(
                WriteBlocksCount, stats.GetSysWriteCounters().GetBlocksCount());
            UNIT_ASSERT_VALUES_EQUAL(
                RealReadBlocksCount,
                stats.GetRealSysReadCounters().GetBlocksCount());
            UNIT_ASSERT_VALUES_EQUAL(
                RealWriteBlocksCount,
                stats.GetRealSysWriteCounters().GetBlocksCount());
        }
    };

    Y_UNIT_TEST(CheckRealSysCountersDuringCompaction)
    {
        TStatsChecker statsChecker;
        auto config = DefaultConfig();
        config.SetWriteBlobThreshold(1_MB);
        config.SetBlobPatchingEnabled(true);
        config.SetHDDMaxBlobsPerRange(999);
        config.SetSSDMaxBlobsPerRange(999);
        config.SetCompactionGarbageThreshold(999);
        config.SetCompactionRangeGarbageThreshold(999);
        auto runtime = PrepareTestActorRuntime(
            config,
            MaxPartitionBlocksCount
        );

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        partition.WriteBlocks(TBlockRange32::WithLength(0, 1024), 1);
        partition.WriteBlocks(TBlockRange32::WithLength(0, 301), 2);

        partition.Compaction();
        partition.Cleanup();

        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            // Sys counter increased by "reading" 1024 - 301 UnchangedBlocks
            // Both counters increased by reading and writing 301 ChangedBlocks
            // from blobstorage
            statsChecker.CheckStats(stats, 301, 301, 1024, 1024);
        }

        partition.WriteBlocks(TBlockRange32::WithLength(0, 256), 3);

        partition.Compaction();
        partition.Cleanup();

        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            // Sys counter increased by "reading" 768 UnchangedBlocks
            // Both counters increased by "reading" 256 ChangedBlocks
            // from blobstorage
            statsChecker.CheckStats(stats, 256, 256, 1024, 1024);
        }

        partition.WriteBlocks(TBlockRange32::MakeClosedInterval(5, 14), 4);

        partition.Compaction();
        partition.Cleanup();

        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            // Blob wasn't written to BlobStorage, so counters don't change
            statsChecker.CheckStats(stats, 0, 0, 0, 0);
        }

        partition.Flush();

        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            // Flush writes to BlobStorage blocks from FreshBlocks,
            // so all counters increase by blocks count
            statsChecker.CheckStats(stats, 0, 10, 0, 10);
        }
        partition.Compaction();
        partition.Cleanup();

        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            // Sys counter increased by writing and reading whole blob of size
            // 1024. Real counter increased by 10 patchong blocks
            statsChecker.CheckStats(stats, 10, 10, 1024, 1024);
        }
    }

    Y_UNIT_TEST(CheckRealSysCountersDuringIncrementalCompaction)
    {
        TStatsChecker statsChecker;
        auto config = DefaultConfig();
        config.SetBlobPatchingEnabled(true);
        config.SetWriteBlobThreshold(1);   // disable FreshBlocks
        config.SetIncrementalCompactionEnabled(true);
        config.SetHDDMaxBlobsPerRange(4);
        config.SetSSDMaxBlobsPerRange(4);
        config.SetMaxSkippedBlobsDuringCompaction(1);
        config.SetTargetCompactionBytesPerOp(64_KB);
        auto runtime = PrepareTestActorRuntime(
            config,
            MaxPartitionBlocksCount
        );

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        partition.WriteBlocks(TBlockRange32::WithLength(0, 1024), 1);
        partition.WriteBlocks(TBlockRange32::MakeClosedInterval(2, 12), 2);
        partition.WriteBlocks(TBlockRange32::MakeClosedInterval(10, 20), 3);

        partition.Compaction();
        partition.Cleanup();

        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            // Incremental compaction divided data into two parts,
            // and compacted to one blob only small of them
            statsChecker.CheckStats(stats, 19, 19, 19, 19);
        }

        partition.WriteBlocks(TBlockRange32::MakeClosedInterval(7, 15), 4);
        partition.Compaction();
        partition.Cleanup();

        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            // Sys counters count whole blob, real sys counters count only
            // patches. Incremental compaction doesn't read the biggest blob.
            statsChecker.CheckStats(stats, 9, 9, 19, 19);
        }
    }

    Y_UNIT_TEST(ShouldRunCompactionIfBlobCountIsGreaterThanThreshold)
    {
        auto config = DefaultConfig();
        config.SetHDDCompactionType(NProto::CT_LOAD);
        config.SetV1GarbageCompactionEnabled(true);
        config.SetCompactionGarbageThreshold(999999999);
        config.SetCompactionRangeGarbageThreshold(999999999);
        config.SetSSDMaxBlobsPerUnit(7);
        config.SetHDDMaxBlobsPerUnit(7);

        ui32 blockCount =  1024 * 1024;

        auto runtime = PrepareTestActorRuntime(config, blockCount);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        ui64 compactionByBlobCount = 0;
        bool compactionRequestObserved = false;
        runtime->SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) {
                switch (event->GetTypeRewrite()) {
                    case TEvPartitionPrivate::EvCompactionRequest: {
                        compactionRequestObserved = true;
                        break;
                    }
                    case TEvStatsService::EvVolumePartCounters: {
                        auto* msg =
                            event->Get<TEvStatsService::TEvVolumePartCounters>();
                        const auto& cc = msg->DiskCounters->Cumulative;
                        compactionByBlobCount =
                            cc.CompactionByBlobCountPerDisk.Value;
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            }
        );

        for (size_t i = 0; i < 6; ++i) {
            partition.WriteBlocks(TBlockRange32::WithLength(i * 1024, 1024), i);
        }


        partition.SendToPipe(
            std::make_unique<TEvPartitionPrivate::TEvUpdateCounters>());
        {
            TDispatchOptions options;
            options.FinalEvents.emplace_back(
                TEvStatsService::EvVolumePartCounters);
            runtime->DispatchEvents(options);
        }
        UNIT_ASSERT_EQUAL(0, compactionByBlobCount);

        // wait for background operations completion
        runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        // blob count is less than 4 * 2 => no compaction
        UNIT_ASSERT(!compactionRequestObserved);

        for (size_t i = 6; i < 10; ++i) {
            partition.WriteBlocks(TBlockRange32::WithLength(i * 1024, 1024), i);
        }

        // wait for background operations completion
        runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        // blob count is greater than threshold on disk => compaction
        UNIT_ASSERT(compactionRequestObserved);

        partition.SendToPipe(
            std::make_unique<TEvPartitionPrivate::TEvUpdateCounters>());
        {
            TDispatchOptions options;
            options.FinalEvents.emplace_back(
                TEvStatsService::EvVolumePartCounters);
            runtime->DispatchEvents(options);
        }

        UNIT_ASSERT(0 < compactionByBlobCount);
    }

    void CheckIncrementAndDecrementCompactionPerRun(
        ui32 rangeCountPerRun,
        ui32 maxBlobsPerUnit,
        ui32 maxBlobsPerRange,
        ui32 diskGarbageThreshold,
        ui32 rangeGarbageThreshold,
        ui32 blobsCountAfterCompaction,
        ui32 increasingPercentageThreshold,
        ui32 decreasingPercentageThreshold,
        ui32 compactionRangeCountPerRun,
        ui32 maxCompactionRangeCountPerRun = 10)
    {
        auto config = DefaultConfig();
        config.SetWriteBlobThreshold(1_MB);
        config.SetBatchCompactionEnabled(true);
        config.SetV1GarbageCompactionEnabled(true);
        config.SetCompactionGarbageThreshold(diskGarbageThreshold);
        config.SetCompactionRangeGarbageThreshold(rangeGarbageThreshold);
        config.SetCompactionRangeCountPerRun(rangeCountPerRun);
        config.SetCompactionCountPerRunIncreasingThreshold(
            increasingPercentageThreshold);
        config.SetCompactionCountPerRunDecreasingThreshold(
            decreasingPercentageThreshold);
        config.SetHDDMaxBlobsPerUnit(maxBlobsPerUnit);
        config.SetSSDMaxBlobsPerUnit(maxBlobsPerUnit);
        config.SetMaxCompactionRangeCountPerRun(maxCompactionRangeCountPerRun);
        config.SetCompactionCountPerRunChangingPeriod(1);
        config.SetSSDMaxBlobsPerRange(maxBlobsPerRange);
        config.SetHDDMaxBlobsPerRange(maxBlobsPerRange);

        auto runtime = PrepareTestActorRuntime(
            config,
            4 * 1024 * 1024
        );
        runtime->AdvanceCurrentTime(TDuration::Seconds(5));

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        const auto blockRange1 = TBlockRange32::WithLength(0, 1024);
        const auto blockRange2 = TBlockRange32::WithLength(1024 * 1024, 1024);
        const auto blockRange3 = TBlockRange32::WithLength(2 * 1024 * 1024, 1024);

        bool compactionFilter = true;
        ui32 resultCompactionRangeCountPerRun = 0;

        runtime->SetEventFilter(
            [&] (TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& event) {
                switch (event->GetTypeRewrite()) {
                    case TEvPartitionPrivate::EvCompactionRequest: {
                        if (compactionFilter) {
                            return true;
                        }
                        compactionFilter = true;
                        break;
                    }
                    case TEvStatsService::EvVolumePartCounters: {
                        auto* msg =
                            event->Get<TEvStatsService::TEvVolumePartCounters>();
                        const auto& cc = msg->DiskCounters->Simple;
                        resultCompactionRangeCountPerRun =
                            cc.CompactionRangeCountPerRun.Value;
                    }
                }
                return false;
            }
        );

        partition.WriteBlocks(blockRange1, 1);
        partition.WriteBlocks(blockRange2, 4);
        partition.WriteBlocks(blockRange3, 7);

        partition.WriteBlocks(blockRange1, 2);
        partition.WriteBlocks(blockRange1, 3);
        partition.WriteBlocks(blockRange2, 5);
        partition.WriteBlocks(blockRange2, 6);
        partition.WriteBlocks(blockRange3, 8);
        partition.WriteBlocks(blockRange3, 9);
        partition.WriteBlocks(blockRange3, 10);
        // wait for background operations completion
        runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        compactionFilter = false;

        partition.Compaction();
        partition.Cleanup();

        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(blobsCountAfterCompaction,
                stats.GetMergedBlobsCount());
        }

        partition.SendToPipe(
            std::make_unique<TEvPartitionPrivate::TEvUpdateCounters>());
        {
            TDispatchOptions options;
            options.FinalEvents.emplace_back(
                TEvStatsService::EvVolumePartCounters);
            runtime->DispatchEvents(options);
            UNIT_ASSERT_VALUES_EQUAL(compactionRangeCountPerRun,
                resultCompactionRangeCountPerRun);
        }
    }

    Y_UNIT_TEST(ShouldDecrementBatchSizeWhenBlobsCountPerDiskIsSmall)
    {
        // blobs percentage: (10 - 9 / 10) * 100 = 10
        // 10 < 100, so batch size should decrement
        CheckIncrementAndDecrementCompactionPerRun(
            2, 9, 999999, 999999, 99999, 7, 200, 100, 1);
    }

    Y_UNIT_TEST(ShouldChangeBatchSizeDueToBlocksPerDisk)
    {
        // real blocks per disk: 1024 * 10 = 10240
        // total blocks per disk: = 1024 * 3 = 3072
        // garbage percentage: 100 * (10 - 3) * 1024 / 3072 = 233
        // percentage more threshold: 100 * (233 - 203) / 203 = 9


        // 9 > 8, so should increment and compact 2 ranges
        CheckIncrementAndDecrementCompactionPerRun(
                1, 1000, 99999, 203, 99999, 5, 8, 5, 2);

        // 9 < 15, so should decrement and compact only 1 range
        CheckIncrementAndDecrementCompactionPerRun(
                2, 1000, 99999, 203, 99999, 7, 30, 15, 1);
    }

    Y_UNIT_TEST(ShouldChangeBatchSizeDueToBlocksPerRangeCount)
    {
        // real blocks per last range: 1024 * 4
        // total blocks per disk: 1024
        // garbage percentage: 100 * (4 - 1) * 1024 / 1024 = 300
        // 100 * (300 - 280) / 280 = 7

        // 7 > 6, so should increment and compact 2 ranges
        CheckIncrementAndDecrementCompactionPerRun(
            1, 1000, 99999, 9999, 280, 5, 6, 4, 2);

        // 7 > 6, but should compact only 1 range due to maxRangeCountPerRun
        CheckIncrementAndDecrementCompactionPerRun(
            1, 1000, 99999, 9999, 280, 7, 6, 4, 1, 1);

        // 7 < 8, so should decrement and compact only 1 range
        CheckIncrementAndDecrementCompactionPerRun(
            2, 1000, 99999, 9999, 280, 7, 30, 8, 1);
    }

    Y_UNIT_TEST(ShouldDecrementBatchSizeWhenBlobsPerRangeCountIsSmall) {
        // CompactionScore in range3: 4 - 4 + eps
        // 100 * (eps - 0) / 1024 < 3, so should decrement and compact 1 range
        CheckIncrementAndDecrementCompactionPerRun(
            2, 1000, 4, 9999, 9999, 7, 50, 10, 1);
    }

    Y_UNIT_TEST(ShouldRespectCompactionCountPerRunChangingPeriod)
    {
        auto config = DefaultConfig();
        config.SetWriteBlobThreshold(1_MB);
        config.SetBatchCompactionEnabled(true);
        config.SetV1GarbageCompactionEnabled(true);
        config.SetCompactionGarbageThreshold(99999);
        config.SetCompactionRangeGarbageThreshold(280);
        config.SetCompactionRangeCountPerRun(1);
        config.SetCompactionCountPerRunIncreasingThreshold(6);
        config.SetCompactionCountPerRunDecreasingThreshold(4);
        config.SetHDDMaxBlobsPerUnit(1000);
        config.SetSSDMaxBlobsPerUnit(1000);
        config.SetMaxCompactionRangeCountPerRun(10);
        config.SetCompactionCountPerRunChangingPeriod(100000);

        auto runtime = PrepareTestActorRuntime(
            config,
            4 * 1024 * 1024
        );

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        const auto blockRange = TBlockRange32::WithLength(0, 1024);

        ui32 resultCompactionRangeCountPerRun = 0;

        runtime->SetEventFilter(
            [&] (TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& event) {
                switch (event->GetTypeRewrite()) {
                    case TEvStatsService::EvVolumePartCounters: {
                        auto* msg =
                            event->Get<TEvStatsService::TEvVolumePartCounters>();
                        const auto& cc = msg->DiskCounters->Simple;
                        resultCompactionRangeCountPerRun =
                            cc.CompactionRangeCountPerRun.Value;
                        break;
                    }
                }
                return false;
            }
        );

        for (int i = 0; i < 10; ++i) {
            partition.WriteBlocks(blockRange, i);
        }

        // wait for background operations completion
        runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        partition.Compaction();
        partition.Cleanup();

        partition.SendToPipe(
            std::make_unique<TEvPartitionPrivate::TEvUpdateCounters>());
        {
            TDispatchOptions options;
            options.FinalEvents.emplace_back(
                TEvStatsService::EvVolumePartCounters);
            runtime->DispatchEvents(options);
            UNIT_ASSERT_VALUES_EQUAL(1, resultCompactionRangeCountPerRun);
        }

        // CompactionRangeCountPerRun was updated more then period seconds ago
        // So it should be changed
        runtime->AdvanceCurrentTime(TDuration::Seconds(102));

        for (int i = 0; i < 10; ++i) {
            partition.WriteBlocks(blockRange, i);
        }

        // wait for background operations completion
        runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        partition.Compaction();
        partition.Cleanup();

        partition.SendToPipe(
            std::make_unique<TEvPartitionPrivate::TEvUpdateCounters>());
        {
            TDispatchOptions options;
            options.FinalEvents.emplace_back(
                TEvStatsService::EvVolumePartCounters);
            runtime->DispatchEvents(options);
            UNIT_ASSERT_VALUES_EQUAL(2, resultCompactionRangeCountPerRun);
        }

        runtime->AdvanceCurrentTime(TDuration::Seconds(50));

        for (int i = 0; i < 10; ++i) {
            partition.WriteBlocks(blockRange, i);
        }

        // wait for background operations completion
        runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));
        partition.Compaction();
        partition.Cleanup();

        partition.SendToPipe(
            std::make_unique<TEvPartitionPrivate::TEvUpdateCounters>());
        {
            TDispatchOptions options;
            options.FinalEvents.emplace_back(
                TEvStatsService::EvVolumePartCounters);
            runtime->DispatchEvents(options);
            // Shouldn't increase compactionRangeCountPerRun due to period
            UNIT_ASSERT_VALUES_EQUAL(2, resultCompactionRangeCountPerRun);
        }
    }

    template <typename FSend, typename FReceive>
    void DoShouldReportLongRunningBlobOperations(
        FSend sendRequest,
        FReceive receiveResponse,
        TEvPartitionCommonPrivate::TEvLongRunningOperation::EOperation
            expectedOperation,
        bool killPartition)
    {
        auto config = DefaultConfig();

        TTestPartitionInfo testPartitionInfo;
        testPartitionInfo.MediaKind = NCloud::NProto::STORAGE_MEDIA_SSD;
        auto runtime = PrepareTestActorRuntime(
            config,
            1024,
            {},
            testPartitionInfo,
            {},
            EStorageAccessMode::Default);

        // Enable Schedule for all actors!!!
        runtime->SetRegistrationObserverFunc(
            [](auto& runtime, const auto& parentId, const auto& actorId)
            {
                Y_UNUSED(parentId);
                runtime.EnableScheduleForActor(actorId);
            });

        // Make partition client.
        TPartitionClient partition(*runtime);
        partition.WaitReady();
        partition.WriteBlocks(TBlockRange32::WithLength(0, 255));

        // Make handler for stealing nested messages
        std::vector<std::unique_ptr<IEventHandle>> stolenRequests;
        auto requestThief = [&](TAutoPtr<IEventHandle>& event)
        {
            switch (event->GetTypeRewrite()) {
                case TEvBlobStorage::EvGetResult:
                case TEvBlobStorage::EvPutResult: {
                    stolenRequests.push_back(
                        std::unique_ptr<IEventHandle>{event.Release()});
                    return TTestActorRuntime::EEventAction::DROP;
                }
            }
            return TTestActorRuntime::DefaultObserverFunc(event);
        };

        // Make handler for intercepting EvLongRunningOperation message.
        // Attention! counters will be doubled, because we will intercept the
        // requests sent to TPartitionActor and TVolumeActor.
        ui32 longRunningBeginCount = 0;
        ui32 longRunningFinishCount = 0;
        ui32 longRunningPingCount = 0;
        ui32 longRunningCanceledCount = 0;

        auto takeCounters = [&](TAutoPtr<IEventHandle>& event)
        {
            using TEvLongRunningOperation =
                TEvPartitionCommonPrivate::TEvLongRunningOperation;
            using EReason = TEvLongRunningOperation::EReason;

            if (event->GetTypeRewrite() ==
                TEvPartitionCommonPrivate::EvLongRunningOperation)
            {
                auto* msg = event->Get<TEvLongRunningOperation>();

                UNIT_ASSERT_VALUES_EQUAL(expectedOperation, msg->Operation);

                switch (msg->Reason) {
                    case EReason::LongRunningDetected:
                        if (msg->FirstNotify) {
                            longRunningBeginCount++;
                        } else {
                            longRunningPingCount++;
                        }
                        break;
                    case EReason::Finished:
                        longRunningFinishCount++;
                        break;
                    case EReason::Cancelled:
                        longRunningCanceledCount++;
                        break;
                }
            }
            return TTestActorRuntime::DefaultObserverFunc(event);
        };

        // Ready to postpone request.
        runtime->SetObserverFunc(requestThief);

        // Starting the execution of the request. It won't
        // be finished since we stole it EvPutResult message.
        sendRequest(partition);
        runtime->DispatchEvents({}, TDuration());
        UNIT_ASSERT_VALUES_UNEQUAL(0, stolenRequests.size());

        // Ready to check counters.
        runtime->SetObserverFunc(takeCounters);

        // Wait for EvLongRunningOperation arrived.
        {
            TDispatchOptions options;
            options.FinalEvents.emplace_back(
                TEvPartitionCommonPrivate::EvLongRunningOperation);
            runtime->AdvanceCurrentTime(TDuration::Seconds(60));
            runtime->DispatchEvents(options, TDuration::Seconds(1));
        }

        // Wait #1 for EvLongRunningOperation ping arrived.
        {
            TDispatchOptions options;
            options.FinalEvents.emplace_back(
                TEvPartitionCommonPrivate::EvLongRunningOperation);
            runtime->AdvanceCurrentTime(TDuration::Seconds(60));
            runtime->DispatchEvents(options, TDuration::Seconds(1));
        }

        // Wait #2 for EvLongRunningOperation ping arrived.
        {
            TDispatchOptions options;
            options.FinalEvents.emplace_back(
                TEvPartitionCommonPrivate::EvLongRunningOperation);
            runtime->AdvanceCurrentTime(TDuration::Seconds(60));
            runtime->DispatchEvents(options, TDuration::Seconds(1));
        }

        if (killPartition) {
            partition.KillTablet();
        } else {
            // Returning stolen requests to complete the execution of the
            // request.
            for (auto& request: stolenRequests) {
                runtime->Send(request.release());
            }
        }

        // Wait for EvLongRunningOperation (finish or cancel) arrival.
        {
            TDispatchOptions options;
            options.FinalEvents.emplace_back(
                TEvPartitionCommonPrivate::EvLongRunningOperation);
            runtime->AdvanceCurrentTime(TDuration::Seconds(60));
            runtime->DispatchEvents(options, TDuration::Seconds(1));
        }

        // Wait for background operations completion.
        runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        // Check request completed.
        {
            auto response = receiveResponse(partition);
            UNIT_ASSERT_VALUES_EQUAL_C(
                killPartition ? E_REJECTED : S_OK,
                response->GetStatus(),
                response->GetErrorReason());
        }
        // Wait for EvVolumePartCounters arrived.
        {
            TDispatchOptions options;
            options.FinalEvents.emplace_back(
                TEvStatsService::EvVolumePartCounters);
            runtime->DispatchEvents(options);

            UNIT_ASSERT_VALUES_EQUAL(2, longRunningBeginCount);
            UNIT_ASSERT_VALUES_EQUAL(4, longRunningPingCount);
            UNIT_ASSERT_VALUES_EQUAL(
                killPartition ? 0 : 2,
                longRunningFinishCount);
            UNIT_ASSERT_VALUES_EQUAL(
                killPartition ? 2 : 0,
                longRunningCanceledCount);
        }

        if (!killPartition) {
            // smoke test for monpage
            auto channelsTab = partition.RemoteHttpInfo(
                BuildRemoteHttpQuery(TestTabletId, {}, "Channels"),
                HTTP_METHOD::HTTP_METHOD_GET);

            UNIT_ASSERT_C(channelsTab->Html.Contains("svg"), channelsTab->Html);
        }
    }

    Y_UNIT_TEST(ShouldReportLongRunningReadBlobOperations)
    {
        auto sendRequest = [](TPartitionClient& partition)
        {
            partition.SendReadBlocksRequest(0);
        };
        auto receiveResponse = [](TPartitionClient& partition)
        {
            return partition.RecvReadBlocksResponse();
        };

        DoShouldReportLongRunningBlobOperations(
            sendRequest,
            receiveResponse,
            TEvPartitionCommonPrivate::TEvLongRunningOperation::EOperation::
                ReadBlob,
            false);
    }

    Y_UNIT_TEST(ShouldReportLongRunningWriteBlobOperations)
    {
        auto sendRequest = [](TPartitionClient& partition)
        {
            partition.SendWriteBlocksRequest(
                TBlockRange32::WithLength(0, 255));
        };
        auto receiveResponse = [](TPartitionClient& partition)
        {
            return partition.RecvWriteBlocksResponse();
        };

        DoShouldReportLongRunningBlobOperations(
            sendRequest,
            receiveResponse,
            TEvPartitionCommonPrivate::TEvLongRunningOperation::EOperation::
                WriteBlob,
            false);
    }

    Y_UNIT_TEST(ShouldReportLongRunningReadBlobOperationCancel)
    {
        auto sendRequest = [](TPartitionClient& partition)
        {
            partition.SendReadBlocksRequest(0);
        };
        auto receiveResponse = [](TPartitionClient& partition)
        {
            return partition.RecvReadBlocksResponse();
        };

        DoShouldReportLongRunningBlobOperations(
            sendRequest,
            receiveResponse,
            TEvPartitionCommonPrivate::TEvLongRunningOperation::EOperation::
                ReadBlob,
            true);
    }

    Y_UNIT_TEST(ShouldReportLongRunningWriteBlobOperationCancel)
    {
        auto sendRequest = [](TPartitionClient& partition)
        {
            partition.SendWriteBlocksRequest(
                TBlockRange32::WithLength(0, 255));
        };
        auto receiveResponse = [](TPartitionClient& partition)
        {
            return partition.RecvWriteBlocksResponse();
        };

        DoShouldReportLongRunningBlobOperations(
            sendRequest,
            receiveResponse,
            TEvPartitionCommonPrivate::TEvLongRunningOperation::EOperation::
                WriteBlob,
            true);
    }

    void EnableReadBlobCorruption(
        TTestActorRuntime& runtime,
        ui16 checkedOffset = InvalidBlobOffset)
    {
        TVector<ui16> blobOffsets;

        runtime.SetEventFilter([=] (
            TTestActorRuntimeBase& runtime,
            TAutoPtr<IEventHandle>& event) mutable
        {
            Y_UNUSED(runtime);

            switch (event->GetTypeRewrite()) {
                case TEvPartitionCommonPrivate::EvReadBlobRequest: {
                    using TEv =
                        TEvPartitionCommonPrivate::TEvReadBlobRequest;
                    auto* msg = event->Get<TEv>();
                    blobOffsets = msg->BlobOffsets;

                    break;
                }
                case TEvBlobStorage::EvGetResult: {
                    auto* msg = event->Get<TEvBlobStorage::TEvGetResult>();
                    UNIT_ASSERT(!blobOffsets.empty());
                    if (checkedOffset == InvalidBlobOffset
                            || blobOffsets[0] == checkedOffset)
                    {
                        auto& rope = msg->Responses[0].Buffer;
                        auto dst = rope.begin();
                        char* to = const_cast<char*>(dst.ContiguousData());
                        memset(to, 0, dst.ContiguousSize());
                    }

                    break;
                }
            }

            return false;
        });
    }

    Y_UNIT_TEST(ShouldDetectBlockCorruptionInBlobs)
    {
        constexpr ui32 blockCount = 1024 * 1024;
        auto config = DefaultConfig();
        config.SetCheckBlockChecksumsInBlobsUponRead(true);
        auto runtime = PrepareTestActorRuntime(config, blockCount);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        const auto range = TBlockRange32::WithLength(0, 1024);
        partition.WriteBlocks(range, 1);

        EnableReadBlobCorruption(*runtime, 0);

        // direct read should fail
        {
            TVector<TString> blocks;
            auto sglist = ResizeBlocks(
                blocks,
                range.Size(),
                TString::TUninitialized(DefaultBlockSize));
            partition.SendReadBlocksLocalRequest(range, std::move(sglist));
            auto response = partition.RecvReadBlocksLocalResponse();
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_REJECTED,
                response->GetStatus(),
                response->GetErrorReason());
        }

        // compaction should also run into the same corrupt block and fail
        {
            partition.SendCompactionRequest(0);
            auto response = partition.RecvCompactionResponse();
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_REJECTED,
                response->GetStatus(),
                response->GetErrorReason());
        }

        const auto smallRange = TBlockRange32::WithLength(0, 1);
        partition.WriteBlocks(smallRange, 1);

        // data was rewritten - compaction shouldn't see blobOffset 0 anymore
        // => compaction won't read any corrupt data and should succeed
        {
            partition.SendCompactionRequest(0);
            auto response = partition.RecvCompactionResponse();
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                response->GetStatus(),
                response->GetErrorReason());
        }

        partition.Flush();

        // but now compaction should fail because we again corrupted a blob -
        // this time we corrupted the blob written by Flush
        {
            partition.SendCompactionRequest(0);
            auto response = partition.RecvCompactionResponse();
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_REJECTED,
                response->GetStatus(),
                response->GetErrorReason());
        }
    }

    Y_UNIT_TEST(ShouldDetectBlockCorruptionInBlobsWhenAddingUnconfirmedBlobs)
    {
        constexpr ui32 blockCount = 1024 * 1024;
        auto config = DefaultConfig();
        config.SetCheckBlockChecksumsInBlobsUponRead(true);
        config.SetAddingUnconfirmedBlobsEnabled(true);
        auto runtime = PrepareTestActorRuntime(config, blockCount);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        const auto range = TBlockRange32::WithLength(0, 1024);
        partition.WriteBlocks(range, 1);

        EnableReadBlobCorruption(*runtime);

        // direct read should fail
        TVector<TString> blocks;
        auto sglist = ResizeBlocks(
            blocks,
            range.Size(),
            TString::TUninitialized(DefaultBlockSize));
        partition.SendReadBlocksLocalRequest(range, std::move(sglist));
        auto response = partition.RecvReadBlocksLocalResponse();
        UNIT_ASSERT_VALUES_EQUAL_C(
            E_REJECTED,
            response->GetStatus(),
            response->GetErrorReason());
    }

    Y_UNIT_TEST(ShouldProperlyCalculateBlockChecksumsForBatchedWrites)
    {
        constexpr ui32 blockCount = 1024 * 1024;
        auto config = DefaultConfig();
        // enabling batching + checksum checks
        config.SetCheckBlockChecksumsInBlobsUponRead(true);
        config.SetWriteRequestBatchingEnabled(true);
        // removing the dependency on the current defaults
        config.SetWriteBlobThreshold(128_KB);
        config.SetMaxBlobRangeSize(128_MB);
        // disabling flush
        config.SetFlushThreshold(1_GB);
        // enabling fresh channel writes to make stats check a bit more
        // convenient
        config.SetFreshChannelCount(1);
        config.SetFreshChannelWriteRequestsEnabled(true);
        auto runtime = PrepareTestActorRuntime(config, blockCount);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        std::unique_ptr<IEventHandle> processQueue;
        bool intercept = true;

        runtime->SetEventFilter([&] (
            TTestActorRuntimeBase& runtime,
            TAutoPtr<IEventHandle>& event)
        {
            Y_UNUSED(runtime);

            switch (event->GetTypeRewrite()) {
                case TEvPartitionPrivate::EvProcessWriteQueue: {
                    if (intercept) {
                        processQueue.reset(event.Release());
                        intercept = false;

                        return true;
                    }

                    break;
                }
            }

            return false;
        });

        // the following 14 blocks get batched into mixed blob 1
        partition.SendWriteBlocksRequest(
            TBlockRange32::WithLength(208792, 1),
            'a');
        partition.SendWriteBlocksRequest(
            TBlockRange32::WithLength(208796, 1),
            'b');
        partition.SendWriteBlocksRequest(
            TBlockRange32::WithLength(208799, 1),
            'c');
        partition.SendWriteBlocksRequest(
            TBlockRange32::WithLength(208864, 1),
            'd');
        partition.SendWriteBlocksRequest(
            TBlockRange32::WithLength(208866, 10),
            'e');
        // the following 17 blocks get batched into mixed blob 2
        partition.SendWriteBlocksRequest(
            TBlockRange32::WithLength(251925, 17),
            'f');
        // and the remaining block goes to fresh blocks
        partition.SendWriteBlocksRequest(
            TBlockRange32::WithLength(800000, 1),
            'g');

        runtime->DispatchEvents({}, TDuration::Seconds(2));
        runtime->Send(processQueue.release());
        partition.RecvWriteBlocksResponse();

        // checking that mixed batching has actually worked
        {
            const auto stats = partition.StatPartition()->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(2, stats.GetMixedBlobsCount());
            UNIT_ASSERT_VALUES_EQUAL(31, stats.GetMixedBlocksCount());
            UNIT_ASSERT_VALUES_EQUAL(1, stats.GetFreshBlobsCount());
            UNIT_ASSERT_VALUES_EQUAL(1, stats.GetFreshBlocksCount());
        }

        // checking that we don't run into a failed checksum check upon read
        {
            TVector<TString> blocks;
            auto sglist = ResizeBlocks(
                blocks,
                1024,
                TString::TUninitialized(DefaultBlockSize));
            partition.SendReadBlocksLocalRequest(
                TBlockRange32::WithLength(208792, 1024),
                std::move(sglist));
            auto response = partition.RecvReadBlocksLocalResponse();
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                response->GetStatus(),
                response->GetErrorReason());
        }

        EnableReadBlobCorruption(*runtime);

        // checking that we actually saved the checksums
        {
            TVector<TString> blocks;
            auto sglist = ResizeBlocks(
                blocks,
                1024,
                TString::TUninitialized(DefaultBlockSize));
            partition.SendReadBlocksLocalRequest(
                TBlockRange32::WithLength(208792, 1024),
                std::move(sglist));
            auto response = partition.RecvReadBlocksLocalResponse();
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_REJECTED,
                response->GetStatus(),
                response->GetErrorReason());
        }
    }

    Y_UNIT_TEST(ShouldCancelRequestsOnTabletRestart)
    {
        constexpr ui32 blockCount = 1024 * 1024;
        auto runtime = PrepareTestActorRuntime({}, blockCount);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        const auto range = TBlockRange32::WithLength(0, 1024);
        partition.SendWriteBlocksRequest(range, 1);

        bool putSeen = false;
        runtime->SetEventFilter([&] (auto& runtime, auto& event) {
            Y_UNUSED(runtime);

            switch (event->GetTypeRewrite()) {
                case TEvBlobStorage::EvPutResult: {
                    putSeen = true;
                    return true;
                }
            }

            return false;
        });

        TDispatchOptions options;
        options.CustomFinalCondition = [&] {
            return putSeen;
        };
        runtime->DispatchEvents(options);

        partition.RebootTablet();

        auto response = partition.RecvWriteBlocksResponse();
        UNIT_ASSERT_VALUES_EQUAL_C(
            E_REJECTED,
            response->GetStatus(),
            response->GetErrorReason());

        UNIT_ASSERT_VALUES_EQUAL(
            "tablet is shutting down",
            response->GetErrorReason());
    }

    Y_UNIT_TEST(ShouldSubtractCleanupBlobsWhenCheckingForBlobsCountCompaction)
    {
        auto config = DefaultConfig();
        config.SetHDDCompactionType(NProto::CT_LOAD);
        config.SetV1GarbageCompactionEnabled(true);
        config.SetCompactionGarbageThreshold(999999999);
        config.SetCompactionRangeGarbageThreshold(999999999);
        config.SetSSDMaxBlobsPerUnit(7);
        config.SetHDDMaxBlobsPerUnit(7);

        ui32 blockCount =  1024 * 1024;

        auto runtime = PrepareTestActorRuntime(config, blockCount);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        ui64 compactionByBlobCount = 0;
        ui64 compactionRequestObserved = 0;
        runtime->SetEventFilter([&] (auto& runtime, auto& event) {
                Y_UNUSED(runtime);
                switch (event->GetTypeRewrite()) {
                    case TEvPartitionPrivate::EvCompactionRequest: {
                        ++compactionRequestObserved;
                        break;
                    }
                    case TEvPartitionPrivate::EvCleanupRequest: {
                        return true;
                    }
                    case TEvStatsService::EvVolumePartCounters: {
                        auto* msg =
                            event->template Get<TEvStatsService::TEvVolumePartCounters>();
                        const auto& cc = msg->DiskCounters->Cumulative;
                        compactionByBlobCount =
                            cc.CompactionByBlobCountPerDisk.Value;
                        break;
                    }
                }
                return false;
            }
        );

        for (size_t i = 0; i < 4; ++i) {
            partition.WriteBlocks(TBlockRange32::WithLength(i * 1024, 1024), i);
        }

        partition.SendToPipe(
            std::make_unique<TEvPartitionPrivate::TEvUpdateCounters>());
        {
            TDispatchOptions options;
            options.FinalEvents.emplace_back(
                TEvStatsService::EvVolumePartCounters);
            runtime->DispatchEvents(options);
        }
        UNIT_ASSERT_EQUAL(0, compactionByBlobCount);

        // wait for background operations completion
        runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        // blob count is less than 4 * 2 => no compaction
        UNIT_ASSERT(!compactionRequestObserved);

        for (size_t i = 0; i < 4; ++i) {
            partition.WriteBlocks(TBlockRange32::WithLength(i * 1024, 1024), i);
        }

        // wait for background operations completion
        runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        // blob count is greater than threshold on disk => only one compaction
        // because two blobs go to cleanup queue and just one is created
        UNIT_ASSERT_VALUES_EQUAL(1, compactionRequestObserved);

        partition.SendToPipe(
            std::make_unique<TEvPartitionPrivate::TEvUpdateCounters>());
        {
            TDispatchOptions options;
            options.FinalEvents.emplace_back(
                TEvStatsService::EvVolumePartCounters);
            runtime->DispatchEvents(options);
        }

        UNIT_ASSERT_VALUES_EQUAL(1, compactionByBlobCount);

        // wait for background operations completion
        runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        // no more compactions generated because blobs in cleanup queue are
        // considered to be already removed
        UNIT_ASSERT_VALUES_EQUAL(1, compactionRequestObserved);

        partition.SendToPipe(
            std::make_unique<TEvPartitionPrivate::TEvUpdateCounters>());
        {
            TDispatchOptions options;
            options.FinalEvents.emplace_back(
                TEvStatsService::EvVolumePartCounters);
            runtime->DispatchEvents(options);
        }

        UNIT_ASSERT_VALUES_EQUAL(0, compactionByBlobCount);
    }

    Y_UNIT_TEST(ShouldRunCompactionIfBlocksCountIsGreaterThanThreshold)
    {
        auto config = DefaultConfig();
        config.SetHDDCompactionType(NProto::CT_LOAD);
        config.SetV1GarbageCompactionEnabled(true);
        config.SetCompactionGarbageThreshold(20);
        config.SetCompactionRangeGarbageThreshold(999999999);
        config.SetSSDMaxBlobsPerUnit(999999999);
        config.SetHDDMaxBlobsPerUnit(999999999);

        ui32 blockCount =  10 * 1024;

        auto runtime = PrepareTestActorRuntime(config, blockCount);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        ui64 compactionByBlockCount = 0;
        bool compactionRequestObserved = false;
        runtime->SetEventFilter([&] (auto& runtime, auto& event) {
                Y_UNUSED(runtime);
                switch (event->GetTypeRewrite()) {
                    case TEvPartitionPrivate::EvCompactionRequest: {
                        compactionRequestObserved = true;
                        break;
                    }
                    case TEvPartitionPrivate::EvCleanupRequest: {
                        return true;
                    }
                    case TEvStatsService::EvVolumePartCounters: {
                        auto* msg =
                            event->template Get<TEvStatsService::TEvVolumePartCounters>();
                        const auto& cc = msg->DiskCounters->Cumulative;
                        compactionByBlockCount =
                            cc.CompactionByGarbageBlocksPerDisk.Value;
                    }
                }
                return false;
            }
        );

        for (size_t i = 0; i < 10; ++i) {
            partition.WriteBlocks(TBlockRange32::WithLength(i * 1024, 1024), i);
        }

        partition.SendToPipe(
            std::make_unique<TEvPartitionPrivate::TEvUpdateCounters>());
        {
            TDispatchOptions options;
            options.FinalEvents.emplace_back(
                TEvStatsService::EvVolumePartCounters);
            runtime->DispatchEvents(options);
        }
        UNIT_ASSERT_EQUAL(0, compactionByBlockCount);

        // wait for background operations completion
        runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        // garbage block count is less than 20%  => no compaction
        UNIT_ASSERT(!compactionRequestObserved);

        for (size_t i = 0; i < 2; ++i) {
            partition.WriteBlocks(TBlockRange32::WithLength(i * 1024, 1024), i);
        }

        // wait for background operations completion
        runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        // garbage block count is greater than threshold => compaction
        UNIT_ASSERT(compactionRequestObserved);

        partition.SendToPipe(
            std::make_unique<TEvPartitionPrivate::TEvUpdateCounters>());
        {
            TDispatchOptions options;
            options.FinalEvents.emplace_back(
                TEvStatsService::EvVolumePartCounters);
            runtime->DispatchEvents(options);
        }

        UNIT_ASSERT_VALUES_EQUAL(1, compactionByBlockCount);

        // wait for background operations completion
        runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        // no more compactions generated because blocks in cleanup queue are
        // considered to be already removed
        UNIT_ASSERT_VALUES_EQUAL(1, compactionRequestObserved);

        partition.SendToPipe(
            std::make_unique<TEvPartitionPrivate::TEvUpdateCounters>());
        {
            TDispatchOptions options;
            options.FinalEvents.emplace_back(
                TEvStatsService::EvVolumePartCounters);
            runtime->DispatchEvents(options);
        }

        UNIT_ASSERT_VALUES_EQUAL(0, compactionByBlockCount);
    }

    Y_UNIT_TEST(ShouldAbortCompactionIfReadBlobFailsWithDeadlineExceeded)
    {
        NProto::TStorageServiceConfig config;
        config.SetBlobStorageAsyncGetTimeoutHDD(TDuration::Seconds(1).MilliSeconds());
        auto runtime = PrepareTestActorRuntime(config);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        partition.WriteBlocks(3, 33);
        partition.Flush();

        ui32 failedReadBlob = 0;
        runtime->SetEventFilter([&]
            (TTestActorRuntimeBase& runtime, TAutoPtr<IEventHandle>& ev)
        {
            Y_UNUSED(runtime);

            if (ev->GetTypeRewrite() == TEvBlobStorage::EvVGet) {
                const auto* msg = ev->Get<TEvBlobStorage::TEvVGet>();
                if (msg->Record.GetHandleClass() == NKikimrBlobStorage::AsyncRead &&
                    msg->Record.GetMsgQoS().HasDeadlineSeconds())
                {
                    return true;
                }
            } else if (ev->GetTypeRewrite() == TEvStatsService::EvVolumePartCounters) {
                auto* msg =
                    ev->Get<TEvStatsService::TEvVolumePartCounters>();
                failedReadBlob =
                    msg->DiskCounters->Simple.ReadBlobDeadlineCount.Value;
            }
            return false;
        });

        partition.SendCompactionRequest();
        runtime->AdvanceCurrentTime(TDuration::Seconds(1));
        auto response = partition.RecvCompactionResponse();
        UNIT_ASSERT_VALUES_EQUAL(E_REJECTED, response->GetError().GetCode());

        runtime->AdvanceCurrentTime(TDuration::Seconds(15));

        partition.SendToPipe(
            std::make_unique<TEvPartitionPrivate::TEvUpdateCounters>());
        {
            TDispatchOptions options;
            options.FinalEvents.emplace_back(TEvStatsService::EvVolumePartCounters);
            runtime->DispatchEvents(options);
        }
        UNIT_ASSERT_VALUES_EQUAL(1, failedReadBlob);
    }

    Y_UNIT_TEST(ShouldAllowForcedCompactionRequestsInPresenseOfTabletCompaction)
    {
        constexpr ui32 rangesCount = 5;
        auto runtime = PrepareTestActorRuntime(DefaultConfig(), rangesCount * 1024);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        for (ui32 range = 0; range < rangesCount; ++range) {
            partition.WriteBlocks(
                TBlockRange32::WithLength(range * 1024, 1024),
                1);
        }
        partition.Flush();

        bool steal = true;
        runtime->SetEventFilter([&]
            (TTestActorRuntimeBase& runtime, TAutoPtr<IEventHandle>& ev)
        {
            Y_UNUSED(runtime);

            if (ev->GetTypeRewrite() == TEvPartitionPrivate::EvCompactionCompleted &&
                steal)
            {
                steal = false;
                return true;
            }
            return false;
        });

        partition.SendCompactionRequest(
            0,
            TCompactionOptions());
        partition.Compaction(
            0,
            TCompactionOptions().
                set(ToBit(ECompactionOption::Forced)));
    }

    Y_UNIT_TEST(ShouldAllowOnlyOneForcedCompactionRequestAtATime)
    {
        constexpr ui32 rangesCount = 5;
        auto runtime = PrepareTestActorRuntime(DefaultConfig(), rangesCount * 1024);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        for (ui32 range = 0; range < rangesCount; ++range) {
            partition.WriteBlocks(
                TBlockRange32::WithLength(range * 1024, 1024),
                1);
        }
        partition.Flush();

        bool steal = true;
        runtime->SetEventFilter([&]
            (TTestActorRuntimeBase& runtime, TAutoPtr<IEventHandle>& ev)
        {
            Y_UNUSED(runtime);

            if (ev->GetTypeRewrite() == TEvPartitionPrivate::EvCompactionCompleted &&
                steal)
            {
                steal = false;
                return true;
            }
            return false;
        });

        partition.SendCompactionRequest(
            0,
            TCompactionOptions().
                set(ToBit(ECompactionOption::Forced)));
        partition.SendCompactionRequest(
            0,
            TCompactionOptions().
                set(ToBit(ECompactionOption::Forced)));

        auto response = partition.RecvCompactionResponse();
        UNIT_ASSERT_VALUES_EQUAL(E_TRY_AGAIN, response->GetStatus());
    }

    Y_UNIT_TEST(ShouldProcessMultipleRangesUponGarbageCompaction)
    {
        auto config = DefaultConfig();
        config.SetBatchCompactionEnabled(true);
        config.SetGarbageCompactionRangeCountPerRun(3);
        config.SetV1GarbageCompactionEnabled(true);
        config.SetCompactionGarbageThreshold(20);
        config.SetCompactionRangeGarbageThreshold(999999);

        auto runtime = PrepareTestActorRuntime(config, MaxPartitionBlocksCount);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        {
            const auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetMergedBlobsCount());
        }

        TAutoPtr<IEventHandle> compactionRequest;
        const auto interceptCompactionRequest =
            [&compactionRequest](TAutoPtr<IEventHandle>& event)
        {
            if (event->GetTypeRewrite() ==
                TEvPartitionPrivate::EvCompactionRequest)
            {
                auto* msg =
                    event->Get<TEvPartitionPrivate::TEvCompactionRequest>();
                if (msg->Mode == TEvPartitionPrivate::GarbageCompaction) {
                    compactionRequest = event.Release();
                    return TTestActorRuntimeBase::EEventAction::DROP;
                }
            }
            return TTestActorRuntime::DefaultObserverFunc(event);
        };
        runtime->SetObserverFunc(interceptCompactionRequest);

        const auto blockRange1 = TBlockRange32::WithLength(0, 1024);
        const auto blockRange2 = TBlockRange32::WithLength(1024 * 1024, 1024);
        const auto blockRange3 =
            TBlockRange32::WithLength(2 * 1024 * 1024, 1024);

        partition.WriteBlocks(blockRange1, 1);
        partition.WriteBlocks(blockRange1, 2);

        partition.WriteBlocks(blockRange2, 3);
        partition.WriteBlocks(blockRange2, 4);
        partition.WriteBlocks(blockRange2, 5);

        partition.WriteBlocks(blockRange3, 6);
        partition.WriteBlocks(blockRange3, 7);
        partition.WriteBlocks(blockRange3, 8);

        {
            const auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(8, stats.GetMergedBlobsCount());
        }

        runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        UNIT_ASSERT(compactionRequest);
        runtime->Send(compactionRequest.Release());

        runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        partition.Cleanup();

        // checking that data wasn't corrupted
        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(2),
            GetBlockContent(partition.ReadBlocks(blockRange1.Start)));
        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(2),
            GetBlockContent(partition.ReadBlocks(blockRange1.End)));

        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(5),
            GetBlockContent(partition.ReadBlocks(blockRange2.Start)));
        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(5),
            GetBlockContent(partition.ReadBlocks(blockRange2.End)));

        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(8),
            GetBlockContent(partition.ReadBlocks(blockRange3.Start)));
        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(8),
            GetBlockContent(partition.ReadBlocks(blockRange3.End)));

        // checking that we now have 1 blob in each of the ranges
        {
            const auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(3, stats.GetMergedBlobsCount());
        }
    }

    Y_UNIT_TEST(ShouldProcessMultipleRangesUponForceCompaction)
    {
        auto config = DefaultConfig();
        config.SetBatchCompactionEnabled(true);
        config.SetForcedCompactionRangeCountPerRun(3);
        config.SetV1GarbageCompactionEnabled(false);

        auto runtime = PrepareTestActorRuntime(config, MaxPartitionBlocksCount);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        {
            const auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetMergedBlobsCount());
        }

        TVector<size_t> rangeSizes;
        const auto interceptCompactionRequest =
            [&rangeSizes](TAutoPtr<IEventHandle>& event)
        {
            if (event->GetTypeRewrite() ==
                TEvPartitionPrivate::EvCompactionRequest)
            {
                auto* msg =
                    event->Get<TEvPartitionPrivate::TEvCompactionRequest>();
                rangeSizes.push_back(msg->RangeBlockIndices.size());
            }
            return TTestActorRuntime::DefaultObserverFunc(event);
        };
        runtime->SetObserverFunc(interceptCompactionRequest);

        const auto blockRange1 = TBlockRange32::WithLength(0, 1024);
        const auto blockRange2 = TBlockRange32::WithLength(1024 * 1024, 1024);
        const auto blockRange3 =
            TBlockRange32::WithLength(2 * 1024 * 1024, 1024);
        const auto blockRange4 =
            TBlockRange32::WithLength(3 * 1024 * 1024, 1024);

        partition.WriteBlocks(blockRange1, 1);
        partition.WriteBlocks(blockRange1, 2);

        partition.WriteBlocks(blockRange2, 3);
        partition.WriteBlocks(blockRange2, 4);
        partition.WriteBlocks(blockRange2, 5);

        partition.WriteBlocks(blockRange3, 6);
        partition.WriteBlocks(blockRange3, 7);
        partition.WriteBlocks(blockRange3, 8);

        partition.WriteBlocks(blockRange4, 9);

        {
            const auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(9, stats.GetMergedBlobsCount());
        }

        partition.SendCompactRangeRequest(0, 0);
        runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));
        partition.Cleanup();

        UNIT_ASSERT_EQUAL(2, rangeSizes.size());
        UNIT_ASSERT_EQUAL(3, rangeSizes[0]);
        UNIT_ASSERT_EQUAL(1, rangeSizes[1]);

        // checking that data wasn't corrupted
        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(2),
            GetBlockContent(partition.ReadBlocks(blockRange1.Start)));
        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(2),
            GetBlockContent(partition.ReadBlocks(blockRange1.End)));

        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(5),
            GetBlockContent(partition.ReadBlocks(blockRange2.Start)));
        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(5),
            GetBlockContent(partition.ReadBlocks(blockRange2.End)));

        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(8),
            GetBlockContent(partition.ReadBlocks(blockRange3.Start)));
        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(8),
            GetBlockContent(partition.ReadBlocks(blockRange3.End)));

        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(9),
            GetBlockContent(partition.ReadBlocks(blockRange4.Start)));
        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent(9),
            GetBlockContent(partition.ReadBlocks(blockRange4.End)));

        // checking that we now have 1 blob in each of the ranges
        {
            const auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(4, stats.GetMergedBlobsCount());
        }
    }

    Y_UNIT_TEST(ShouldSkipEmptyRangesUponForcedCompactionWithMultipleRanges)
    {
        auto config = DefaultConfig();
        config.SetBatchCompactionEnabled(true);
        config.SetForcedCompactionRangeCountPerRun(3);
        config.SetV1GarbageCompactionEnabled(false);

        auto runtime = PrepareTestActorRuntime(config, MaxPartitionBlocksCount);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        {
            const auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetMergedBlobsCount());
        }

        const auto blockRange1 = TBlockRange32::WithLength(3 * 1024, 1024);
        const auto blockRange2 = TBlockRange32::WithLength(4 * 1024, 1024);

        partition.WriteBlocks(blockRange1, 1);
        partition.WriteBlocks(blockRange2, 2);

        const auto response = partition.CompactRange(0, 5119);
        UNIT_ASSERT_EQUAL(S_OK, response->GetError().GetCode());

        partition.SendGetCompactionStatusRequest(response->Record.GetOperationId());
        const auto compactionStatus = partition.RecvGetCompactionStatusResponse();
        UNIT_ASSERT(SUCCEEDED(compactionStatus->GetStatus()));
        UNIT_ASSERT_EQUAL(5, compactionStatus->Record.GetTotal());
    }

    Y_UNIT_TEST(ShouldBatchSmallWritesToFreshChannelIfThresholdNotExceeded)
    {
        NProto::TStorageServiceConfig config;
        config.SetWriteRequestBatchingEnabled(true);
        config.SetWriteBlobThreshold(2_MB);
        auto runtime = PrepareTestActorRuntime(config);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        const ui32 blockCount = 500;
        runtime->SetObserverFunc(
            PartitionBatchWriteCollector(*runtime, blockCount));

        for (ui32 i = 0; i < blockCount; ++i) {
            partition.SendWriteBlocksRequest(i, i);
        }

        for (ui32 i = 0; i < blockCount; ++i) {
            auto response = partition.RecvWriteBlocksResponse();
            UNIT_ASSERT(SUCCEEDED(response->GetStatus()));
        }

        auto response = partition.StatPartition();
        const auto& stats = response->Record.GetStats();
        UNIT_ASSERT_VALUES_EQUAL(0, stats.GetMixedBlocksCount());
        UNIT_ASSERT_VALUES_EQUAL(0, stats.GetMergedBlocksCount());
        UNIT_ASSERT_VALUES_EQUAL(blockCount, stats.GetFreshBlocksCount());

        // checking that drain-related counters are in a consistent state
        partition.Drain();
    }

    Y_UNIT_TEST(
        ShouldAutomaticallyRunCompactionAndWriteBlobToMixedChannelIfBlobSizeIsBelowTheThreshold)
    {
        static constexpr ui32 compactionThreshold = 4;

        auto config = DefaultConfig();
        config.SetHDDMaxBlobsPerRange(compactionThreshold);
        config.SetCompactionMergedBlobThresholdHDD(17_KB);

        auto runtime = PrepareTestActorRuntime(
            config,
            1024,
            {},
            {.MediaKind = NCloud::NProto::STORAGE_MEDIA_HYBRID});

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        for (size_t i = 1; i < compactionThreshold; ++i) {
            partition.WriteBlocks(i, i);
            partition.Flush();
        }

        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(
                compactionThreshold - 1,
                stats.GetMixedBlobsCount());
            UNIT_ASSERT_VALUES_EQUAL(
                compactionThreshold - 1,
                stats.GetMixedBlocksCount());
            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetMergedBlobsCount());
        }

        partition.WriteBlocks(0, 0);
        partition.Flush();

        // wait for background operations completion
        runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        // data size for compaction is less than CompactionMergedBlobThresholdHDD
        // so the whole data should be moved to a mixed channel
        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(
                compactionThreshold + 1,
                stats.GetMixedBlobsCount());
            UNIT_ASSERT_VALUES_EQUAL(
                compactionThreshold * 2,
                stats.GetMixedBlocksCount());
            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetMergedBlobsCount());
        }

        partition.Cleanup();

        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(1, stats.GetMixedBlobsCount());
            UNIT_ASSERT_VALUES_EQUAL(
                compactionThreshold,
                stats.GetMixedBlocksCount());
            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetMergedBlobsCount());
        }
    }

    Y_UNIT_TEST(CompactionShouldWriteDataToDifferentChannelsDependingOnThreshold)
    {
        auto config = DefaultConfig();
        config.SetCompactionMergedBlobThresholdHDD(17_KB);

        auto runtime = PrepareTestActorRuntime(
            config,
            1024,
            {},
            {.MediaKind = NCloud::NProto::STORAGE_MEDIA_HYBRID});

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        partition.WriteBlocks(1, 1);
        partition.Flush();
        partition.WriteBlocks(2, 2);
        partition.Flush();

        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(2, stats.GetMixedBlobsCount());
            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetMergedBlobsCount());
        }

        partition.WriteBlocks(0, 0);

        // data should be written to a mixed channel if data size is less than threshold
        partition.Compaction();

        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(3, stats.GetMixedBlobsCount());
            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetMergedBlobsCount());
        }

        partition.WriteBlocks(TBlockRange32::WithLength(3, 5), 3);
        partition.Flush();

        // data should be written to a merged channel if data size is greater than threshold
        partition.Compaction();

        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(4, stats.GetMixedBlobsCount());
            UNIT_ASSERT_VALUES_EQUAL(1, stats.GetMergedBlobsCount());
        }

        partition.Cleanup();

        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetMixedBlobsCount());
            UNIT_ASSERT_VALUES_EQUAL(1, stats.GetMergedBlobsCount());
        }
    }

    Y_UNIT_TEST(CompactionShouldMoveDataFromMergedToMixedWhenDataSizeIsAboveTheTreshold)
    {
        auto config = DefaultConfig();
        config.SetWriteBlobThreshold(3_KB);
        config.SetCompactionMergedBlobThresholdHDD(17_KB);

        auto runtime = PrepareTestActorRuntime(
            config,
            1024,
            {},
            {.MediaKind = NCloud::NProto::STORAGE_MEDIA_HYBRID});

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        // all data should be written to a merged channel directly because data
        // size is less than threshold
        for (int i = 0; i < 4; ++i) {
            partition.WriteBlocks(i, i);
        }

        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetFreshBlocksCount());
            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetMixedBlobsCount());
            UNIT_ASSERT_VALUES_EQUAL(4, stats.GetMergedBlobsCount());
        }

        // data should be moved to a mixed channel as a result of compaction,
        // because data is less than threshold
        partition.Compaction();

        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetFreshBlocksCount());
            UNIT_ASSERT_VALUES_EQUAL(1, stats.GetMixedBlobsCount());
            UNIT_ASSERT_VALUES_EQUAL(4, stats.GetMergedBlobsCount());
        }

        partition.Cleanup();

        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetFreshBlocksCount());
            UNIT_ASSERT_VALUES_EQUAL(1, stats.GetMixedBlobsCount());
            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetMergedBlobsCount());
        }

        for (int i = 0; i < 4; ++i) {
            UNIT_ASSERT_VALUES_EQUAL(
                GetBlockContent(i),
                GetBlockContent(partition.ReadBlocks(i))
            );
        }
    }

    Y_UNIT_TEST(CompactionShouldMoveMergedBlobWithHolesToMixedChannel)
    {
        auto config = DefaultConfig();
        config.SetWriteBlobThreshold(12_KB);
        config.SetCompactionMergedBlobThresholdHDD(1_MB);

        auto runtime = PrepareTestActorRuntime(
            config,
            1024,
            {},
            {.MediaKind = NCloud::NProto::STORAGE_MEDIA_HYBRID});

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        partition.WriteBlocks(TBlockRange32::WithLength(5, 5), '1');   // merged
        partition.WriteBlocks(TBlockRange32::WithLength(12, 1), '2');  // fresh
        partition.WriteBlocks(TBlockRange32::WithLength(35, 20), '3'); // merged

        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(1, stats.GetFreshBlocksCount());
            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetMixedBlobsCount());
            UNIT_ASSERT_VALUES_EQUAL(2, stats.GetMergedBlobsCount());
        }

        // data should be moved to a mixed channel as a result of compaction,
        // because data is less than threshold
        partition.Flush();
        partition.Compaction();
        partition.Cleanup();

        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetFreshBlocksCount());
            UNIT_ASSERT_VALUES_EQUAL(1, stats.GetMixedBlobsCount());
            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetMergedBlobsCount());
        }

        const auto checkValue = [&](std::vector<std::tuple<int, int, char>> values)
        {
            for (const auto& value: values) {
                for (int i = std::get<0>(value); i < std::get<1>(value); ++i) {
                    UNIT_ASSERT_VALUES_EQUAL(
                        GetBlockContent(std::get<2>(value)),
                        GetBlockContent(partition.ReadBlocks(i)));
                }
            }
        };

        checkValue({{5, 5, '1'}, {12, 1, '2'}, {35, 20, '3'}});

        partition.WriteBlocks(TBlockRange32::WithLength(0, 2), '4');   // fresh
        partition.Flush(); // mixed
        partition.WriteBlocks(TBlockRange32::WithLength(2, 2), '5');   // fresh
        partition.WriteBlocks(TBlockRange32::WithLength(12, 3), '6');   // merged
        partition.WriteBlocks(TBlockRange32::WithLength(27, 1), '7');   // fresh

        partition.Compaction();
        partition.Cleanup();

        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(3, stats.GetFreshBlocksCount());
            UNIT_ASSERT_VALUES_EQUAL(1, stats.GetMixedBlobsCount());
            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetMergedBlobsCount());
        }

        checkValue({{0, 2, '4'}, {2, 2, '5'}, {5, 5, '1'}, {12, 3, '6'}, {27, 1, '7'}, {35, 20, '3'}});
    }

    Y_UNIT_TEST(
        ShouldNotEraseSkippedBlocksFromIndexDuringIncrementalCompactionToMixed)
    {
        auto config = DefaultConfig();
        config.SetWriteBlobThreshold(1_MB);
        config.SetCompactionMergedBlobThresholdHDD(1_MB);
        config.SetIncrementalCompactionEnabled(true);
        config.SetMaxSkippedBlobsDuringCompaction(1);
        config.SetTargetCompactionBytesPerOp(1);

        auto runtime = PrepareTestActorRuntime(
            config,
            1024,
            {},
            {.MediaKind = NCloud::NProto::STORAGE_MEDIA_HYBRID});

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        // blob 1 needs to eventually have more live blocks than other blobs in order
        // not to be compacted
        partition.WriteBlocks(TBlockRange32::WithLength(5, 55), '1');
        partition.Flush();
        partition.WriteBlocks(TBlockRange32::WithLength(2, 10), '2');
        partition.Flush();
        partition.WriteBlocks(TBlockRange32::WithLength(45, 20), '3');
        partition.Flush();

        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(3, stats.GetMixedBlobsCount());
            UNIT_ASSERT_VALUES_EQUAL(85, stats.GetMixedBlocksCount());
            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetMergedBlocksCount());
        }

        partition.Compaction();
        partition.Cleanup();
        partition.CollectGarbage();

        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(2, stats.GetMixedBlobsCount());
            UNIT_ASSERT_VALUES_EQUAL(85, stats.GetMixedBlocksCount());
            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetMergedBlocksCount());
        }

        for (ui32 i = 12; i < 45; ++i) {
            UNIT_ASSERT_VALUES_EQUAL(
                GetBlocksContent('1'),
                GetBlocksContent(partition.ReadBlocks(i))
            );
        }

        for (ui32 i = 2; i < 12; ++i) {
            UNIT_ASSERT_VALUES_EQUAL(
                GetBlocksContent('2'),
                GetBlocksContent(partition.ReadBlocks(i))
            );
        }

        for (ui32 i = 45; i < 65; ++i) {
            UNIT_ASSERT_VALUES_EQUAL(
                GetBlocksContent('3'),
                GetBlocksContent(partition.ReadBlocks(i))
            );
        }
    }

    Y_UNIT_TEST(ShouldCheckRange)
    {
        constexpr ui32 blockCount = 1024 * 1024;
        auto runtime = PrepareTestActorRuntime(DefaultConfig(), blockCount);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        partition.WriteBlocks(
            TBlockRange32::MakeClosedInterval(0, 1024 * 10),
            1);
        partition.WriteBlocks(
            TBlockRange32::MakeClosedInterval(1024 * 5, 1024 * 11),
            1);

        const auto step = 16;
        for (ui32 i = 1024 * 10; i < 1024 * 12; i += step) {
            partition.WriteBlocks(TBlockRange32::WithLength(i, step), 1);
        }

        for (ui32 i = 1024 * 20; i < 1024 * 21; i += step) {
            partition.WriteBlocks(TBlockRange32::WithLength(i, step + 1), 1);
        }

        partition.WriteBlocks(
            TBlockRange32::MakeClosedInterval(1001111, 1001210),
            1);

        partition.ZeroBlocks(TBlockRange32::MakeClosedInterval(1024, 3023));
        partition.ZeroBlocks(TBlockRange32::MakeClosedInterval(5024, 5033));

        ui32 status = -1;
        ui32 error = -1;

        runtime->SetObserverFunc(
            [&](TAutoPtr<IEventHandle>& event)
            {
                switch (event->GetTypeRewrite()) {
                    case TEvVolume::EvCheckRangeResponse: {
                        using TEv = TEvVolume::TEvCheckRangeResponse;
                        const auto* msg = event->Get<TEv>();
                        error = msg->GetStatus();
                        status = msg->Record.GetStatus().GetCode();
                        break;
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        const auto checkRange = [&](ui32 idx, ui32 size)
        {
            status = -1;

            const auto response = partition.CheckRange("id", idx, size);

            TDispatchOptions options;
            options.FinalEvents.emplace_back(TEvVolume::EvCheckRangeResponse);
            runtime->DispatchEvents(options, TDuration::Seconds(3));

            UNIT_ASSERT_VALUES_EQUAL(S_OK, status);
            UNIT_ASSERT_VALUES_EQUAL(S_OK, error);
        };

        checkRange(0, 1024);
        checkRange(1024, 512);
        checkRange(1, 1);
        checkRange(1000, 1000);
    }

    Y_UNIT_TEST(ShouldCheckRangeWithBrokenBlocks)
    {
        constexpr ui32 blockCount = 1024 * 1024;
        auto runtime = PrepareTestActorRuntime(DefaultConfig(), blockCount);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        partition.WriteBlocks(
            TBlockRange32::MakeClosedInterval(0, 1024 * 10),
            1);
        partition.WriteBlocks(
            TBlockRange32::MakeClosedInterval(1024 * 5, 1024 * 11),
            1);

        const auto step = 16;
        for (ui32 i = 1024 * 10; i < 1024 * 12; i += step) {
            partition.WriteBlocks(TBlockRange32::WithLength(i, step), 1);
        }

        for (ui32 i = 1024 * 20; i < 1024 * 21; i += step) {
            partition.WriteBlocks(TBlockRange32::WithLength(i, step + 1), 1);
        }

        partition.WriteBlocks(
            TBlockRange32::MakeClosedInterval(1001111, 1001210),
            1);

        ui32 status = -1;
        ui32 error = -1;

        runtime->SetObserverFunc(
            [&](TAutoPtr<IEventHandle>& event)
            {
                switch (event->GetTypeRewrite()) {
                    case TEvVolume::EvCheckRangeResponse: {
                        using TEv = TEvVolume::TEvCheckRangeResponse;
                        const auto* msg = event->Get<TEv>();
                        status = msg->Record.GetStatus().GetCode();
                        error = msg->Record.GetError().GetCode();

                        break;
                    }
                    case TEvService::EvReadBlocksLocalResponse: {
                        using TEv = TEvService::TEvReadBlocksLocalResponse;

                        auto response = std::make_unique<TEv>(
                            MakeError(E_IO, "block is broken"));

                        runtime->Send(
                            new IEventHandle(
                                event->Recipient,
                                event->Sender,
                                response.release(),
                                0,   // flags
                                event->Cookie),
                            0);

                        return TTestActorRuntime::EEventAction::DROP;

                        break;
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        const auto checkRange = [&](ui32 idx, ui32 size)
        {
            status = -1;
            partition.SendCheckRangeRequest("id", idx, size);
            const auto response =
                partition.RecvResponse<TEvVolume::TEvCheckRangeResponse>();

            TDispatchOptions options;
            options.FinalEvents.emplace_back(TEvVolume::EvCheckRangeResponse);

            UNIT_ASSERT_VALUES_EQUAL(E_IO, status);
            UNIT_ASSERT_VALUES_EQUAL(S_OK, error);
        };
        checkRange(0, 1024);
        checkRange(1024, 512);
        checkRange(1, 1);
        checkRange(1000, 1000);
    }

    Y_UNIT_TEST(ShouldSuccessfullyCheckRangeIfDiskIsEmpty)
    {
        constexpr ui32 blockCount = 1024 * 1024;
        auto runtime = PrepareTestActorRuntime(DefaultConfig(), blockCount);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        const ui32 idx = 0;
        const ui32 size = 1;
        const auto response = partition.CheckRange("id", idx, size);

        TDispatchOptions options;
        options.FinalEvents.emplace_back(TEvVolume::EvCheckRangeResponse);

        runtime->DispatchEvents(options, TDuration::Seconds(1));

        UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());
        UNIT_ASSERT_VALUES_EQUAL(S_OK, response->Record.GetStatus().GetCode());
    }

    Y_UNIT_TEST(ShouldntCheckRangeWithBigBlockCount)
    {
        constexpr ui32 blockCount = 1024 * 1024;
        constexpr ui32 bytesPerStripe = 1024;
        NProto::TStorageServiceConfig config;
        config.SetBytesPerStripe(bytesPerStripe);
        auto runtime = PrepareTestActorRuntime(config, blockCount);

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        const ui32 idx = 0;

        partition.SendCheckRangeRequest(
            "id",
            idx,
            bytesPerStripe / DefaultBlockSize + 1);
        const auto response =
            partition.RecvResponse<TEvVolume::TEvCheckRangeResponse>();

        TDispatchOptions options;
        options.FinalEvents.emplace_back(TEvVolume::EvCheckRangeResponse);

        runtime->DispatchEvents(options, TDuration::Seconds(1));

        UNIT_ASSERT_VALUES_EQUAL(E_ARGUMENT, response->GetStatus());
    }

    Y_UNIT_TEST(ShouldGetSameChecksumsWhileCheckRangeSimmilarDisks)
    {
        constexpr ui32 blockCount = 1024 * 1024;

        auto runtime = PrepareTestActorRuntime(DefaultConfig(), blockCount);

        TPartitionClient partition1(*runtime);
        TPartitionClient partition2(*runtime);

        partition1.WaitReady();
        partition2.WaitReady();

        const auto writeData = [&](TPartitionClient& partition)
        {
            partition.WriteBlocks(
                TBlockRange32::MakeClosedInterval(0, 1024 * 10),
                42);
            partition.WriteBlocks(
                TBlockRange32::MakeClosedInterval(1024 * 5, 1024 * 15),
                99);
        };

        writeData(partition1);
        writeData(partition2);

        const auto response1 = partition1.CheckRange("id", 0, 1024, true);
        const auto response2 = partition2.CheckRange("id", 0, 1024, true);

        TDispatchOptions options;
        options.FinalEvents.emplace_back(TEvVolume::EvCheckRangeResponse);
        runtime->DispatchEvents(options, TDuration::Seconds(3));

        const auto& checksums1 = response1->Record.GetChecksums();
        const auto& checksums2 = response2->Record.GetChecksums();

        ASSERT_VECTORS_EQUAL(
            TVector<ui32>(checksums1.begin(), checksums1.end()),
            TVector<ui32>(checksums2.begin(), checksums2.end()));
    }

    Y_UNIT_TEST(ShouldGetDifferentChecksumsWhileCheckRangeDifferentDisks)
    {
        constexpr ui32 blockCount = 1024 * 1024;

        auto runtime = PrepareTestActorRuntime(DefaultConfig(), blockCount);

        TPartitionClient partition1(*runtime);
        TPartitionClient partition2(*runtime);

        partition1.WaitReady();
        partition2.WaitReady();

        partition1.WriteBlocks(
            TBlockRange32::MakeClosedInterval(0, 1024 * 10),
            42);

        partition2.WriteBlocks(
            TBlockRange32::MakeClosedInterval(0, 1024 * 10),
            99);

        const auto response1 = partition1.CheckRange("id", 0, 1024, true);
        const auto response2 = partition2.CheckRange("id", 0, 1024, true);

        TDispatchOptions options;
        options.FinalEvents.emplace_back(TEvVolume::EvCheckRangeResponse);
        runtime->DispatchEvents(options, TDuration::Seconds(3));

        const auto& checksums1 = response1->Record.GetChecksums();
        const auto& checksums2 = response2->Record.GetChecksums();

        UNIT_ASSERT_VALUES_EQUAL(
            checksums1.size(),
            checksums2.size());

        ui32 totalChecksums = 0;
        ui32 differentChecksums = 0;
        for (int i = 0; i < checksums1.size(); ++i) {
            if (checksums1.at(i) != checksums2.at(i)) {
                ++differentChecksums;
            }
            ++totalChecksums;
        }

        UNIT_ASSERT(differentChecksums*2 < totalChecksums);
    }

    void TestForcedCompaction(ui32 rangesPerRun)
    {
        auto config = DefaultConfig();
        config.SetBatchCompactionEnabled(true);
        config.SetForcedCompactionRangeCountPerRun(rangesPerRun);
        config.SetV1GarbageCompactionEnabled(false);
        config.SetWriteBlobThreshold(15_KB);
        config.SetIncrementalCompactionEnabled(true);

        auto runtime = PrepareTestActorRuntime(
            config,
            MaxPartitionBlocksCount,
            {},
            {.MediaKind = NCloud::NProto::STORAGE_MEDIA_HYBRID});

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        {
            partition.WriteBlocks(TBlockRange32::WithLength(0, 5), 1);
            partition.WriteBlocks(TBlockRange32::WithLength(1024, 5), 2);

            partition.WriteBlocks(TBlockRange32::WithLength(0, 1), 3);
            partition.WriteBlocks(TBlockRange32::WithLength(1028, 3), 4);
            partition.Flush();

            const auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(1, stats.GetMixedBlobsCount());
            UNIT_ASSERT_VALUES_EQUAL(4, stats.GetMixedBlocksCount());
            UNIT_ASSERT_VALUES_EQUAL(2, stats.GetMergedBlobsCount());
            UNIT_ASSERT_VALUES_EQUAL(10, stats.GetMergedBlocksCount());
        }

        {
            const auto response = partition.CompactRange(0, 0);
            UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());

            const auto statResponse = partition.StatPartition();
            const auto& stats = statResponse->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(1, stats.GetMixedBlobsCount());
            UNIT_ASSERT_VALUES_EQUAL(4, stats.GetMixedBlocksCount());
            UNIT_ASSERT_VALUES_EQUAL(4, stats.GetMergedBlobsCount());
            UNIT_ASSERT_VALUES_EQUAL(22, stats.GetMergedBlocksCount());
        }

        {
            partition.Cleanup();

            const auto statResponse = partition.StatPartition();
            const auto& stats = statResponse->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetMixedBlobsCount());
            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetMixedBlocksCount());
            UNIT_ASSERT_VALUES_EQUAL(2, stats.GetMergedBlobsCount());
            UNIT_ASSERT_VALUES_EQUAL(12, stats.GetMergedBlocksCount());

            UNIT_ASSERT_VALUES_EQUAL(
                GetBlockContent(3),
                GetBlockContent(partition.ReadBlocks(0)));
            UNIT_ASSERT_VALUES_EQUAL(
                GetBlockContent(1),
                GetBlockContent(partition.ReadBlocks(1)));
            UNIT_ASSERT_VALUES_EQUAL(
                GetBlockContent(2),
                GetBlockContent(partition.ReadBlocks(1024)));
            UNIT_ASSERT_VALUES_EQUAL(
                GetBlockContent(4),
                GetBlockContent(partition.ReadBlocks(1028)));
        }

        {
            const auto response = partition.CompactRange(0, 0);
            UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());
            partition.Cleanup();
        }

        // write to the same blocks, we need several blobs in one compacted range
        {
            partition.WriteBlocks(TBlockRange32::WithLength(0, 5), 12);
            partition.WriteBlocks(TBlockRange32::WithLength(1024, 5), 22);

            partition.WriteBlocks(TBlockRange32::WithLength(0, 1), 32);
            partition.WriteBlocks(TBlockRange32::WithLength(1028, 3), 42);
            partition.Flush();

            const auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(1, stats.GetMixedBlobsCount());
            UNIT_ASSERT_VALUES_EQUAL(4, stats.GetMixedBlocksCount());
            UNIT_ASSERT_VALUES_EQUAL(4, stats.GetMergedBlobsCount());
            UNIT_ASSERT_VALUES_EQUAL(22, stats.GetMergedBlocksCount());
        }

        {
            const auto response = partition.CompactRange(0, 0);
            UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());
            partition.Cleanup();

            const auto statResponse = partition.StatPartition();
            const auto& stats = statResponse->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetMixedBlobsCount());
            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetMixedBlocksCount());
            UNIT_ASSERT_VALUES_EQUAL(2, stats.GetMergedBlobsCount());
            UNIT_ASSERT_VALUES_EQUAL(12, stats.GetMergedBlocksCount());

            UNIT_ASSERT_VALUES_EQUAL(
                GetBlockContent(32),
                GetBlockContent(partition.ReadBlocks(0)));
            UNIT_ASSERT_VALUES_EQUAL(
                GetBlockContent(12),
                GetBlockContent(partition.ReadBlocks(1)));
            UNIT_ASSERT_VALUES_EQUAL(
                GetBlockContent(22),
                GetBlockContent(partition.ReadBlocks(1024)));
            UNIT_ASSERT_VALUES_EQUAL(
                GetBlockContent(42),
                GetBlockContent(partition.ReadBlocks(1028)));
        }
    }

    Y_UNIT_TEST(ShouldCompactSeveralBlobsInTheSameRangeWithOneRangePerRun)
    {
        TestForcedCompaction(1);
    }

    Y_UNIT_TEST(ShouldCompactSeveralBlobsInTheSameRangeWithSeveralRangesPerRun)
    {
        TestForcedCompaction(10);
    }

    Y_UNIT_TEST(ShouldWriteToMixedChannelOnHddIfThresholdExceeded)
    {
        auto config = DefaultConfig();
        config.SetWriteRequestBatchingEnabled(true);
        config.SetWriteBlobThreshold(3_MB);
        config.SetWriteMixedBlobThresholdHDD(512_KB);

        auto runtime = PrepareTestActorRuntime(
            config,
            4096,
            {},
            {.MediaKind = NCloud::NProto::STORAGE_MEDIA_HYBRID});

        TPartitionClient partition(*runtime);
        partition.WaitReady();

        partition.WriteBlocks(TBlockRange32::WithLength(0, 100));
        partition.WriteBlocks(TBlockRange32::WithLength(512, 512));
        partition.WriteBlocks(TBlockRange32::WithLength(2048, 1024));

        {
            auto response = partition.StatPartition();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(100, stats.GetFreshBlocksCount());
            UNIT_ASSERT_VALUES_EQUAL(512, stats.GetMixedBlocksCount());
            UNIT_ASSERT_VALUES_EQUAL(1, stats.GetMixedBlobsCount());
            UNIT_ASSERT_VALUES_EQUAL(1024, stats.GetMergedBlocksCount());
            UNIT_ASSERT_VALUES_EQUAL(1, stats.GetMergedBlobsCount());
        }
    }
}

}   // namespace NCloud::NBlockStore::NStorage::NPartition
