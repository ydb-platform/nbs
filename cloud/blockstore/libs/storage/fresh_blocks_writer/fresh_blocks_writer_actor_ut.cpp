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
#include <cloud/blockstore/libs/storage/fresh_blocks_writer/fresh_blocks_writer_actor.h>
#include <cloud/blockstore/libs/storage/model/channel_data_kind.h>
#include <cloud/blockstore/libs/storage/partition/model/fresh_blob.h>
#include <cloud/blockstore/libs/storage/partition/model/fresh_blob_test.h>
#include <cloud/blockstore/libs/storage/partition/part.h>
#include <cloud/blockstore/libs/storage/partition/part_actor.h>
#include <cloud/blockstore/libs/storage/partition/part_events_private.h>
#include <cloud/blockstore/libs/storage/partition_common/events_private.h>
#include <cloud/blockstore/libs/storage/testlib/part_client.h>
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

using namespace NPartition;

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

struct TTestEnv
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

TTestEnv PrepareTestActorRuntime(
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

    auto testEnv = TTestEnv{
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
