#include <cloud/blockstore/libs/diagnostics/block_digest.h>
#include <cloud/blockstore/libs/storage/api/fresh_blocks_writer.h>
#include <cloud/blockstore/libs/storage/fresh_blocks_writer/fresh_blocks_writer_actor.h>
#include <cloud/blockstore/libs/storage/partition/part.h>
#include <cloud/blockstore/libs/storage/partition/part_actor.h>
#include <cloud/blockstore/libs/storage/partition/part_events_private.h>
#include <cloud/blockstore/libs/storage/testlib/part_client.h>
#include <cloud/blockstore/libs/storage/testlib/test_env.h>
#include <cloud/blockstore/libs/storage/testlib/test_runtime.h>
#include <cloud/blockstore/libs/storage/testlib/ut_helpers.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/size_literals.h>

namespace NCloud::NBlockStore::NStorage::NPartition {

using namespace NActors;

using namespace NKikimr;

using namespace NCloud::NStorage;

using namespace NLWTrace;

using namespace NPartition;

namespace {

////////////////////////////////////////////////////////////////////////////////

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

////////////////////////////////////////////////////////////////////////////////

constexpr TDuration WaitTimeout = TDuration::Seconds(5);
constexpr ui32 DataChannelOffset = 3;
const TActorId VolumeActorId(0, "VVV");

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
    config.SetFreshBlocksWriterEnabled(true);

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

struct TConfigs
{
    TStorageConfigPtr StorageConfig;
    TDiagnosticsConfigPtr DiagnosticsConfig;
};

struct TTestEnv
{
    std::shared_ptr<TConfigs> Configs;
    NProto::TPartitionConfig PartitionConfig;
    std::unique_ptr<TTestActorRuntime> Runtime;

    TActorId PartitionActorId = {};

    TActorId FreshBlocksWriterActorId = {};

    void SetupRegisterObserver()
    {
        Runtime->SetRegistrationObserverFunc(
            [&](
                TTestActorRuntimeBase& runtime,
                const TActorId& parentId,
                const TActorId& actorId)
            {
                Y_UNUSED(parentId);
                auto actor =
                    dynamic_cast<TPartitionActor*>(runtime.FindActor(actorId));
                if (actor) {
                    PartitionActorId = actorId;

                    FreshBlocksWriterActorId = runtime.Register(
                        std::make_unique<
                            NFreshBlocksWriter::TFreshBlocksWriterActor>(
                            Configs->StorageConfig,
                            PartitionConfig,
                            EStorageAccessMode::Default,
                            TestTabletId,
                            0,
                            1,
                            actorId,
                            VolumeActorId,
                            Configs->DiagnosticsConfig,
                            CreateBlockDigestGeneratorStub(),
                            CreateProfileLogStub())
                            .release());
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
            NCloud::NProto::TFeaturesConfig()));

    auto partConfig = GetPartitionConfig(
        std::move(testPartitionInfo),
        blockCount,
        tabletInfo->Channels.size());

    auto diagConfig = CreateTestDiagnosticsConfig();

    auto configs = std::make_shared<TConfigs>(
        std::move(storageConfig),
        std::move(diagConfig));

    auto createFunc = [=](const TActorId& owner, TTabletStorageInfo* info)
    {
        auto tablet = CreatePartitionTablet(
            owner,
            info,
            configs->StorageConfig,
            configs->DiagnosticsConfig,
            CreateProfileLogStub(),
            CreateBlockDigestGeneratorStub(),
            partConfig,
            storageAccessMode,
            0,   // partitionIndex
            1,   // siblingCount
            VolumeActorId,
            0   // volumeTabletId
        );
        return tablet.release();
    };

    auto bootstrapper =
        CreateTestBootstrapper(*runtime, tabletInfo.release(), createFunc);
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
    {}

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
        return std::unique_ptr<TResponse>(
            handle->Release<TResponse>().Release());
    }

    auto CreateWaitReadyRequest()
    {
        return std::make_unique<
            NFreshBlocksWriter::TEvFreshBlocksWriter::TEvWaitReadyRequest>();
    }

    std::unique_ptr<TEvService::TEvWriteBlocksRequest> CreateWriteBlocksRequest(
        const TBlockRange32& writeRange,
        char fill)
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

    std::unique_ptr<TEvService::TEvWriteBlocksRequest> CreateWriteBlocksRequest(
        ui32 blockIndex,
        char fill)
    {
        return CreateWriteBlocksRequest(
            TBlockRange32::WithLength(blockIndex, 1),
            fill);
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
        return CreateReadBlocksRequest(
            TBlockRange32::WithLength(blockIndex, 1),
            checkpointId);
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

    BLOCKSTORE_FRESH_BLOCKS_WRITER_REQUESTS(
        BLOCKSTORE_DECLARE_METHOD,
        NFreshBlocksWriter::TEvFreshBlocksWriter);

    BLOCKSTORE_PARTITION_REQUESTS_FWD_SERVICE(
        BLOCKSTORE_DECLARE_METHOD,
        TEvService)
    BLOCKSTORE_PARTITION_REQUESTS_FWD_VOLUME(
        BLOCKSTORE_DECLARE_METHOD,
        TEvVolume)

#undef BLOCKSTORE_DECLARE_METHOD
};

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

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TFreshBlocksWriterTest)
{
    Y_UNIT_TEST(ShouldWaitReady)
    {
        auto testEnv = PrepareTestActorRuntime();

        TPartitionClient partition(*testEnv.Runtime);
        partition.WaitReady();

        TFreshBlocksWriterClient fbwClient(
            *testEnv.Runtime,
            testEnv.FreshBlocksWriterActorId);

        fbwClient.WaitReady();

        auto req = std::make_unique<TEvVolume::TEvGetPartitionInfoRequest>();
        fbwClient.Send(std::move(req));
        auto response =
            fbwClient.RecvResponse<TEvVolume::TEvGetPartitionInfoResponse>();
        UNIT_ASSERT_C(
            SUCCEEDED(response->GetStatus()),
            response->GetErrorReason());
    }

    Y_UNIT_TEST(ShouldAddFreshBlocksBeforeReply)
    {
        auto testEnv = PrepareTestActorRuntime();
        auto& runtime = *testEnv.Runtime;

        bool addFreshBlocksRequestObserved = false;

        runtime.SetEventFilter(
            [&](TTestActorRuntimeBase& runtime, TAutoPtr<IEventHandle>& event)
            {
                Y_UNUSED(runtime);

                if (event->GetTypeRewrite() ==
                    TEvPartitionCommonPrivate::EvAddFreshBlocksRequest)
                {
                    addFreshBlocksRequestObserved = true;
                }

                // Drop add fresh blocks response to be sure that we don't wait
                // partition.
                if (event->GetTypeRewrite() ==
                    TEvPartitionCommonPrivate::EvAddFreshBlocksResponse)
                {
                    return true;
                }

                if (event->GetTypeRewrite() ==
                    TEvService::EvWriteBlocksResponse) {
                    UNIT_ASSERT(addFreshBlocksRequestObserved);
                }

                return event->GetTypeRewrite() ==
                       TEvPartitionCommonPrivate::EvTrimFreshLogRequest;
            });

        TPartitionClient partition(runtime);
        partition.WaitReady();

        TFreshBlocksWriterClient fbwClient(
            runtime,
            testEnv.FreshBlocksWriterActorId);

        fbwClient.WaitReady();

        fbwClient.WriteBlocks(0, '0');
    }


    Y_UNIT_TEST(ShouldWriteFreshBlocks)
    {
        auto testEnv = PrepareTestActorRuntime();
        auto& runtime = *testEnv.Runtime;

        // TODO(issue-4875): remove trim events dropping after adding trim
        // synchronization
        runtime.SetEventFilter(
            [&](TTestActorRuntimeBase& runtime, TAutoPtr<IEventHandle>& event)
            {
                Y_UNUSED(runtime);
                if (event->GetTypeRewrite() == TEvService::EvWriteBlocksRequest) {
                    UNIT_ASSERT_VALUES_UNEQUAL(testEnv.PartitionActorId, event->GetRecipientRewrite());

                    return false;
                }
                return event->GetTypeRewrite() ==
                       TEvPartitionCommonPrivate::EvTrimFreshLogRequest;
            });

        TPartitionClient partition(runtime);
        partition.WaitReady();

        TFreshBlocksWriterClient fbwClient(
            runtime,
            testEnv.FreshBlocksWriterActorId);

        fbwClient.WaitReady();

        fbwClient.WriteBlocks(0, '0');

        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent('0'),
            GetBlockContent(fbwClient.ReadBlocks(0)));

        fbwClient.WriteBlocks(1, '1');

        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent('1'),
            GetBlockContent(fbwClient.ReadBlocks(1)));

        fbwClient.WriteBlocks(2, '2');

        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent('2'),
            GetBlockContent(fbwClient.ReadBlocks(2)));
        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent('1'),
            GetBlockContent(fbwClient.ReadBlocks(1)));
        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent('0'),
            GetBlockContent(fbwClient.ReadBlocks(0)));
    }

    Y_UNIT_TEST(ShouldPassLargeWritesToPartition)
    {
        auto testEnv = PrepareTestActorRuntime();
        auto& runtime = *testEnv.Runtime;

        // TODO(issue-4875): remove trim events dropping after adding trim
        // synchronization
        runtime.SetEventFilter(
            [&](TTestActorRuntimeBase& runtime, TAutoPtr<IEventHandle>& event)
            {
                Y_UNUSED(runtime);
                if (event->GetTypeRewrite() ==
                    TEvPartitionCommonPrivate::EvWriteBlobRequest)
                {
                    UNIT_ASSERT_VALUES_UNEQUAL(
                        testEnv.FreshBlocksWriterActorId,
                        event->GetRecipientRewrite());

                    return false;
                }
                return event->GetTypeRewrite() ==
                       TEvPartitionCommonPrivate::EvTrimFreshLogRequest;
            });

        TPartitionClient partition(runtime);
        partition.WaitReady();

        TFreshBlocksWriterClient fbwClient(
            runtime,
            testEnv.FreshBlocksWriterActorId);

        fbwClient.WaitReady();

        fbwClient.WriteBlocks(TBlockRange32::WithLength(0, 1024), 'A');

        UNIT_ASSERT_VALUES_EQUAL(
            GetBlocksContent('A', 1024),
            GetBlocksContent(
                fbwClient.ReadBlocks(TBlockRange32::WithLength(0, 1024))));

        fbwClient.WriteBlocks(TBlockRange32::WithLength(0, 32), 'B');

        UNIT_ASSERT_VALUES_EQUAL(
            GetBlocksContent('B', 32),
            GetBlocksContent(
                fbwClient.ReadBlocks(TBlockRange32::WithLength(0, 32))));
    }

    Y_UNIT_TEST(ShouldWriteBlocksWithCorrectCommitId)
    {
        auto testEnv = PrepareTestActorRuntime(DefaultConfig(), 2048);
        auto& runtime = *testEnv.Runtime;

        // TODO(issue-4875): remove trim events dropping after adding trim
        // synchronization
        runtime.SetEventFilter(
            [&](TTestActorRuntimeBase& runtime, TAutoPtr<IEventHandle>& event)
            {
                Y_UNUSED(runtime);
                return event->GetTypeRewrite() ==
                       TEvPartitionCommonPrivate::EvTrimFreshLogRequest;
            });

        TPartitionClient partition(runtime);
        partition.WaitReady();

        TFreshBlocksWriterClient fbwClient(
            runtime,
            testEnv.FreshBlocksWriterActorId);

        fbwClient.WaitReady();

        // Partition write
        // 0 1 2 3 4 5 6 7 ...
        // A A A A A A A A ...
        fbwClient.WriteBlocks(TBlockRange32::WithLength(0, 1024), 'A');

        // FBW write
        // 0 1 2 3 4 5 6 7 ...
        // A B A A A A A A ...
        fbwClient.WriteBlocks(1, 'B');

        // FBW write
        // 0 1 2 3 4 5 6 7 ...
        // A B A C C C A A ...
        fbwClient.WriteBlocks(TBlockRange32::WithLength(3, 3), 'C');

        // Partition write
        // 0 1 2 3 4 5 6 7 ...
        // A B A C C D D D ...
        fbwClient.WriteBlocks(TBlockRange32::WithLength(5, 1024), 'D');

        // FBW write
        // 0 1 2 3 4 5 6 7 ...
        // A B A C E E D D ...
        fbwClient.WriteBlocks(TBlockRange32::WithLength(4, 2), 'E');

        auto actualContent = GetBlocksContent(
            fbwClient.ReadBlocks(TBlockRange32::WithLength(0, 7)));

        TStringBuilder expectedContent;
        expectedContent << GetBlockContent('A') << GetBlockContent('B')
                        << GetBlockContent('A') << GetBlockContent('C')
                        << GetBlockContent('E') << GetBlockContent('E')
                        << GetBlockContent('D');

        UNIT_ASSERT_EQUAL(expectedContent, actualContent);
    }
}

}   // namespace NCloud::NBlockStore::NStorage::NPartition
