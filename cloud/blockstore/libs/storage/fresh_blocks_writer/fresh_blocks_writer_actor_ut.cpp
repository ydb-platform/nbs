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

#include <cloud/storage/core/libs/common/helpers.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/size_literals.h>

namespace NCloud::NBlockStore::NStorage::NPartition {

using namespace NActors;

using namespace NKikimr;

using namespace NCloud::NStorage;

using namespace NLWTrace;

using namespace NPartition;

using namespace std::chrono_literals;

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
        NCloud::NProto::STORAGE_MEDIA_SSD;
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

    std::unique_ptr<TEvService::TEvWriteBlocksLocalRequest>
    CreateWriteBlocksLocalRequest(
        const TBlockRange32& writeRange,
        TGuardedSgList sglist,
        ui64 blockSize)
    {
        auto request =
            std::make_unique<TEvService::TEvWriteBlocksLocalRequest>();
        request->Record.SetStartIndex(writeRange.Start);
        request->Record.Sglist = std::move(sglist);
        request->Record.BlocksCount = writeRange.Size();
        request->Record.BlockSize = blockSize;
        return request;
    }

    std::unique_ptr<TEvService::TEvWriteBlocksLocalRequest>
    CreateWriteBlocksLocalRequest(
        const TBlockRange32& writeRange,
        TStringBuf blockContent)
    {
        TSgList sglist;
        sglist.resize(
            writeRange.Size(),
            {blockContent.data(), blockContent.size()});

        return CreateWriteBlocksLocalRequest(
            writeRange,
            TGuardedSgList(std::move(sglist)),
            blockContent.size() / writeRange.Size());
    }


    std::unique_ptr<TEvService::TEvWriteBlocksLocalRequest>
    CreateWriteBlocksLocalRequest(
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
        auto config = DefaultConfig();
        config.SetWriteBlobThresholdSSD(128_KB);

        auto testEnv = PrepareTestActorRuntime(config);
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

    Y_UNIT_TEST(ShouldSupportWriteBlocksLocalRequest)
    {
        auto testEnv = PrepareTestActorRuntime();
        auto& runtime = *testEnv.Runtime;

        std::unique_ptr<IEventHandle> stolenAddFreshBlocksRequest;

        bool writeBlocksCompletedObserved = false;

        runtime.SetObserverFunc(
            [&](TAutoPtr<IEventHandle>& event)
            {
                // Drop add fresh blocks response to be sure that we don't wait
                // partition.
                if (event->GetTypeRewrite() ==
                    TEvPartitionCommonPrivate::EvAddFreshBlocksRequest)
                {
                    stolenAddFreshBlocksRequest.reset(event.Release());
                    return TTestActorRuntime::EEventAction::DROP;
                }

                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        TPartitionClient partition(runtime);
        partition.WaitReady();

        TFreshBlocksWriterClient fbwClient(
            runtime,
            testEnv.FreshBlocksWriterActorId);
        fbwClient.WaitReady();

        auto partActor = testEnv.PartitionActorId;
        {
            auto content = GetBlockContent('0');

            TSgList sglist;
            sglist.resize(1, {content.data(), content.size()});
            TGuardedSgList guardedSglist(std::move(sglist));
            fbwClient.WriteBlocksLocal(
                TBlockRange32::WithLength(0, 1),
                guardedSglist,
                4_KB);
            guardedSglist.Close();
        }

        runtime.SetObserverFunc(
            [&](TAutoPtr<IEventHandle>& event)
            {
                writeBlocksCompletedObserved |=
                    event->GetTypeRewrite() ==
                    TEvPartitionCommonPrivate::EvWriteFreshBlocksCompleted;

                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        runtime.SendAsync(stolenAddFreshBlocksRequest.release());

        runtime.DispatchEvents({}, 10ms);

        UNIT_ASSERT(writeBlocksCompletedObserved);
        // If part actor commits suicide, we will re register new part
        // actor.
        UNIT_ASSERT_VALUES_EQUAL(partActor, testEnv.PartitionActorId);
    }

    Y_UNIT_TEST(ShouldBatchSmallWritesInOneWriteBlobRequest)
    {
        auto config = DefaultConfig();
        config.SetWriteBlobThresholdSSD(128_KB);
        config.SetWriteRequestBatchingEnabled(true);

        auto testEnv = PrepareTestActorRuntime(config);
        auto& runtime = *testEnv.Runtime;

        TPartitionClient partition(runtime);
        partition.WaitReady();

        TFreshBlocksWriterClient fbwClient(
            runtime,
            testEnv.FreshBlocksWriterActorId);
        fbwClient.WaitReady();

        const ui32 blockCount = 1000;
        ui64 writeBlobRequestCount = 0;

        runtime.SetObserverFunc(
            [&](TAutoPtr<IEventHandle>& event)
            {
                if (event->GetTypeRewrite() ==
                    TEvPartitionCommonPrivate::EvWriteBlobRequest)
                {
                    ++writeBlobRequestCount;
                }

                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        for (ui32 i = 0; i < blockCount; ++i) {
            fbwClient.SendWriteBlocksRequest(i, i);
        }

        for (ui32 i = 0; i < blockCount; ++i) {
            auto response = fbwClient.RecvWriteBlocksResponse();
            UNIT_ASSERT(SUCCEEDED(response->GetStatus()));
        }

        const ui64 blocksCountInOneBatch =
            config.GetWriteBlobThresholdSSD() / 4_KB  - 1;

        UNIT_ASSERT_VALUES_EQUAL(
            static_cast<ui64>(
                std::ceil((1.0 * blockCount) / blocksCountInOneBatch)),
            writeBlobRequestCount);

        for (ui32 i = 0; i < blockCount; ++i) {
            UNIT_ASSERT_VALUES_EQUAL(
                GetBlockContent(i),
                GetBlockContent(fbwClient.ReadBlocks(i)));
        }
    }

    Y_UNIT_TEST(ShouldSupportFreshZeroRequests)
    {
        auto testEnv = PrepareTestActorRuntime();
        auto& runtime = *testEnv.Runtime;

        // TODO(issue-4875): remove trim events dropping after adding trim
        // synchronization
        runtime.SetEventFilter(
            [&](TTestActorRuntimeBase& runtime, TAutoPtr<IEventHandle>& event)
            {
                Y_UNUSED(runtime);
                if (event->GetTypeRewrite() == TEvService::EvWriteBlocksRequest)
                {
                    UNIT_ASSERT_VALUES_UNEQUAL(
                        testEnv.PartitionActorId,
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

        fbwClient.WriteBlocks(0, '0');
        fbwClient.WriteBlocks(1, '1');
        fbwClient.WriteBlocks(2, '2');

        fbwClient.ZeroBlocks(0);
        {
            TStringBuilder expectedContent;
            expectedContent << GetBlockContent('\0') << GetBlockContent('1')
                            << GetBlockContent('2');

            UNIT_ASSERT_VALUES_EQUAL(
                expectedContent,
                GetBlocksContent(
                    fbwClient.ReadBlocks(TBlockRange32::WithLength(0, 3))));
        }

        fbwClient.ZeroBlocks(1);
        {
            TStringBuilder expectedContent;
            expectedContent << GetBlockContent('\0') << GetBlockContent('\0')
                            << GetBlockContent('2');

            UNIT_ASSERT_VALUES_EQUAL(
                expectedContent,
                GetBlocksContent(
                    fbwClient.ReadBlocks(TBlockRange32::WithLength(0, 3))));
        }

        fbwClient.ZeroBlocks(2);
        {
            UNIT_ASSERT_VALUES_EQUAL(
                TString{},
                GetBlocksContent(
                    fbwClient.ReadBlocks(TBlockRange32::WithLength(0, 3))));
        }
    }

    Y_UNIT_TEST(ShouldRejectSmallWritesAfterReachingFreshByteCountHardLimit)
    {
        auto config = DefaultConfig();
        config.SetFreshBlocksWriterEnabled(true);
        config.SetFreshByteCountHardLimit(8_KB);
        config.SetFlushThreshold(4_MB);

        auto testEnv = PrepareTestActorRuntime(config);
        auto& runtime = *testEnv.Runtime;

        ui64 addFreshBlocksCount = 0;

        // TODO(issue-4875): remove trim events dropping after adding trim
        // synchronization
        runtime.SetEventFilter(
            [&](TTestActorRuntimeBase& runtime, TAutoPtr<IEventHandle>& event)
            {
                Y_UNUSED(runtime);

                if (event->GetTypeRewrite() == TEvPartitionCommonPrivate::EvAddFreshBlocksResponse) {
                    ++addFreshBlocksCount;
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

        fbwClient.WriteBlocks(TBlockRange32::MakeOneBlock(0), 1);
        fbwClient.WriteBlocks(TBlockRange32::MakeOneBlock(0), 1);

        fbwClient.SendWriteBlocksRequest(TBlockRange32::MakeOneBlock(0), 1);
        auto response = fbwClient.RecvWriteBlocksResponse();
        UNIT_ASSERT_VALUES_EQUAL_C(
            E_REJECTED,
            response->GetStatus(),
            response->GetErrorReason());
        UNIT_ASSERT(
            HasProtoFlag(response->GetError().GetFlags(), NProto::EF_SILENT));

        UNIT_ASSERT_VALUES_EQUAL(2, addFreshBlocksCount);

        partition.Flush();

        fbwClient.SendWriteBlocksRequest(TBlockRange32::MakeOneBlock(0), 1);
        response = fbwClient.RecvWriteBlocksResponse();
        UNIT_ASSERT_VALUES_EQUAL_C(
            S_OK,
            response->GetStatus(),
            response->GetErrorReason());
    }

    Y_UNIT_TEST(ShouldNotTrimInProgressWrites)
    {
        auto testEnv = PrepareTestActorRuntime();
        auto& runtime = *testEnv.Runtime;

        TPartitionClient partition(runtime);
        partition.WaitReady();

        TFreshBlocksWriterClient fbwClient(
            runtime,
            testEnv.FreshBlocksWriterActorId);

        fbwClient.WaitReady();

        fbwClient.WriteBlocks(0, '0');

        ui64 addFreshBlocksCommitId = 0;
        std::unique_ptr<IEventHandle> stolenAddFreshBlocksRequest;

        bool seenFlushCompleted = false;
        bool seenTrimCompleted = false;

        runtime.SetObserverFunc(
            [&](TAutoPtr<IEventHandle>& event)
            {
                if (event->GetTypeRewrite() ==
                    TEvPartitionCommonPrivate::EvAddFreshBlocksRequest)
                {
                    auto* ev = event->Get<TEvPartitionCommonPrivate::TEvAddFreshBlocksRequest>();
                    addFreshBlocksCommitId = ev->CommitId;

                    stolenAddFreshBlocksRequest.reset(event.Release());
                    return TTestActorRuntime::EEventAction::DROP;
                }

                if (event->GetTypeRewrite() == TEvBlobStorage::EvCollectGarbage)
                {
                    auto* ev = event->Get<TEvBlobStorage::TEvCollectGarbage>();

                    auto trimFreshLogToCommitId =
                        MakeCommitId(ev->CollectGeneration, ev->CollectStep);
                    UNIT_ASSERT(
                        trimFreshLogToCommitId < addFreshBlocksCommitId);
                }

                seenFlushCompleted |= event->GetTypeRewrite() ==
                                      TEvPartitionPrivate::EvFlushCompleted;

                seenTrimCompleted |=
                    event->GetTypeRewrite() ==
                    TEvPartitionCommonPrivate::EvTrimFreshLogCompleted;

                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        fbwClient.WriteBlocks(1, '1');

        UNIT_ASSERT(stolenAddFreshBlocksRequest);

        {
            partition.SendFlushRequest();

            TDispatchOptions dispatchOptions;
            dispatchOptions.CustomFinalCondition = [&]()
            {
                return seenFlushCompleted;
            };

            runtime.DispatchEvents(dispatchOptions, 10ms);
        }

        {
            partition.SendTrimFreshLogRequest();

            TDispatchOptions dispatchOptions;
            dispatchOptions.CustomFinalCondition = [&]()
            {
                return seenTrimCompleted;
            };

            runtime.DispatchEvents(dispatchOptions, 10ms);
        }

        runtime.Send(stolenAddFreshBlocksRequest.release());

        auto actualContent = GetBlocksContent(
            fbwClient.ReadBlocks(TBlockRange32::WithLength(0, 2)));

        TStringBuilder expectedContent;
        expectedContent << GetBlockContent('0') << GetBlockContent('1');

        UNIT_ASSERT_EQUAL(expectedContent, actualContent);
    }

    Y_UNIT_TEST(ShouldNotLoseInFlightWritesOnReboot)
    {
        auto testEnv = PrepareTestActorRuntime();
        auto& runtime = *testEnv.Runtime;

        TPartitionClient partition(runtime);
        partition.WaitReady();

        TFreshBlocksWriterClient fbwClient(
            runtime,
            testEnv.FreshBlocksWriterActorId);
        fbwClient.WaitReady();

        std::unique_ptr<IEventHandle> stolenPutRequest;
        bool stealPutRequest = true;

        runtime.SetObserverFunc(
            [&](TAutoPtr<IEventHandle>& event)
            {
                if (event->GetTypeRewrite() == TEvBlobStorage::EvPut &&
                    !stolenPutRequest && stealPutRequest)
                {
                    stolenPutRequest.reset(event.Release());
                    return TTestActorRuntime::EEventAction::DROP;
                }

                if (event->GetTypeRewrite() == TEvPartitionCommonPrivate::EvTrimFreshLogRequest)
                {
                    return TTestActorRuntime::EEventAction::DROP;
                }

                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        fbwClient.SendWriteBlocksRequest(0, '1');
        runtime.DispatchEvents({}, 10ms);

        fbwClient.WriteBlocks(1, '2');

        partition.Flush();

        fbwClient.WriteBlocks(2, '3');

        partition.Flush();

        stealPutRequest = false;
        runtime.SendAsync(stolenPutRequest.release());

        auto response = fbwClient.RecvWriteBlocksResponse();
        UNIT_ASSERT_C(
            !HasError(response->GetError()),
            FormatError(response->GetError()));

        partition.KillTablet();
        partition.ReconnectPipe();
        partition.WaitReady();

        TFreshBlocksWriterClient newFbwClient(
            runtime,
            testEnv.FreshBlocksWriterActorId);
        newFbwClient.WaitReady();

        auto actualContent = GetBlocksContent(
            newFbwClient.ReadBlocks(TBlockRange32::WithLength(0, 3)));
        UNIT_ASSERT_VALUES_EQUAL(
            TStringBuilder() << GetBlockContent('1') << GetBlockContent('2')
                             << GetBlockContent('3'),
            actualContent);
    }

    Y_UNIT_TEST(ShouldWaitForAddFreshBlocksBeforeCompaction)
    {
        auto testEnv = PrepareTestActorRuntime();
        auto& runtime = *testEnv.Runtime;

        TPartitionClient partition(runtime);
        partition.WaitReady();

        TFreshBlocksWriterClient fbwClient(
            runtime,
            testEnv.FreshBlocksWriterActorId);

        fbwClient.WaitReady();

        fbwClient.WriteBlocks(0, '0');

        partition.Flush();

        std::unique_ptr<IEventHandle> stolenAddFreshBlocksRequest;

        bool executeTransactionEventObserved = false;

        runtime.SetObserverFunc(
            [&](TAutoPtr<IEventHandle>& event)
            {
                if (event->GetTypeRewrite() ==
                    TEvPartitionCommonPrivate::EvAddFreshBlocksRequest)
                {
                    stolenAddFreshBlocksRequest.reset(event.Release());
                    return TTestActorRuntime::EEventAction::DROP;
                }

                executeTransactionEventObserved |=
                    event->GetTypeRewrite() ==
                    TEvPartitionCommonPrivate::EvExecuteTransactions;

                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        fbwClient.WriteBlocks(1, '1');

        UNIT_ASSERT(stolenAddFreshBlocksRequest);

        UNIT_ASSERT(!executeTransactionEventObserved);

        partition.SendCompactionRequest();

        runtime.DispatchEvents({}, 10ms);

        UNIT_ASSERT(!executeTransactionEventObserved);

        runtime.Send(stolenAddFreshBlocksRequest.release());

        auto resp = partition.RecvCompactionResponse();
        UNIT_ASSERT_C(
            !HasError(resp->GetError()),
            FormatError(resp->GetError()));

        UNIT_ASSERT(executeTransactionEventObserved);

        auto actualContent = GetBlocksContent(
            fbwClient.ReadBlocks(TBlockRange32::WithLength(0, 2)));

        TStringBuilder expectedContent;
        expectedContent << GetBlockContent('0') << GetBlockContent('1');

        UNIT_ASSERT_EQUAL(expectedContent, actualContent);
    }

    Y_UNIT_TEST(ShouldWaitForAddFreshBlocksBeforeCheckpointCreation)
    {
        auto testEnv = PrepareTestActorRuntime();
        auto& runtime = *testEnv.Runtime;

        TPartitionClient partition(runtime);
        partition.WaitReady();

        TFreshBlocksWriterClient fbwClient(
            runtime,
            testEnv.FreshBlocksWriterActorId);

        fbwClient.WaitReady();

        fbwClient.WriteBlocks(0, '0');

        partition.Flush();

        std::unique_ptr<IEventHandle> stolenAddFreshBlocksRequest;

        bool grabAddFreshBlocksRequest = false;
        bool executeTransactionEventObserved = false;

        runtime.SetObserverFunc(
            [&](TAutoPtr<IEventHandle>& event)
            {
                if (event->GetTypeRewrite() ==
                        TEvPartitionCommonPrivate::EvAddFreshBlocksRequest &&
                    grabAddFreshBlocksRequest)
                {
                    stolenAddFreshBlocksRequest.reset(event.Release());
                    return TTestActorRuntime::EEventAction::DROP;
                }

                executeTransactionEventObserved |=
                    event->GetTypeRewrite() ==
                    TEvPartitionCommonPrivate::EvExecuteTransactions;

                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        grabAddFreshBlocksRequest = true;
        fbwClient.WriteBlocks(1, '1');
        grabAddFreshBlocksRequest = false;

        UNIT_ASSERT(stolenAddFreshBlocksRequest);
        UNIT_ASSERT(!executeTransactionEventObserved);

        partition.SendCreateCheckpointRequest("ch1");
        runtime.DispatchEvents({}, 10ms);

        fbwClient.WriteBlocks(2, '2');

        UNIT_ASSERT(!executeTransactionEventObserved);

        // Check that checkpoint is not created yet
        fbwClient.SendReadBlocksRequest(1, "ch1");
        auto response = fbwClient.RecvReadBlocksResponse();
        UNIT_ASSERT_VALUES_EQUAL(E_NOT_FOUND, response->GetError().GetCode());

        runtime.Send(stolenAddFreshBlocksRequest.release());

        auto resp = partition.RecvCreateCheckpointResponse();
        UNIT_ASSERT_C(
            !HasError(resp->GetError()),
            FormatError(resp->GetError()));
        UNIT_ASSERT(executeTransactionEventObserved);

        auto actualContent = GetBlocksContent(
            fbwClient.ReadBlocks(TBlockRange32::WithLength(0, 3), "ch1"));

        TStringBuilder expectedContent;
        expectedContent << GetBlockContent('0') << GetBlockContent('1')
                        << GetBlockContent('\0');

        UNIT_ASSERT_VALUES_EQUAL(expectedContent, actualContent);

        actualContent = GetBlocksContent(
            fbwClient.ReadBlocks(TBlockRange32::WithLength(0, 3)));

        expectedContent.clear();
        expectedContent << GetBlockContent('0') << GetBlockContent('1')
                        << GetBlockContent('2');

        UNIT_ASSERT_VALUES_EQUAL(expectedContent, actualContent);
    }
}

}   // namespace NCloud::NBlockStore::NStorage::NPartition
