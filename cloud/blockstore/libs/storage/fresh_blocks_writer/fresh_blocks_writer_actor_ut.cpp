#include <cloud/blockstore/libs/storage/partition/part_actor.h>
#include <cloud/blockstore/libs/diagnostics/block_digest.h>
#include <cloud/blockstore/libs/storage/api/fresh_blocks_writer.h>
#include <cloud/blockstore/libs/storage/fresh_blocks_writer/fresh_blocks_writer_actor.h>
#include <cloud/blockstore/libs/storage/partition/part.h>
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

#undef BLOCKSTORE_DECLARE_METHOD
};

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

        auto req = std::make_unique<TEvVolume::TEvGetPartitionInfoRequest>();
        fbwClient.Send(std::move(req));
        auto response =
            fbwClient.RecvResponse<TEvVolume::TEvGetPartitionInfoResponse>();
        UNIT_ASSERT_C(
            SUCCEEDED(response->GetStatus()),
            response->GetErrorReason());
    }
}

}   // namespace NCloud::NBlockStore::NStorage::NPartition
