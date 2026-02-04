#include "fresh_blocks_companion.h"

// #include <cloud/blockstore/libs/storage/partition/model/fresh_blob.h>
#include <cloud/blockstore/libs/storage/core/transaction_time_tracker.h>

// TODO: invalid reference
#include <cloud/blockstore/libs/storage/service/service_events_private.h>
#include <cloud/blockstore/libs/storage/testlib/test_env.h>
#include <cloud/blockstore/libs/storage/testlib/test_runtime.h>
#include <cloud/blockstore/libs/storage/volume/volume_events_private.h>

#include <cloud/storage/core/libs/common/helpers.h>
#include <cloud/storage/core/libs/common/sglist_test.h>
#include <cloud/storage/core/libs/tablet/blob_id.h>

#include <contrib/ydb/core/base/blobstorage.h>
#include <contrib/ydb/core/blobstorage/vdisk/common/vdisk_events.h>
#include <contrib/ydb/core/testlib/basics/storage.h>

#include <library/cpp/testing/unittest/registar.h>

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

////////////////////////////////////////////////////////////////////////////////

ui32 GetMaxIORequestsInFlight(
    const TStorageConfig& config,
    const NProto::TPartitionConfig& partitionConfig)
{
    switch (partitionConfig.GetStorageMediaKind()) {
        case NCloud::NProto::EStorageMediaKind::STORAGE_MEDIA_SSD:
            return config.GetMaxIORequestsInFlightSSD();
        default:
            return config.GetMaxIORequestsInFlight();
    }
}

// ////////////////////////////////////////////////////////////////////////////////

struct TWaitReadyRequest
{
};

struct TWaitReadyResponse
{
};

enum ETestEvents
{
    EvBegin = EventSpaceBegin(NKikimr::TEvents::ES_USERSPACE + 10),
    EvWaitReadyRequest = EvBegin + 1,
    EvWaitReadyResponse = EvBegin + 2,
};

using TEvWaitReadyRequest =
    TRequestEvent<TWaitReadyRequest, ETestEvents::EvWaitReadyRequest>;
using TEvWaitReadyResponse =
    TResponseEvent<TWaitReadyResponse, ETestEvents::EvWaitReadyResponse>;

////////////////////////////////////////////////////////////////////////////////

struct TState
{
    TPartitionChannelsState ChannelsState;
    TCommitIdsState CommitIdsState;
    TPartitionFreshBlobState FreshBlobState;
    TPartitionFlushState FlushState;
    TPartitionTrimFreshLogState TrimFreshLogState;
    TPartitionFreshBlocksState FreshBlocksState;

    TState(
        TStorageConfigPtr config,
        const NProto::TPartitionConfig& partitionConfig,
        TTabletStorageInfoPtr info,
        ui64 generation)
        : ChannelsState(
              partitionConfig,
              TFreeSpaceConfig{
                  config->GetChannelFreeSpaceThreshold() / 100.,
                  config->GetChannelMinFreeSpace() / 100.,
              },
              GetMaxIORequestsInFlight(*config, partitionConfig),
              config->GetReassignChannelsPercentageThreshold(),
              config->GetReassignFreshChannelsPercentageThreshold(),
              config->GetReassignMixedChannelsPercentageThreshold(),
              config->GetReassignSystemChannelsImmediately(),
              Min(info->Channels.size(),
                  partitionConfig.ExplicitChannelProfilesSize()))
        , CommitIdsState(generation, 0)
        , TrimFreshLogState(CommitIdsState)
        , FreshBlocksState(CommitIdsState, FlushState, TrimFreshLogState)
    {}
};

const TString Transactions[] = {
    "Total",
};

class TActorWithFresh final
    : public NActors::TActor<TActorWithFresh>
    , public TTabletBase<TActorWithFresh>
    , public IFreshBlocksCompanionClient
{
private:
    const TStorageConfigPtr Config;
    const EStorageAccessMode StorageAccessMode;
    NProto::TPartitionConfig PartitionConfig;

    std::unique_ptr<TState> State;

    TLogTitle LogTitle;

    std::unique_ptr<TFreshBlocksCompanion> FreshBlocksCompanion;

    TTransactionTimeTracker TransactionTimeTracker;

    bool Ready = false;
    TVector<TRequestInfoPtr> WaitReadyRequests;

public:
    TActorWithFresh(
        TActorId owner,
        TStorageConfigPtr config,
        EStorageAccessMode storageAccessMode,
        NProto::TPartitionConfig partitionConfig,
        TTabletStorageInfoPtr storage);

    void DefaultSignalTabletActive(const TActorContext& ctx) override;
    void OnActivateExecutor(const TActorContext& ctx) override;
    void OnDetach(const TActorContext& ctx) override;
    void OnTabletDead(
        TEvTablet::TEvTabletDead::TPtr& ev,
        const TActorContext& ctx) override;

    // ISuicideActor overrides

    void Suicide(const TActorContext& ctx) override;

    // IFreshBlocksCompanionClient overrides

    void FreshBlobsLoaded(const NActors::TActorContext& ctx) override;

private:
    STFUNC(StateBoot);
    STFUNC(StateInit);
    STFUNC(StateWork);
    STFUNC(StateZombie);

    void HandleWaitReady(
        const TEvWaitReadyRequest::TPtr& ev,
        const NActors::TActorContext& ctx);
};

///////////////////////////////////////////////////////////////////////////////

TActorWithFresh::TActorWithFresh(
        TActorId owner,
        TStorageConfigPtr config,
        EStorageAccessMode storageAccessMode,
        NProto::TPartitionConfig partitionConfig,
        TTabletStorageInfoPtr storage)
    : TActor(&TThis::StateBoot)
    , TTabletBase(owner, std::move(storage), &TransactionTimeTracker)
    , Config(config)
    , StorageAccessMode(storageAccessMode)
    , PartitionConfig(partitionConfig)
    , LogTitle(
          GetCycleCount(),
          TLogTitle::TPartition{
              .TabletId = TabletID(),
              .DiskId = PartitionConfig.GetDiskId(),
              .PartitionIndex = 0,
              .PartitionCount = 1})
    , TransactionTimeTracker(Transactions)
{}

void TActorWithFresh::DefaultSignalTabletActive(const TActorContext& ctx)
{
    Y_UNUSED(ctx);
}

void TActorWithFresh::OnActivateExecutor(const TActorContext& ctx)
{
    Become(&TThis::StateInit);

    State = std::make_unique<TState>(
        Config,
        PartitionConfig,
        Info(),
        Executor()->Generation());

    FreshBlocksCompanion = std::make_unique<TFreshBlocksCompanion>(
        StorageAccessMode,
        PartitionConfig,
        Info(),
        *this,
        State->ChannelsState,
        State->FreshBlobState,
        State->FlushState,
        State->TrimFreshLogState,
        State->FreshBlocksState,
        LogTitle);

    FreshBlocksCompanion->LoadFreshBlobs(ctx, 0);
}

void TActorWithFresh::OnDetach(const TActorContext& ctx)
{
    Die(ctx);
}

void TActorWithFresh::OnTabletDead(
    TEvTablet::TEvTabletDead::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);
    Die(ctx);
}

void TActorWithFresh::Suicide(const TActorContext& ctx)
{
    NCloud::Send<TEvents::TEvPoisonPill>(ctx, Tablet());
    Become(&TThis::StateZombie);
}

void TActorWithFresh::FreshBlobsLoaded(const NActors::TActorContext& ctx)
{
    Ready = true;
    Become(&TThis::StateWork);

    for (auto& request: WaitReadyRequests) {
        NCloud::Reply(ctx, *request, std::make_unique<TEvWaitReadyResponse>());
    }

    WaitReadyRequests.clear();

    // allow pipes to connect
    SignalTabletActive(ctx);
}

void TActorWithFresh::HandleWaitReady(
    const TEvWaitReadyRequest::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    if (Ready) {
        NCloud::Reply(ctx, *ev, std::make_unique<TEvWaitReadyResponse>());
        return;
    }

    WaitReadyRequests.emplace_back(
        CreateRequestInfo(ev->Sender, ev->Cookie, ev->Get()->CallContext));
    Cerr << "wr req count: " << WaitReadyRequests.size() << Endl;
}

STFUNC(TActorWithFresh::StateBoot)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvWaitReadyRequest, HandleWaitReady);

        IgnoreFunc(TEvTabletPipe::TEvServerConnected);
        IgnoreFunc(TEvTabletPipe::TEvServerDisconnected);

        default:
            StateInitImpl(ev, SelfId());
            break;
    }
}

STFUNC(TActorWithFresh::StateInit)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvWaitReadyRequest, HandleWaitReady);

        IgnoreFunc(TEvTabletPipe::TEvServerConnected);
        IgnoreFunc(TEvTabletPipe::TEvServerDisconnected);

        HFunc(
            TEvPartitionCommonPrivate::TEvLoadFreshBlobsCompleted,
            FreshBlocksCompanion->HandleLoadFreshBlobsCompleted);

        default:
            if (!HandleDefaultEvents(ev, SelfId())) {
                Y_ABORT();
            }
            break;
    }
}

STFUNC(TActorWithFresh::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvWaitReadyRequest, HandleWaitReady);

        IgnoreFunc(TEvTabletPipe::TEvServerConnected);
        IgnoreFunc(TEvTabletPipe::TEvServerDisconnected);

        default:
            if (!HandleDefaultEvents(ev, SelfId())) {
                Y_ABORT();
            }
            break;
    }
}

STFUNC(TActorWithFresh::StateZombie)
{
    switch (ev->GetTypeRewrite()) {
        IgnoreFunc(TEvWaitReadyRequest);

        HFunc(TEvTablet::TEvTabletDead, HandleTabletDead);

        default:
            if (!HandleDefaultEvents(ev, SelfId())) {
                Y_ABORT();
            }
            break;
    }
}

////////////////////////////////////////////////////////////////////////////////

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
            NCloud::NProto::TFeaturesConfig()));

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

    auto diagConfig = CreateTestDiagnosticsConfig();

    auto createFunc = [=](const TActorId& owner, TTabletStorageInfo* info)
    {
        auto tablet = std::make_unique<TActorWithFresh>(
            owner,
            storageConfig,
            storageAccessMode,
            partConfig,
            info);
        return tablet.release();
    };

    auto bootstrapper =
        CreateTestBootstrapper(runtime, tabletInfo.release(), createFunc);
    runtime.EnableScheduleForActor(bootstrapper);
}

////////////////////////////////////////////////////////////////////////////////

void InitLogSettings(TTestActorRuntime& runtime)
{
    for (ui32 i = TBlockStoreComponents::START; i < TBlockStoreComponents::END;
         ++i)
    {
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

    InitTestActorRuntime(
        *runtime,
        config,
        blockCount,
        channelsCount ? *channelsCount : tabletInfo->Channels.size(),
        std::move(tabletInfo),
        testPartitionInfo,
        storageAccessMode);

    return runtime;
}

////////////////////////////////////////////////////////////////////////////////

class TTestTabletClient
{
private:
    TTestActorRuntime& Runtime;
    ui32 NodeIdx = 0;
    ui64 TabletId;

    const TActorId Sender;
    TActorId PipeClient;

public:
    TTestTabletClient(
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
        TVector<ui64> tablets = {TabletId};
        auto guard = CreateTabletScheduledEventsGuard(tablets, Runtime, Sender);

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
        Runtime
            .SendToPipe(PipeClient, Sender, request.release(), NodeIdx, cookie);
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

    void WaitReady()
    {
        auto request = std::make_unique<TEvWaitReadyRequest>();
        SendToPipe(std::move(request));
        RecvResponse<TEvWaitReadyResponse>();
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TFreshBlocksCompanionTest)
{
    Y_UNIT_TEST(ShouldHandleBSErrorsOnInitFreshBlocksFromChannel)
    {
        auto config = DefaultConfig();
        config.SetFreshChannelWriteRequestsEnabled(true);

        auto runtime = PrepareTestActorRuntime(config);

        auto client = std::make_unique<TTestTabletClient>(*runtime);
        client->WaitReady();

        bool evRangeResultSeen = false;

        ui32 evLoadFreshBlobsCompletedCount = 0;

        runtime->SetEventFilter(
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
                            TLogoBlobID(),   // doesn't matter
                            TLogoBlobID(),   // doesn't matter
                            0);              // doesn't matter

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
            });

        client->RebootTablet();
        client->WaitReady();

        UNIT_ASSERT_VALUES_EQUAL(true, evRangeResultSeen);

        // tablet rebooted twice (after explicit RebootTablet() and on fail)
        UNIT_ASSERT_VALUES_EQUAL(2, evLoadFreshBlobsCompletedCount);
    }
}

}   // namespace NCloud::NBlockStore::NStorage::NPartition
