#include "part_actor.h"

#include <cloud/blockstore/libs/diagnostics/block_digest.h>
#include <cloud/blockstore/libs/diagnostics/critical_events.h>
#include <cloud/blockstore/libs/storage/api/volume_proxy.h>

#include <cloud/storage/core/libs/api/hive_proxy.h>
#include <cloud/storage/core/libs/common/verify.h>

#include <contrib/ydb/core/base/tablet_pipe.h>
#include <contrib/ydb/core/mind/local.h>
#include <contrib/ydb/core/node_whiteboard/node_whiteboard.h>
#include <contrib/ydb/core/tablet/tablet_counters_aggregator.h>

#include <library/cpp/blockcodecs/codecs.h>

namespace NCloud::NBlockStore::NStorage::NPartition {

using namespace NActors;

using namespace NKikimr;
using namespace NKikimr::NNodeWhiteboard;
using namespace NKikimr::NTabletFlatExecutor;

using namespace NCloud::NStorage;

////////////////////////////////////////////////////////////////////////////////

static constexpr TDuration YellowStateUpdateInterval = TDuration::Seconds(15);
static constexpr TDuration BackpressureReportSendInterval = TDuration::Seconds(5);

////////////////////////////////////////////////////////////////////////////////

const TPartitionActor::TStateInfo TPartitionActor::States[STATE_MAX] = {
    { "Boot",   (IActor::TReceiveFunc)&TPartitionActor::StateBoot   },
    { "Init",   (IActor::TReceiveFunc)&TPartitionActor::StateInit   },
    { "Work",   (IActor::TReceiveFunc)&TPartitionActor::StateWork   },
    { "Zombie", (IActor::TReceiveFunc)&TPartitionActor::StateZombie },
};

TPartitionActor::TPartitionActor(
        const TActorId& owner,
        TTabletStorageInfoPtr storage,
        TStorageConfigPtr config,
        TDiagnosticsConfigPtr diagnosticsConfig,
        IProfileLogPtr profileLog,
        IBlockDigestGeneratorPtr blockDigestGenerator,
        NProto::TPartitionConfig partitionConfig,
        EStorageAccessMode storageAccessMode,
        ui32 siblingCount,
        const TActorId& volumeActorId)
    : TActor(&TThis::StateBoot)
    , TTabletBase(owner, std::move(storage))
    , Config(std::move(config))
    , PartitionConfig(std::move(partitionConfig))
    , DiagnosticsConfig(std::move(diagnosticsConfig))
    , ProfileLog(std::move(profileLog))
    , BlockDigestGenerator(std::move(blockDigestGenerator))
    , StorageAccessMode(storageAccessMode)
    , SiblingCount(siblingCount)
    , VolumeActorId(volumeActorId)
    , ChannelHistorySize(CalcChannelHistorySize())
    , BlobCodec(NBlockCodecs::Codec(Config->GetBlobCompressionCodec()))
{}

TPartitionActor::~TPartitionActor()
{
    ReleaseTransactions();
}

TString TPartitionActor::GetStateName(ui32 state)
{
    if (state < STATE_MAX) {
        return States[state].Name;
    }
    return "<unknown>";
}

void TPartitionActor::Enqueue(STFUNC_SIG)
{
    ALOG_ERROR(TBlockStoreComponents::PARTITION,
        "[" << TabletID() << "]"
        << " IGNORING message type# " << ev->GetTypeRewrite()
        << " from Sender# " << ToString(ev->Sender)
        << " in StateBoot");
}

////////////////////////////////////////////////////////////////////////////////

void TPartitionActor::DefaultSignalTabletActive(const TActorContext& ctx)
{
    Y_UNUSED(ctx); // postpone until LoadState transaction completes
}

void TPartitionActor::Activate(const TActorContext& ctx)
{
    BecomeAux(ctx, STATE_WORK);

    // allow pipes to connect
    SignalTabletActive(ctx);

    // resend pending requests
    SendPendingRequests(ctx, PendingRequests);

    // TODO: it is too expensive for huge volumes
    //State->CollectGarbageHardRequested = true;
    EnqueueCollectGarbageIfNeeded(ctx);

    EnqueueFlushIfNeeded(ctx);
    EnqueueCompactionIfNeeded(ctx);
    EnqueueCleanupIfNeeded(ctx);
    EnqueueAddConfirmedBlobsIfNeeded(ctx);

    State->FinishLoadState();

    LOG_INFO(ctx, TBlockStoreComponents::PARTITION,
        "[%lu] State initialization finished",
        TabletID());
}

void TPartitionActor::Suicide(const TActorContext& ctx)
{
    NCloud::Send<TEvents::TEvPoisonPill>(ctx, Tablet());
    BecomeAux(ctx, STATE_ZOMBIE);
}

void TPartitionActor::BecomeAux(const TActorContext& ctx, EState state)
{
    Y_ABORT_UNLESS(state < STATE_MAX);

    Become(States[state].Func);
    CurrentState = state;

    LOG_DEBUG(ctx, TBlockStoreComponents::PARTITION,
        "[%lu] Switched to state %s (system: %s, user: %s, executor: %s)",
        TabletID(),
        States[state].Name.data(),
        ToString(Tablet()).data(),
        ToString(SelfId()).data(),
        ToString(ExecutorID()).data());

    ReportTabletState(ctx);
}

void TPartitionActor::ReportTabletState(const TActorContext& ctx)
{
    auto service = MakeNodeWhiteboardServiceId(SelfId().NodeId());

    auto request = std::make_unique<TEvWhiteboard::TEvTabletStateUpdate>(
        TabletID(),
        CurrentState);

    NCloud::Send(ctx, service, std::move(request));
}

////////////////////////////////////////////////////////////////////////////////

void TPartitionActor::RegisterCounters(const TActorContext& ctx)
{
    if (!Counters) {
        auto counters = CreatePartitionCounters();

        // LAME: ownership transferred to executor
        Counters = counters.get();
        Executor()->RegisterExternalTabletCounters(counters.release());

        // only aggregated statistics will be reported by default
        // (you can always turn on per-tablet statistics on monitoring page)
        // TabletCountersAddTablet(TabletID(), ctx);

        ScheduleCountersUpdate(ctx);
    }

    if (!PartCounters) {
        PartCounters = CreatePartitionDiskCounters(
            EPublishingPolicy::Repl,
            DiagnosticsConfig->GetHistogramCounterOptions());
    }
}

void TPartitionActor::ScheduleCountersUpdate(const TActorContext& ctx)
{
    if (!UpdateCountersScheduled) {
        ctx.Schedule(UpdateCountersInterval,
            new TEvPartitionPrivate::TEvUpdateCounters());
        UpdateCountersScheduled = true;
    }
}

void TPartitionActor::UpdateCounters(const TActorContext& ctx)
{
    if (!Counters || !PartCounters || !State) {
        return;
    }

    const auto& stats = State->GetStats();

#define BLOCKSTORE_PARTITION_UPDATE_COUNTER(name, category, ...)               \
    {                                                                          \
        auto& counter = Counters->Cumulative()                                 \
            [TPartitionCounters::CUMULATIVE_COUNTER_##category##_##name];      \
        ui64 value = stats.Get##category##Counters().Get##name();              \
        Y_DEBUG_ABORT_UNLESS(value >= counter.Get());                                \
        if (value < counter.Get()) {                                           \
            ReportCounterUpdateRace();                                         \
            LOG_ERROR(ctx, TBlockStoreComponents::PARTITION,                   \
                "[%lu] VERIFY violation %lu < %lu for counter %s::%s",         \
                TabletID(),                                                    \
                value,                                                         \
                counter.Get(),                                                 \
                #category,                                                     \
                #name);                                                        \
        }                                                                      \
        counter.Increment(value - counter.Get());                              \
    }                                                                          \
// BLOCKSTORE_PARTITION_UPDATE_COUNTER

    BLOCKSTORE_PARTITION_IO_COUNTERS(BLOCKSTORE_PARTITION_UPDATE_COUNTER)

#undef BLOCKSTORE_PARTITION_UPDATE_COUNTER

    SendStatsToService(ctx);
}

////////////////////////////////////////////////////////////////////////////////

void TPartitionActor::ScheduleYellowStateUpdate(const TActorContext& ctx)
{
    if (!UpdateYellowStateScheduled) {
        ctx.Schedule(YellowStateUpdateInterval,
            new TEvPartitionPrivate::TEvUpdateYellowState());
        UpdateYellowStateScheduled = true;
    }
}

void TPartitionActor::UpdateYellowState(const TActorContext& ctx)
{
    ctx.Register(CreateTabletDSChecker(SelfId(), Info()));
}

void TPartitionActor::ReassignChannelsIfNeeded(const NActors::TActorContext& ctx)
{
    const auto timeout = Config->GetReassignRequestRetryTimeout();

    if (ReassignRequestSentTs.GetValue()
            && ReassignRequestSentTs + timeout >= ctx.Now())
    {
        return;
    }

    auto channels = State->GetChannelsToReassign();

    if (channels.empty()) {
        return;
    }

    if (ReassignRequestSentTs.GetValue()) {
        LOG_WARN(ctx, TBlockStoreComponents::PARTITION,
            "[%lu][d:%s] Retrying reassign request (timeout: %lu milliseconds)",
            TabletID(),
            PartitionConfig.GetDiskId().c_str(),
            timeout.MilliSeconds());
    }

    {
        TStringBuilder sb;
        for (const auto channel: channels) {
            if (sb.size()) {
                sb << ", ";
            }

            sb << channel;
        }

        LOG_WARN(ctx, TBlockStoreComponents::PARTITION,
            "[%lu][d:%s] Reassign request sent for channels: %s",
            TabletID(),
            PartitionConfig.GetDiskId().c_str(),
            sb.c_str());
    }

    NCloud::Send<TEvHiveProxy::TEvReassignTabletRequest>(
        ctx,
        MakeHiveProxyServiceId(),
        0,  // cookie
        TabletID(),
        std::move(channels));

    ReportReassignTablet();
    ReassignRequestSentTs = ctx.Now();
}

////////////////////////////////////////////////////////////////////////////////

bool TPartitionActor::OnRenderAppHtmlPage(
    NMon::TEvRemoteHttpInfo::TPtr ev,
    const TActorContext& ctx)
{
    if (!Executor() || !Executor()->GetStats().IsActive) {
        return false;
    }

    if (ev) {
        HandleHttpInfo(ev, ctx);
    }
    return true;
}

void TPartitionActor::OnActivateExecutor(const TActorContext& ctx)
{
    LOG_INFO(ctx, TBlockStoreComponents::PARTITION,
        "[%lu] Activated executor",
        TabletID());

    BecomeAux(ctx, STATE_INIT);

    RegisterCounters(ctx);

    ctx.Schedule(BackpressureReportSendInterval,
        new TEvPartitionPrivate::TEvSendBackpressureReport());

    if (!Executor()->GetStats().IsFollower) {
        ExecuteTx<TInitSchema>(ctx, PartitionConfig.GetBlocksCount());
    }
}

void TPartitionActor::OnDetach(const TActorContext& ctx)
{
    Counters = nullptr;

    BeforeDie(ctx);
    Die(ctx);
}

void TPartitionActor::OnTabletDead(
    TEvTablet::TEvTabletDead::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    BeforeDie(ctx);
    Die(ctx);
}

void TPartitionActor::BeforeDie(const TActorContext& ctx)
{
    ClearBaseDiskIdToTabletIdMapping(ctx);
    TerminateTransactions(ctx);
    KillActors(ctx);
    ClearWriteQueue(ctx);
    CancelPendingRequests(ctx, PendingRequests);
}

void TPartitionActor::KillActors(const TActorContext& ctx)
{
    for (const auto& actor: Actors.GetActors()) {
        NCloud::Send<TEvents::TEvPoisonPill>(ctx, actor);
    }
}

void TPartitionActor::AddTransaction(
    TRequestInfo& transaction,
    TRequestInfo::TCancelRoutine cancelRoutine)
{
    transaction.CancelRoutine = cancelRoutine;

    transaction.Ref();

    STORAGE_VERIFY(
        transaction.Empty(),
        TWellKnownEntityTypes::TABLET,
        TabletID());

    ActiveTransactions.PushBack(&transaction);
}

void TPartitionActor::RemoveTransaction(TRequestInfo& requestInfo)
{
    STORAGE_VERIFY(
        !requestInfo.Empty(),
        TWellKnownEntityTypes::TABLET,
        TabletID());

    requestInfo.Unlink();

    STORAGE_VERIFY(
        requestInfo.RefCount() > 1,
        TWellKnownEntityTypes::TABLET,
        TabletID());
    requestInfo.UnRef();
}

void TPartitionActor::TerminateTransactions(const TActorContext& ctx)
{
    while (ActiveTransactions) {
        TRequestInfo* requestInfo = ActiveTransactions.PopFront();
        STORAGE_VERIFY(
            requestInfo->RefCount() >= 1,
            TWellKnownEntityTypes::TABLET,
            TabletID());
        requestInfo->CancelRequest(ctx);
        requestInfo->UnRef();
    }
}

void TPartitionActor::ReleaseTransactions()
{
    while (ActiveTransactions) {
        TRequestInfo* requestInfo = ActiveTransactions.PopFront();
        STORAGE_VERIFY(
            requestInfo->RefCount() >= 1,
            TWellKnownEntityTypes::TABLET,
            TabletID());
        requestInfo->UnRef();
    }
}

void TPartitionActor::ProcessIOQueue(const TActorContext& ctx, ui32 channel)
{
    while (auto requestActor = State->DequeueIORequest(channel)) {
        auto actorId = NCloud::Register(ctx, std::move(requestActor));
        LOG_DEBUG(ctx, TBlockStoreComponents::PARTITION,
            "[%lu][d:%s] Partition registered request actor with id [%lu]",
            TabletID(),
            PartitionConfig.GetDiskId().c_str(),
            actorId);
        Actors.Insert(actorId);
    }
}

////////////////////////////////////////////////////////////////////////////////

ui64 TPartitionActor::CalcChannelHistorySize() const
{
    ui64 sum = 0;
    for (const auto& ch: Info()->Channels) {
        sum += ch.History.size();
    }
    return sum;
}

NKikimr::NMetrics::TResourceMetrics* TPartitionActor::GetResourceMetrics()
{
    return Executor()->GetResourceMetrics();
}

void TPartitionActor::UpdateWriteThroughput(
    const TInstant& now,
    const NKikimr::NMetrics::TChannel& channel,
    const NKikimr::NMetrics::TGroupId& group,
    ui64 value)
{
    GetResourceMetrics()->WriteThroughput[std::make_pair(channel, group)].Increment(value, now);
    GetResourceMetrics()->WriteIops[std::make_pair(channel, group)].Increment(1, now);
}

void TPartitionActor::UpdateReadThroughput(
    const TInstant& now,
    const NKikimr::NMetrics::TChannel& channel,
    const NKikimr::NMetrics::TGroupId& group,
    ui64 value,
    bool isOverlayDisk)
{
    if (isOverlayDisk) {
        const TString& overlayKind = Config->GetCommonOverlayPrefixPoolKind();
        auto& tabletOps = OverlayMetrics.PoolKind2TabletOps
            [overlayKind][std::make_pair(channel, group)];
        tabletOps.ReadOperations.ByteCount += value;
        tabletOps.ReadOperations.Iops += 1;
    } else {
        const auto metricsKey = std::make_pair(channel, group);

        GetResourceMetrics()->ReadThroughput[metricsKey].Increment(value, now);
        GetResourceMetrics()->ReadIops[metricsKey].Increment(1, now);
    }
}

void TPartitionActor::UpdateNetworkStat(
    const TInstant& now,
    ui64 value)
{
    GetResourceMetrics()->Network.Increment(value, now);
}

void TPartitionActor::UpdateStorageStat(i64 value)
{
    GetResourceMetrics()->StorageUser.Increment(value);
}

void TPartitionActor::UpdateCPUUsageStat(TInstant now, ui64 execCylces)
{
    const auto duration = CyclesToDurationSafe(execCylces);
    UserCPUConsumption += duration.MicroSeconds();
    GetResourceMetrics()->CPU.Increment(duration.MicroSeconds(), now);
}

bool TPartitionActor::InitReadWriteBlockRange(
    ui64 blockIndex,
    ui32 blockCount,
    TBlockRange64* range) const
{
    if (!blockCount) {
        return false;
    }

    *range = TBlockRange64::WithLength(blockIndex, blockCount);

    return
        State->CheckBlockRange(*range) &&
        (range->Size() <= Config->GetMaxReadWriteRangeSize() / State->GetBlockSize());
}

bool TPartitionActor::InitChangedBlocksRange(
    ui64 blockIndex,
    ui32 blockCount,
    TBlockRange64* range) const
{
    if (!blockCount) {
        return false;
    }

    *range = TBlockRange64::WithLength(blockIndex, blockCount);

    return
        State->CheckBlockRange(*range) &&
        (range->Size() <= Config->GetMaxChangedBlocksRangeBlocksCount());
}

void TPartitionActor::UpdateChannelPermissions(
    const TActorContext& ctx,
    ui32 channel,
    EChannelPermissions permissions)
{
    if (State->UpdatePermissions(channel, permissions)) {
        SendBackpressureReport(ctx);
    }
}

void TPartitionActor::SendBackpressureReport(const TActorContext& ctx) const
{
    NCloud::Send(
        ctx,
        LauncherID(),
        std::make_unique<TEvPartition::TEvBackpressureReport>(
            MakeIntrusive<TCallContext>(),
            State->CalculateCurrentBackpressure()
        ));
}

void TPartitionActor::SendGarbageCollectorCompleted(
    const TActorContext& ctx) const
{
    LOG_INFO(ctx, TBlockStoreComponents::PARTITION,
        "[%lu][d:%s] Send garbage collector completed",
        TabletID(),
        PartitionConfig.GetDiskId().c_str());
    NCloud::Send(
        ctx,
        LauncherID(),
        std::make_unique<TEvPartition::TEvGarbageCollectorCompleted>(TabletID()));
}

////////////////////////////////////////////////////////////////////////////////

void TPartitionActor::HandlePoisonPill(
    const TEvents::TEvPoisonPill::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    LOG_INFO(ctx, TBlockStoreComponents::PARTITION,
        "[%lu][d:%s] Stop tablet because of PoisonPill request",
        TabletID(),
        PartitionConfig.GetDiskId().c_str());

    Suicide(ctx);
}

void TPartitionActor::HandleUpdateCounters(
    const TEvPartitionPrivate::TEvUpdateCounters::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    UpdateCountersScheduled = false;

    UpdateCounters(ctx);
    ScheduleCountersUpdate(ctx);
}

void TPartitionActor::HandleUpdateYellowState(
    const TEvPartitionPrivate::TEvUpdateYellowState::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    UpdateYellowStateScheduled = false;

    UpdateYellowState(ctx);
}

void TPartitionActor::HandleSendBackpressureReport(
    const TEvPartitionPrivate::TEvSendBackpressureReport::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    if (State) {
        SendBackpressureReport(ctx);
    }

    ctx.Schedule(BackpressureReportSendInterval,
        new TEvPartitionPrivate::TEvSendBackpressureReport());
}

void TPartitionActor::RebootPartitionOnCommitIdOverflow(
    const TActorContext& ctx,
    const TStringBuf& requestName)
{
    LOG_ERROR_S(ctx, TBlockStoreComponents::PARTITION,
        "[" << TabletID() << "]"
        << " CommitId overflow in " << requestName << ". Restarting partition");
    ReportTabletCommitIdOverflow();
    Suicide(ctx);
}

void TPartitionActor::RebootPartitionOnCollectCounterOverflow(
    const TActorContext& ctx,
    const TStringBuf& requestName)
{
    LOG_ERROR_S(ctx, TBlockStoreComponents::PARTITION,
        "[" << TabletID() << "]"
        << " CollectCounter overflow in " << requestName << ". Restarting partition");
    ReportTabletCollectCounterOverflow();
    Suicide(ctx);
}

void TPartitionActor::HandleCheckBlobstorageStatusResult(
    const TEvTablet::TEvCheckBlobstorageStatusResult::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    bool anyChannelYellow = false;
    for (const auto& channel: Info()->Channels) {
        bool diskSpaceYellowMove = false;
        bool diskSpaceYellowStop = false;
        if (auto latestEntry = channel.LatestEntry()) {
            diskSpaceYellowMove = IsIn(msg->LightYellowMoveGroups, latestEntry->GroupID);
            diskSpaceYellowStop = IsIn(msg->YellowStopGroups, latestEntry->GroupID);
        }

        EChannelPermissions permissions = EChannelPermission::SystemWritesAllowed;
        if (!diskSpaceYellowStop) {
            permissions |= EChannelPermission::UserWritesAllowed;
        }
        UpdateChannelPermissions(ctx, channel.Channel, permissions);

        anyChannelYellow |= diskSpaceYellowMove;
    }

    if (anyChannelYellow) {
        ScheduleYellowStateUpdate(ctx);
        ReassignChannelsIfNeeded(ctx);
    }
}

void TPartitionActor::MapBaseDiskIdToTabletId(
    const NActors::TActorContext& ctx)
{
    if (State && State->GetBaseDiskTabletId() != 0) {
        auto request = std::make_unique<TEvVolume::TEvMapBaseDiskIdToTabletId>(
            State->GetBaseDiskId(),
            State->GetBaseDiskTabletId());

        auto event = std::make_unique<IEventHandle>(
            MakeVolumeProxyServiceId(),
            SelfId(),
            request.release());

        LOG_INFO_S(ctx, TBlockStoreComponents::PARTITION,
            "[" << TabletID() << "]"
            << " Sending MapBaseDiskIdToTabletId to VolumeProxy: BaseDiskId="
            << State->GetBaseDiskId().Quote()
            << " BaseTabletId="
            << State->GetBaseDiskTabletId());

        ctx.Send(event.release());
    }
}

void TPartitionActor::ClearBaseDiskIdToTabletIdMapping(
    const NActors::TActorContext& ctx)
{
    if (State && State->GetBaseDiskTabletId() != 0) {
        auto request = std::make_unique<
            TEvVolume::TEvClearBaseDiskIdToTabletIdMapping>(
                State->GetBaseDiskId());

        auto event = std::make_unique<IEventHandle>(
            MakeVolumeProxyServiceId(),
            SelfId(),
            request.release());

        LOG_INFO_S(ctx, TBlockStoreComponents::PARTITION,
            "[" << TabletID() << "]"
            << " Sending ClearBaseDiskIdToTabletIdMapping to VolumeProxy: BaseDiskId="
            << State->GetBaseDiskId().Quote());

        ctx.Send(event.release());
    }
}


void TPartitionActor::HandleReassignTabletResponse(
    const TEvHiveProxy::TEvReassignTabletResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    if (FAILED(msg->GetStatus())) {
        LOG_WARN_S(ctx, TBlockStoreComponents::PARTITION,
            "[" << TabletID() << "]"
            << " Reassign request failed");

        ReassignRequestSentTs = TInstant::Zero();
    }
}

void TPartitionActor::HandleDrain(
    const TEvPartition::TEvDrainRequest::TPtr& ev,
    const TActorContext& ctx)
{
    DrainActorCompanion.HandleDrain(ev, ctx);
}

void TPartitionActor::HandleWaitForInFlightWrites(
    const TEvPartition::TEvWaitForInFlightWritesRequest::TPtr& ev,
    const TActorContext& ctx)
{
    DrainActorCompanion.HandleWaitForInFlightWrites(ev, ctx);
}

void TPartitionActor::HandleLockAndDrainRange(
    const TEvPartition::TEvLockAndDrainRangeRequest::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    Y_UNUSED(ev);
    Y_UNUSED(ctx);
    Y_DEBUG_ABORT_UNLESS(0);
}

bool TPartitionActor::HandleRequests(STFUNC_SIG)
{
    switch (ev->GetTypeRewrite()) {
        BLOCKSTORE_PARTITION_REQUESTS(BLOCKSTORE_HANDLE_REQUEST, TEvPartition)
        BLOCKSTORE_PARTITION_REQUESTS_PRIVATE(
            BLOCKSTORE_HANDLE_REQUEST,
            TEvPartitionPrivate)
        BLOCKSTORE_PARTITION_COMMON_REQUESTS_PRIVATE(
            BLOCKSTORE_HANDLE_REQUEST,
            TEvPartitionCommonPrivate)
        BLOCKSTORE_PARTITION_REQUESTS_FWD_SERVICE(
            BLOCKSTORE_HANDLE_REQUEST,
            TEvService)
        BLOCKSTORE_PARTITION_REQUESTS_FWD_VOLUME(
            BLOCKSTORE_HANDLE_REQUEST,
            TEvVolume)

        default:
            return false;
    }

    return true;
}

bool TPartitionActor::RejectRequests(STFUNC_SIG)
{
    switch (ev->GetTypeRewrite()) {
        BLOCKSTORE_PARTITION_REQUESTS(BLOCKSTORE_REJECT_REQUEST, TEvPartition)
        BLOCKSTORE_PARTITION_REQUESTS_PRIVATE(
            BLOCKSTORE_REJECT_REQUEST,
            TEvPartitionPrivate)
        BLOCKSTORE_PARTITION_COMMON_REQUESTS_PRIVATE(
            BLOCKSTORE_REJECT_REQUEST,
            TEvPartitionCommonPrivate)
        BLOCKSTORE_PARTITION_REQUESTS_FWD_SERVICE(
            BLOCKSTORE_REJECT_REQUEST,
            TEvService)
        BLOCKSTORE_PARTITION_REQUESTS_FWD_VOLUME(
            BLOCKSTORE_REJECT_REQUEST,
            TEvVolume)

        default:
            return false;
    }

    return true;
}

void TPartitionActor::SetFirstGarbageCollectionCompleted()
{
    FirstGarbageCollectionCompleted = true;
}

bool TPartitionActor::IsFirstGarbageCollectionCompleted() const
{
    return FirstGarbageCollectionCompleted;
}

TDuration TPartitionActor::GetBlobStorageAsyncRequestTimeout() const
{
    return PartitionConfig.GetStorageMediaKind() == NProto::STORAGE_MEDIA_SSD
               ? Config->GetBlobStorageAsyncRequestTimeoutSSD()
               : Config->GetBlobStorageAsyncRequestTimeoutHDD();
}

////////////////////////////////////////////////////////////////////////////////

STFUNC(TPartitionActor::StateBoot)
{
    UpdateActorStatsSampled(ActorContext());
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);

        IgnoreFunc(TEvTabletPipe::TEvServerConnected);
        IgnoreFunc(TEvTabletPipe::TEvServerDisconnected);

        HFunc(TEvPartitionPrivate::TEvUpdateCounters, HandleUpdateCounters);
        HFunc(TEvPartitionPrivate::TEvSendBackpressureReport, HandleSendBackpressureReport);

        BLOCKSTORE_HANDLE_REQUEST(WaitReady, TEvPartition)

        IgnoreFunc(TEvHiveProxy::TEvReassignTabletResponse);

        default:
            if (!RejectRequests(ev)) {
                StateInitImpl(ev, SelfId());
            }
            break;
    }
}

STFUNC(TPartitionActor::StateInit)
{
    UpdateActorStatsSampled(ActorContext());
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);

        IgnoreFunc(TEvTabletPipe::TEvServerConnected);
        IgnoreFunc(TEvTabletPipe::TEvServerDisconnected);

        HFunc(TEvPartitionPrivate::TEvUpdateCounters, HandleUpdateCounters);
        HFunc(TEvPartitionPrivate::TEvSendBackpressureReport, HandleSendBackpressureReport);
        HFunc(TEvPartitionCommonPrivate::TEvLoadFreshBlobsCompleted, HandleLoadFreshBlobsCompleted);
        HFunc(TEvPartitionPrivate::TEvConfirmBlobsCompleted, HandleConfirmBlobsCompleted);

        HFunc(TEvVolume::TEvGetUsedBlocksResponse, HandleGetUsedBlocksResponse);

        BLOCKSTORE_HANDLE_REQUEST(WaitReady, TEvPartition)

        IgnoreFunc(TEvHiveProxy::TEvReassignTabletResponse);

        default:
            if (!RejectRequests(ev) &&
                !HandleDefaultEvents(ev, SelfId()))
            {
                HandleUnexpectedEvent(
                    ev,
                    TBlockStoreComponents::PARTITION,
                    __PRETTY_FUNCTION__);
            }
            break;
    }
}

STFUNC(TPartitionActor::StateWork)
{
    UpdateActorStatsSampled(ActorContext());
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);

        HFunc(TEvTablet::TEvCheckBlobstorageStatusResult, HandleCheckBlobstorageStatusResult);

        IgnoreFunc(TEvTabletPipe::TEvServerConnected);
        IgnoreFunc(TEvTabletPipe::TEvServerDisconnected);

        HFunc(TEvPartitionPrivate::TEvUpdateCounters, HandleUpdateCounters);
        HFunc(TEvPartitionPrivate::TEvUpdateYellowState, HandleUpdateYellowState);
        HFunc(TEvPartitionPrivate::TEvSendBackpressureReport, HandleSendBackpressureReport);
        HFunc(TEvPartitionPrivate::TEvProcessWriteQueue, HandleProcessWriteQueue);

        HFunc(TEvPartitionCommonPrivate::TEvReadBlobCompleted, HandleReadBlobCompleted);
        HFunc(TEvPartitionCommonPrivate::TEvLongRunningOperation, HandleLongRunningBlobOperation);
        HFunc(TEvPartitionPrivate::TEvWriteBlobCompleted, HandleWriteBlobCompleted);
        HFunc(TEvPartitionPrivate::TEvPatchBlobCompleted, HandlePatchBlobCompleted);
        HFunc(TEvPartitionPrivate::TEvReadBlocksCompleted, HandleReadBlocksCompleted);
        HFunc(TEvPartitionPrivate::TEvWriteBlocksCompleted, HandleWriteBlocksCompleted);
        HFunc(TEvPartitionPrivate::TEvZeroBlocksCompleted, HandleZeroBlocksCompleted);
        HFunc(TEvPartitionPrivate::TEvFlushCompleted, HandleFlushCompleted);
        HFunc(TEvPartitionCommonPrivate::TEvTrimFreshLogCompleted, HandleTrimFreshLogCompleted);
        HFunc(TEvPartitionPrivate::TEvCompactionCompleted, HandleCompactionCompleted);
        HFunc(TEvPartitionPrivate::TEvMetadataRebuildCompleted, HandleMetadataRebuildCompleted);
        HFunc(TEvPartitionPrivate::TEvScanDiskCompleted, HandleScanDiskCompleted);
        HFunc(TEvPartitionPrivate::TEvCollectGarbageCompleted, HandleCollectGarbageCompleted);
        HFunc(TEvPartitionPrivate::TEvForcedCompactionCompleted, HandleForcedCompactionCompleted);
        HFunc(TEvPartitionPrivate::TEvGetChangedBlocksCompleted, HandleGetChangedBlocksCompleted);
        HFunc(TEvPartitionPrivate::TEvAddConfirmedBlobsCompleted, HandleAddConfirmedBlobsCompleted);
        HFunc(TEvPartitionCommonPrivate::TEvDescribeBlocksCompleted, HandleDescribeBlocksCompleted);

        HFunc(TEvHiveProxy::TEvReassignTabletResponse, HandleReassignTabletResponse);

        IgnoreFunc(TEvPartitionPrivate::TEvCleanupResponse);
        IgnoreFunc(TEvPartitionPrivate::TEvCollectGarbageResponse);
        IgnoreFunc(TEvPartitionPrivate::TEvCompactionResponse);
        IgnoreFunc(TEvPartitionPrivate::TEvMetadataRebuildUsedBlocksResponse);
        IgnoreFunc(TEvPartitionPrivate::TEvMetadataRebuildBlockCountResponse);
        IgnoreFunc(TEvPartitionPrivate::TEvFlushResponse);
        IgnoreFunc(TEvPartitionCommonPrivate::TEvTrimFreshLogResponse);
        IgnoreFunc(TEvPartitionPrivate::TEvAddConfirmedBlobsResponse);

        default:
            if (!HandleRequests(ev) &&
                !HandleDefaultEvents(ev, SelfId()))
            {
                HandleUnexpectedEvent(
                    ev,
                    TBlockStoreComponents::PARTITION,
                    __PRETTY_FUNCTION__);
            }
            break;
    }
}

STFUNC(TPartitionActor::StateZombie)
{
    UpdateActorStatsSampled(ActorContext());
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvTablet::TEvTabletDead, HandleTabletDead);
        IgnoreFunc(TEvTablet::TEvTabletStop);

        IgnoreFunc(TEvents::TEvPoisonPill);
        IgnoreFunc(TEvTabletPipe::TEvServerConnected);
        IgnoreFunc(TEvTabletPipe::TEvServerDisconnected);

        IgnoreFunc(TEvPartitionPrivate::TEvUpdateCounters);
        IgnoreFunc(TEvPartitionPrivate::TEvUpdateYellowState);
        IgnoreFunc(TEvPartitionPrivate::TEvSendBackpressureReport);
        IgnoreFunc(TEvPartitionPrivate::TEvProcessWriteQueue);

        IgnoreFunc(TEvPartitionCommonPrivate::TEvReadBlobCompleted);
        IgnoreFunc(TEvPartitionCommonPrivate::TEvLongRunningOperation);
        IgnoreFunc(TEvPartitionCommonPrivate::TEvTrimFreshLogCompleted);
        IgnoreFunc(TEvPartitionPrivate::TEvWriteBlobCompleted);
        IgnoreFunc(TEvPartitionPrivate::TEvReadBlocksCompleted);
        IgnoreFunc(TEvPartitionPrivate::TEvWriteBlocksCompleted);
        IgnoreFunc(TEvPartitionPrivate::TEvZeroBlocksCompleted);
        IgnoreFunc(TEvPartitionPrivate::TEvFlushCompleted);
        IgnoreFunc(TEvPartitionPrivate::TEvCompactionCompleted);
        IgnoreFunc(TEvPartitionPrivate::TEvMetadataRebuildCompleted);
        IgnoreFunc(TEvPartitionPrivate::TEvScanDiskCompleted);
        IgnoreFunc(TEvPartitionPrivate::TEvCollectGarbageCompleted);
        IgnoreFunc(TEvPartitionPrivate::TEvForcedCompactionCompleted);
        IgnoreFunc(TEvPartitionPrivate::TEvGetChangedBlocksCompleted);

        IgnoreFunc(TEvPartitionPrivate::TEvCleanupResponse);
        IgnoreFunc(TEvPartitionPrivate::TEvCollectGarbageResponse);
        IgnoreFunc(TEvPartitionPrivate::TEvCompactionResponse);
        IgnoreFunc(TEvPartitionPrivate::TEvMetadataRebuildUsedBlocksResponse);
        IgnoreFunc(TEvPartitionPrivate::TEvMetadataRebuildBlockCountResponse);
        IgnoreFunc(TEvPartitionPrivate::TEvFlushResponse);

        IgnoreFunc(TEvHiveProxy::TEvReassignTabletResponse);

        default:
            if (!RejectRequests(ev)) {
                HandleUnexpectedEvent(
                    ev,
                    TBlockStoreComponents::PARTITION,
                    __PRETTY_FUNCTION__);
            }
            break;
    }
}

////////////////////////////////////////////////////////////////////////////////

void TPartitionActor::HandleGetPartitionInfo(
    const TEvVolume::TEvGetPartitionInfoRequest::TPtr& ev,
    const TActorContext& ctx)
{
    LOG_DEBUG(ctx, TBlockStoreComponents::PARTITION, "GetPartitionInfo request");

    auto json = State->AsJson();
    auto response = std::make_unique<TEvVolume::TEvGetPartitionInfoResponse>();
    response->Record.SetPayload(json.GetStringRobust());
    NCloud::Send(ctx, ev->Sender, std::move(response), ev->Cookie);
}

////////////////////////////////////////////////////////////////////////////////

void SetCounters(
    NProto::TIOCounters& counters,
    const TDuration execTime,
    const TDuration waitTime,
    ui64 blocksCount)
{
    counters.SetRequestsCount(1);
    counters.SetExecTime(execTime.MicroSeconds());
    counters.SetWaitTime(waitTime.MicroSeconds());
    counters.SetBlocksCount(blocksCount);
}

////////////////////////////////////////////////////////////////////////////////

NProto::TError VerifyBlockChecksum(
    const ui32 actualChecksum,
    const NKikimr::TLogoBlobID& blobID,
    const ui64 blockIndex,
    const ui16 blobOffset,
    const ui32 expectedChecksum)
{
    if (expectedChecksum == 0) {
        // 0 is a special case - block digest calculation can be
        // switched on and off => some blobs may not have digests for
        // some blocks
        //
        // we should just skip validation for this block

        return {};
    }

    if (actualChecksum != expectedChecksum) {
        ReportBlockDigestMismatchInBlob();
        // we might read proper data upon retry - let's give it a chance
        return MakeError(
            E_REJECTED,
            TStringBuilder() << "block digest mismatch, block="
                << blockIndex << ", offset=" << blobOffset
                << ", blobId=" << blobID
                << ", expected=" << expectedChecksum
                << ", actual=" << actualChecksum);
    }

    return {};
}

}   // namespace NCloud::NBlockStore::NStorage::NPartition
