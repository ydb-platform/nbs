#include "part2_actor.h"

#include <cloud/blockstore/libs/diagnostics/critical_events.h>
#include <cloud/blockstore/libs/storage/core/unimplemented.h>
#include <cloud/blockstore/libs/storage/partition_common/events_private.h>

#include <cloud/storage/core/libs/api/hive_proxy.h>
#include <cloud/storage/core/libs/common/verify.h>

#include <contrib/ydb/core/base/tablet_pipe.h>
#include <contrib/ydb/core/node_whiteboard/node_whiteboard.h>
#include <contrib/ydb/core/tablet/tablet_counters_aggregator.h>

namespace NCloud::NBlockStore::NStorage::NPartition2 {

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
    Y_UNUSED(ctx);

    if (!Counters || !PartCounters || !State) {
        return;
    }

    const auto& stats = State->GetStats();

#define BLOCKSTORE_PARTITION2_UPDATE_COUNTER(name, category, ...)              \
    {                                                                          \
        auto& counter = Counters->Cumulative()                                 \
            [TPartitionCounters::CUMULATIVE_COUNTER_##category##_##name];      \
        ui64 value = stats.Get##category##Counters().Get##name();              \
        Y_DEBUG_ABORT_UNLESS(value >= counter.Get());                                \
        if (value < counter.Get()) {                                           \
            ReportCounterUpdateRace();                                         \
        }                                                                      \
        counter.Increment(value - counter.Get());                              \
    }                                                                          \
// BLOCKSTORE_PARTITION2_UPDATE_COUNTER

    BLOCKSTORE_PARTITION2_IO_COUNTERS(BLOCKSTORE_PARTITION2_UPDATE_COUNTER)

#undef BLOCKSTORE_PARTITION2_UPDATE_COUNTER

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
            "[%lu] Retrying reassign request (timeout: %lu milliseconds)",
            TabletID(),
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
            "[%lu] Reassign request sent for channels: %s",
            TabletID(),
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
        ExecuteTx<TInitSchema>(ctx);
    }
}

void TPartitionActor::Activate(const TActorContext& ctx)
{
    BecomeAux(ctx, STATE_WORK);

    // allow pipes to connect
    SignalTabletActive(ctx);

    // resend pending requests
    SendPendingRequests(ctx, PendingRequests);

    if (!Config->GetRunV2SoftGcAtStartup()) {
        State->CollectGarbageHardRequested = true;
    }
    if (!Config->GetDontEnqueueCollectGarbageUponPartitionStartup()) {
        EnqueueCollectGarbageIfNeeded(ctx);
    }

    EnqueueFlushIfNeeded(ctx);
    EnqueueCompactionIfNeeded(ctx);

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
    TerminateTransactions(ctx);
    KillActors(ctx);
    ClearWriteQueue(ctx);
    CancelPendingRequests(ctx, PendingRequests);
}

void TPartitionActor::KillActors(const TActorContext& ctx)
{
    for (const auto& actor: Actors) {
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

void TPartitionActor::TerminateTransactions(const TActorContext& ctx)
{
    while (ActiveTransactions) {
        auto* requestInfo = ActiveTransactions.PopFront();
        STORAGE_VERIFY(
            requestInfo->RefCount() >= 1,
            TWellKnownEntityTypes::TABLET,
            TabletID());

        requestInfo->CancelRequest(ctx);
        requestInfo->UnRef();
    }
}

void TPartitionActor::ProcessIOQueue(const TActorContext& ctx, ui32 channel)
{
    while (auto requestActor = State->DequeueIORequest(channel)) {
        auto actorId = NCloud::Register(ctx, std::move(requestActor));
        Actors.insert(actorId);
    }
}

void TPartitionActor::ProcessCCCRequestQueue(
    const TActorContext& ctx)
{
    if (State->HasCCCRequestInProgress()) {
        // already in progress
        return;
    }

    auto& queue = State->GetCCCRequestQueue();

    if (queue.empty()) {
        // nothing to process
        return;
    }

    auto& front = queue.front();

    if (State->HasFreshBlocksInFlightUntil(front.CommitId)) {
        // wait for inflight fresh to complete
        return;
    }

    if (front.OnStartProcessing) {
        front.OnStartProcessing(ctx);
    }

    auto tx = std::move(front.Tx);
    queue.pop_front();

    State->StartProcessingCCCRequest();

    ExecuteTx(ctx, std::move(tx));
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
        )
    );
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

void TPartitionActor::UpdateExecutorStats(const TActorContext& ctx)
{
    auto& metrics = *Executor()->GetResourceMetrics();
    metrics.TryUpdate(ctx);
}

void TPartitionActor::UpdateNetworkStats(const TActorContext& ctx, ui64 value)
{
    auto& metrics = Executor()->GetResourceMetrics()->Network;
    metrics.Increment(value, ctx.Now());
}

void TPartitionActor::UpdateStorageStats(const TActorContext& ctx, i64 value)
{
    Y_UNUSED(ctx);

    auto& metrics = Executor()->GetResourceMetrics()->StorageUser;
    metrics.Increment(value);
}

void TPartitionActor::UpdateCPUUsageStats(const TActorContext& ctx, TDuration value)
{
    UserCPUConsumption += value.MicroSeconds();
    auto& metrics = Executor()->GetResourceMetrics()->CPU;
    metrics.Increment(value.MicroSeconds(), ctx.Now());
}

void TPartitionActor::UpdateWriteThroughput(
    const TActorContext& ctx,
    const NKikimr::NMetrics::TChannel& channel,
    const NKikimr::NMetrics::TGroupId& group,
    ui64 value)
{
    auto& metrics = Executor()->GetResourceMetrics()->WriteThroughput[
        std::make_pair(channel, group)];
    metrics.Increment(value, ctx.Now());

    auto& iops = Executor()->GetResourceMetrics()->WriteIops[
        std::make_pair(channel, group)];
    iops.Increment(1, ctx.Now());
}

void TPartitionActor::UpdateReadThroughput(
    const TActorContext& ctx,
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
        auto& metrics = Executor()->GetResourceMetrics()->ReadThroughput[
            std::make_pair(channel, group)];
        metrics.Increment(value, ctx.Now());

        auto& iops = Executor()->GetResourceMetrics()->ReadIops[
            std::make_pair(channel, group)];
        iops.Increment(1, ctx.Now());
    }
}

////////////////////////////////////////////////////////////////////////////////

void TPartitionActor::HandlePoisonPill(
    const TEvents::TEvPoisonPill::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    LOG_INFO(ctx, TBlockStoreComponents::PARTITION,
        "[%lu] Stop tablet because of PoisonPill request",
        TabletID());

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

////////////////////////////////////////////////////////////////////////////////

#define BLOCKSTORE_HANDLE_UNIMPLEMENTED_REQUEST(name, ns)                      \
    void TPartitionActor::Handle##name(                                        \
        const ns::TEv##name##Request::TPtr& ev,                                \
        const TActorContext& ctx)                                              \
    {                                                                          \
        RejectUnimplementedRequest<ns::T##name##Method>(ev, ctx);              \
    }                                                                          \
                                                                               \
// BLOCKSTORE_HANDLE_UNIMPLEMENTED_REQUEST

BLOCKSTORE_HANDLE_UNIMPLEMENTED_REQUEST(RebuildMetadata,          TEvVolume);
BLOCKSTORE_HANDLE_UNIMPLEMENTED_REQUEST(GetRebuildMetadataStatus, TEvVolume);
BLOCKSTORE_HANDLE_UNIMPLEMENTED_REQUEST(ScanDisk,                 TEvVolume);
BLOCKSTORE_HANDLE_UNIMPLEMENTED_REQUEST(GetScanDiskStatus,        TEvVolume);

////////////////////////////////////////////////////////////////////////////////

bool TPartitionActor::HandleRequests(STFUNC_SIG)
{
    switch (ev->GetTypeRewrite()) {
        BLOCKSTORE_PARTITION_REQUESTS(BLOCKSTORE_HANDLE_REQUEST, TEvPartition)
        BLOCKSTORE_PARTITION2_REQUESTS_PRIVATE(
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
        BLOCKSTORE_PARTITION2_REQUESTS_PRIVATE(
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
        HFunc(TEvPartitionPrivate::TEvInitFreshZonesCompleted, HandleInitFreshZonesCompleted);

        BLOCKSTORE_HANDLE_REQUEST(WaitReady, TEvPartition)
        BLOCKSTORE_HANDLE_REQUEST(InitIndex, TEvPartitionPrivate)

        IgnoreFunc(TEvHiveProxy::TEvReassignTabletResponse);

        default:
            if (!RejectRequests(ev) && !HandleDefaultEvents(ev, SelfId())) {
                HandleUnexpectedEvent(ev, TBlockStoreComponents::PARTITION);
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

        HFunc(TEvPartitionPrivate::TEvReadBlobCompleted, HandleReadBlobCompleted);
        HFunc(TEvPartitionPrivate::TEvWriteBlobCompleted, HandleWriteBlobCompleted);
        HFunc(TEvPartitionPrivate::TEvReadBlocksCompleted, HandleReadBlocksCompleted);
        HFunc(TEvPartitionPrivate::TEvWriteBlocksCompleted, HandleWriteBlocksCompleted);
        HFunc(TEvPartitionPrivate::TEvZeroBlocksCompleted, HandleZeroBlocksCompleted);
        HFunc(TEvPartitionPrivate::TEvFlushCompleted, HandleFlushCompleted);
        HFunc(TEvPartitionPrivate::TEvCompactionCompleted, HandleCompactionCompleted);
        HFunc(TEvPartitionPrivate::TEvCollectGarbageCompleted, HandleCollectGarbageCompleted);
        HFunc(TEvPartitionPrivate::TEvForcedCompactionCompleted, HandleForcedCompactionCompleted);
        HFunc(TEvPartitionCommonPrivate::TEvTrimFreshLogCompleted, HandleTrimFreshLogCompleted);
        HFunc(TEvPartitionPrivate::TEvGetChangedBlocksCompleted, HandleGetChangedBlocksCompleted);

        HFunc(TEvHiveProxy::TEvReassignTabletResponse, HandleReassignTabletResponse);

        IgnoreFunc(TEvPartitionPrivate::TEvCleanupResponse);
        IgnoreFunc(TEvPartitionPrivate::TEvCollectGarbageResponse);
        IgnoreFunc(TEvPartitionPrivate::TEvCompactionResponse);
        IgnoreFunc(TEvPartitionPrivate::TEvUpdateIndexStructuresResponse);
        IgnoreFunc(TEvPartitionPrivate::TEvFlushResponse);
        IgnoreFunc(TEvPartitionCommonPrivate::TEvTrimFreshLogResponse);

        default:
            if (!HandleRequests(ev) && !HandleDefaultEvents(ev, SelfId())) {
                HandleUnexpectedEvent(ev, TBlockStoreComponents::PARTITION);
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

        IgnoreFunc(TEvPartitionCommonPrivate::TEvTrimFreshLogCompleted);
        IgnoreFunc(TEvPartitionPrivate::TEvReadBlobCompleted);
        IgnoreFunc(TEvPartitionPrivate::TEvWriteBlobCompleted);
        IgnoreFunc(TEvPartitionPrivate::TEvReadBlocksCompleted);
        IgnoreFunc(TEvPartitionPrivate::TEvWriteBlocksCompleted);
        IgnoreFunc(TEvPartitionPrivate::TEvZeroBlocksCompleted);
        IgnoreFunc(TEvPartitionPrivate::TEvFlushCompleted);
        IgnoreFunc(TEvPartitionPrivate::TEvCompactionCompleted);
        IgnoreFunc(TEvPartitionPrivate::TEvCollectGarbageCompleted);
        IgnoreFunc(TEvPartitionPrivate::TEvForcedCompactionCompleted);
        IgnoreFunc(TEvPartitionPrivate::TEvGetChangedBlocksCompleted);

        IgnoreFunc(TEvPartitionPrivate::TEvCleanupResponse);
        IgnoreFunc(TEvPartitionPrivate::TEvCollectGarbageResponse);
        IgnoreFunc(TEvPartitionPrivate::TEvCompactionResponse);
        IgnoreFunc(TEvPartitionPrivate::TEvUpdateIndexStructuresResponse);
        IgnoreFunc(TEvPartitionPrivate::TEvFlushResponse);

        IgnoreFunc(TEvHiveProxy::TEvReassignTabletResponse);

        default:
            if (!RejectRequests(ev)) {
                HandleUnexpectedEvent(ev, TBlockStoreComponents::PARTITION);
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

}   // namespace NCloud::NBlockStore::NStorage::NPartition2
