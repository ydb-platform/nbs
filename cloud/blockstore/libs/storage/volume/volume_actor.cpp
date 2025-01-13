#include "volume_actor.h"

#include "volume_database.h"
#include "volume_tx.h"

#include <cloud/blockstore/libs/storage/api/bootstrapper.h>
#include <cloud/blockstore/libs/storage/api/service.h>
#include <cloud/blockstore/libs/storage/api/stats_service.h>
#include <cloud/blockstore/libs/storage/api/undelivered.h>
#include <cloud/blockstore/libs/storage/core/monitoring_utils.h>
#include <cloud/blockstore/libs/storage/core/proto_helpers.h>
#include <cloud/blockstore/libs/storage/volume/model/volume_throttler_logger.h>

// TODO: invalid reference
#include <cloud/blockstore/libs/storage/service/service_events_private.h>

#include <cloud/storage/core/libs/throttling/tablet_throttler.h>
#include <cloud/storage/core/libs/throttling/tablet_throttler_logger.h>

#include <contrib/ydb/core/base/tablet_pipe.h>
#include <contrib/ydb/core/node_whiteboard/node_whiteboard.h>
#include <contrib/ydb/core/tablet/tablet_counters_aggregator.h>

#include <library/cpp/lwtrace/log_shuttle.h>
#include <library/cpp/lwtrace/protos/lwtrace.pb.h>
#include <library/cpp/lwtrace/signature.h>

#include <util/string/builder.h>
#include <util/stream/str.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

using namespace NKikimr;
using namespace NKikimr::NNodeWhiteboard;
using namespace NKikimr::NTabletFlatExecutor;

using namespace NCloud::NBlockStore::NStorage::NPartition;

using namespace NCloud::NStorage;

namespace {

constexpr TInstant DRTabletIdRequestRetryInterval = TInstant::Seconds(3);

}   // namespace

////////////////////////////////////////////////////////////////////////////////

const TVolumeActor::TStateInfo TVolumeActor::States[STATE_MAX] = {
    { "Boot",   (IActor::TReceiveFunc)&TVolumeActor::StateBoot   },
    { "Init",   (IActor::TReceiveFunc)&TVolumeActor::StateInit   },
    { "Work",   (IActor::TReceiveFunc)&TVolumeActor::StateWork   },
    { "Zombie", (IActor::TReceiveFunc)&TVolumeActor::StateZombie },
};

TVolumeActor::TVolumeActor(
        const TActorId& owner,
        TTabletStorageInfoPtr storage,
        TStorageConfigPtr config,
        TDiagnosticsConfigPtr diagnosticsConfig,
        IProfileLogPtr profileLog,
        IBlockDigestGeneratorPtr blockDigestGenerator,
        ITraceSerializerPtr traceSerializer,
        NRdma::IClientPtr rdmaClient,
        NServer::IEndpointEventHandlerPtr endpointEventHandler,
        EVolumeStartMode startMode)
    : TActor(&TThis::StateBoot)
    , TTabletBase(owner, std::move(storage))
    , GlobalStorageConfig(config)
    , Config(std::move(config))
    , DiagnosticsConfig(std::move(diagnosticsConfig))
    , ProfileLog(std::move(profileLog))
    , BlockDigestGenerator(std::move(blockDigestGenerator))
    , TraceSerializer(std::move(traceSerializer))
    , RdmaClient(std::move(rdmaClient))
    , EndpointEventHandler(std::move(endpointEventHandler))
    , StartMode(startMode)
    , ThrottlerLogger(
        TabletID(),
        [this](ui32 opType, TDuration time) {
            UpdateDelayCounter(
                static_cast<TVolumeThrottlingPolicy::EOpType>(opType),
                time);
        }
    )
{}

TVolumeActor::~TVolumeActor()
{
    // For unit tests under asan only, since test actor
    // runtime just calls destructor upon test completion
    // Usually we terminate inflight transactions
    // in BeforeDie.
    ReleaseTransactions();
}

TString TVolumeActor::GetStateName(ui32 state)
{
    if (state < STATE_MAX) {
        return States[state].Name;
    }
    return "<unknown>";
}

void TVolumeActor::Enqueue(STFUNC_SIG)
{
    ALOG_ERROR(TBlockStoreComponents::VOLUME,
        "[" << TabletID() << "]"
        << " IGNORING message type# " << ev->GetTypeRewrite()
        << " from Sender# " << ToString(ev->Sender)
        << " in StateBoot");
}

void TVolumeActor::DefaultSignalTabletActive(const TActorContext& ctx)
{
    Y_UNUSED(ctx); // postpone until LoadState transaction completes
}

void TVolumeActor::BecomeAux(const TActorContext& ctx, EState state)
{
    Y_DEBUG_ABORT_UNLESS(state < STATE_MAX);

    if (state == EState::STATE_INIT) {
        VolumeRequestIdGenerator =
            TCompositeId::FromGeneration(Executor()->Generation());
    }

    Become(States[state].Func);
    CurrentState = state;

    LOG_DEBUG(ctx, TBlockStoreComponents::VOLUME,
        "[%lu] Switched to state %s (system: %s, user: %s, executor: %s)",
        TabletID(),
        States[state].Name.data(),
        ToString(Tablet()).data(),
        ToString(SelfId()).data(),
        ToString(ExecutorID()).data());

    ReportTabletState(ctx);
}

void TVolumeActor::ReportTabletState(const TActorContext& ctx)
{
    auto service = MakeNodeWhiteboardServiceId(SelfId().NodeId());

    auto request = std::make_unique<TEvWhiteboard::TEvTabletStateUpdate>(
        TabletID(),
        CurrentState);

    NCloud::Send(ctx, service, std::move(request));
}

void TVolumeActor::RegisterCounters(const TActorContext& ctx)
{
    Y_DEBUG_ABORT_UNLESS(State);

    Y_UNUSED(ctx);

    if (!Counters) {
        auto counters = CreateVolumeCounters();

        // LAME: ownership transferred to executor
        Counters = counters.get();
        Executor()->RegisterExternalTabletCounters(counters.release());

        // only aggregated statistics will be reported by default
        // (you can always turn on per-tablet statistics on monitoring page)
        // TabletCountersAddTablet(TabletID(), ctx);
    }

    if (!VolumeSelfCounters) {
        VolumeSelfCounters = CreateVolumeSelfCounters(
            State->CountersPolicy(),
            DiagnosticsConfig->GetHistogramCounterOptions());
    }
}

void TVolumeActor::ScheduleRegularUpdates(const TActorContext& ctx)
{
    if (!UpdateCountersScheduled) {
        ctx.Schedule(UpdateCountersInterval,
            new TEvVolumePrivate::TEvUpdateCounters());
        UpdateCountersScheduled = true;
    }

    if (!UpdateLeakyBucketCountersScheduled) {
        ctx.Schedule(UpdateLeakyBucketCountersInterval,
            new TEvVolumePrivate::TEvUpdateThrottlerState());
        UpdateLeakyBucketCountersScheduled = true;
    }

    if (!UpdateReadWriteClientInfoScheduled) {
        ctx.Schedule(UpdateCountersInterval,
            new TEvVolumePrivate::TEvUpdateReadWriteClientInfo());
        UpdateReadWriteClientInfoScheduled = true;
    }

    if (!RemoveExpiredVolumeParamsScheduled && State) {
        const auto delay = State->GetVolumeParams().GetNextExpirationDelay(ctx.Now());
        if (delay.Defined()) {
            ctx.Schedule(ctx.Now() + *delay,
                    new TEvVolumePrivate::TEvRemoveExpiredVolumeParams());
            RemoveExpiredVolumeParamsScheduled = true;
        }
    }
}

void TVolumeActor::UpdateLeakyBucketCounters(const TActorContext& ctx)
{
    Y_UNUSED(ctx);

    if (!State) {
        return;
    }

    auto& simple = VolumeSelfCounters->Simple;
    auto& cumulative = VolumeSelfCounters->Cumulative;
    const auto& tp = State->GetThrottlingPolicy();
    ui64 currentRate = tp.CalculateCurrentSpentBudgetShare(ctx.Now()) * 100;
    simple.MaxUsedQuota.Set(Max(simple.MaxUsedQuota.Value, currentRate));
    cumulative.UsedQuota.Increment(currentRate);
}

void TVolumeActor::UpdateCounters(const TActorContext& ctx)
{
    Y_UNUSED(ctx);

    if (!State) {
        return;
    }

    SendPartStatsToService(ctx);
    SendSelfStatsToService(ctx);
}

void TVolumeActor::OnActivateExecutor(const TActorContext& ctx)
{
    ExecutorActivationTimestamp = ctx.Now();

    LOG_INFO(ctx, TBlockStoreComponents::VOLUME,
        "[%lu] Activated executor",
        TabletID());

    ScheduleRegularUpdates(ctx);

    if (!Executor()->GetStats().IsFollower) {
        ExecuteTx<TInitSchema>(ctx);
    }

    BecomeAux(ctx, STATE_INIT);
}

bool TVolumeActor::ReassignChannelsEnabled() const
{
    return true;
}

bool TVolumeActor::OnRenderAppHtmlPage(
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

void TVolumeActor::OnDetach(const TActorContext& ctx)
{
    Counters = nullptr;

    BeforeDie(ctx);
    Die(ctx);
}

void TVolumeActor::OnTabletDead(
    TEvTablet::TEvTabletDead::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    BeforeDie(ctx);
    Die(ctx);
}

void TVolumeActor::BeforeDie(const TActorContext& ctx)
{
    UnregisterVolume(ctx);
    StopPartitions(ctx, {});
    TerminateTransactions(ctx);
    KillActors(ctx);
    CancelRequests(ctx);

    for (auto& [_, handler]: OnPartitionStopped) {
        std::invoke(handler, ctx);
    }
    OnPartitionStopped.clear();

    for (auto& [part, handler]: WaitForPartitions) {
        if (handler) {
            std::invoke(handler, ctx, MakeError(E_REJECTED, "tablet is shutting down"));
        }
    }
    StoppedPartitions.clear();
    WaitForPartitions.clear();
}

void TVolumeActor::KillActors(const TActorContext& ctx)
{
    for (auto& actor: Actors) {
        NCloud::Send<TEvents::TEvPoisonPill>(ctx, actor);
    }
}

void TVolumeActor::AddTransaction(TRequestInfo& requestInfo)
{
    requestInfo.Ref();

    Y_ABORT_UNLESS(requestInfo.Empty());
    ActiveTransactions.PushBack(&requestInfo);
}

void TVolumeActor::RemoveTransaction(TRequestInfo& requestInfo)
{
    Y_ABORT_UNLESS(!requestInfo.Empty());
    requestInfo.Unlink();

    Y_ABORT_UNLESS(requestInfo.RefCount() > 1);
    requestInfo.UnRef();
}

void TVolumeActor::TerminateTransactions(const TActorContext& ctx)
{
    while (ActiveTransactions) {
        TRequestInfo* requestInfo = ActiveTransactions.PopFront();
        if (requestInfo->CancelRoutine) {
            requestInfo->CancelRequest(ctx);
        }

        Y_ABORT_UNLESS(requestInfo->RefCount() >= 1);
        requestInfo->UnRef();
    }
}

void TVolumeActor::ReleaseTransactions()
{
    while (ActiveTransactions) {
        TRequestInfo* requestInfo = ActiveTransactions.PopFront();
        Y_ABORT_UNLESS(requestInfo->RefCount() >= 1);
        requestInfo->UnRef();
    }
}

TString TVolumeActor::GetVolumeStatusString(TVolumeActor::EStatus status) const
{
    switch (status)
    {
        case TVolumeActor::STATUS_ONLINE: {
            return State->GetLocalMountClientId().empty() ?
                "Online":
                "Online (preempted)";
        }
        case TVolumeActor::STATUS_INACTIVE: return "Inactive";
        case TVolumeActor::STATUS_MOUNTED: return "Mounted";
        default: return "Offline";
    }
}

TVolumeActor::EStatus TVolumeActor::GetVolumeStatus() const
{
    if (State) {
        auto state = State->GetPartitionsState();
        if (state == TPartitionInfo::READY) {
            return (StartMode == EVolumeStartMode::MOUNTED)
                ? TVolumeActor::STATUS_MOUNTED
                : TVolumeActor::STATUS_ONLINE;
        } else if (state == TPartitionInfo::UNKNOWN) {
            return TVolumeActor::STATUS_INACTIVE;
        }
    }
    return TVolumeActor::STATUS_OFFLINE;
}

NRdma::IClientPtr TVolumeActor::GetRdmaClient() const
{
    return (Config->GetUseNonreplicatedRdmaActor() && State->GetUseRdma())
               ? RdmaClient
               : NRdma::IClientPtr{};
}

ui64 TVolumeActor::GetBlocksCount() const
{
    return State ? State->GetBlocksCount() : 0;
}

void TVolumeActor::OnServicePipeDisconnect(
    const TActorContext& ctx,
    const TActorId& serverId,
    TInstant timestamp)
{
    Y_UNUSED(ctx);
    if (State) {
        State->SetServiceDisconnected(serverId, timestamp);
    }
}

void TVolumeActor::RegisterVolume(const TActorContext& ctx)
{
    DoRegisterVolume(ctx, State->GetDiskId());
}

void TVolumeActor::DoRegisterVolume(
    const TActorContext& ctx,
    const TString& diskId)
{
    Y_DEBUG_ABORT_UNLESS(diskId);

    NProto::TVolume volume;
    VolumeConfigToVolume(State->GetMeta().GetVolumeConfig(), volume);

    if (State->GetDiskId() != diskId) {
        // Handle shadow disk.
        volume.SetDiskId(diskId);
    }

    {
        auto request = std::make_unique<TEvService::TEvRegisterVolume>(
            volume.GetDiskId(),
            TabletID(),
            volume);
        NCloud::Send(ctx, MakeStorageServiceId(), std::move(request));
    }

    {
        auto request = std::make_unique<TEvStatsService::TEvRegisterVolume>(
            volume.GetDiskId(),
            TabletID(),
            std::move(volume));
        NCloud::Send(ctx, MakeStorageStatsServiceId(), std::move(request));
    }
}

void TVolumeActor::UnregisterVolume(const TActorContext& ctx)
{
    if (!State) {
        return;
    }

    DoUnregisterVolume(ctx, State->GetDiskId());

    // Unregister shadow disks volumes.
    for (const auto& [checkpointId, checkpointInfo]:
         State->GetCheckpointStore().GetActiveCheckpoints())
    {
        if (checkpointInfo.ShadowDiskId) {
            DoUnregisterVolume(ctx, checkpointInfo.ShadowDiskId);
        }
    }
}

void TVolumeActor::DoUnregisterVolume(
    const TActorContext& ctx,
    const TString& diskId)
{
    Y_DEBUG_ABORT_UNLESS(diskId);

    NCloud::Send(
        ctx,
        MakeStorageServiceId(),
        std::make_unique<TEvService::TEvUnregisterVolume>(diskId));

    NCloud::Send(
        ctx,
        MakeStorageStatsServiceId(),
        std::make_unique<TEvStatsService::TEvUnregisterVolume>(diskId));
}

void TVolumeActor::SendVolumeConfigUpdated(const TActorContext& ctx)
{
    NProto::TVolume volume;
    VolumeConfigToVolume(State->GetMeta().GetVolumeConfig(), volume);
    {
        auto request = std::make_unique<TEvService::TEvVolumeConfigUpdated>(
            State->GetDiskId(),
            volume);
        NCloud::Send(ctx, MakeStorageServiceId(), std::move(request));
    }

    {
        auto request = std::make_unique<TEvStatsService::TEvVolumeConfigUpdated>(
            State->GetDiskId(),
            std::move(volume));
        NCloud::Send(ctx, MakeStorageStatsServiceId(), std::move(request));
    }
}

void TVolumeActor::SendVolumeSelfCounters(const TActorContext& ctx)
{
    auto request = std::make_unique<TEvStatsService::TEvVolumeSelfCounters>(
        State->GetConfig().GetDiskId(),
        State->HasActiveClients(ctx.Now()),
        State->IsPreempted(SelfId()),
        std::move(VolumeSelfCounters),
        FailedBoots);
    FailedBoots = 0;
    NCloud::Send(ctx, MakeStorageStatsServiceId(), std::move(request));
}

void TVolumeActor::ResetThrottlingPolicy()
{
    ThrottlerLogger.SetupTabletId(TabletID());
    if (!Throttler) {
        Throttler = CreateTabletThrottler(
            *this,
            ThrottlerLogger,
            State->AccessThrottlingPolicy());
    } else {
        Throttler->ResetPolicy(State->AccessThrottlingPolicy());
    }
}

void TVolumeActor::CancelRequests(const TActorContext& ctx)
{
    if (ShuttingDown) {
        return;
    }
    ShuttingDown = true;

    for (auto& [volumeRequestId, volumeRequest]: VolumeRequests) {
        volumeRequest.CancelRequest(
            ctx,
            MakeError(E_REJECTED, "Shutting down"));
    }
    VolumeRequests.clear();

    if (Throttler) {
        // Throttler can be uninitialized if tablet was not fully initialized.
        Throttler->OnShutDown(ctx);
    }

    while (ActiveReadHistoryRequests) {
        TRequestInfo* requestInfo = ActiveReadHistoryRequests.PopFront();
        TStringStream out;
        NMonitoringUtils::DumpTabletNotReady(out);
        NCloud::Reply(
            ctx,
            *requestInfo,
            std::make_unique<NMon::TEvRemoteHttpInfoRes>(out.Str()));

        Y_ABORT_UNLESS(requestInfo->RefCount() >= 1);
        requestInfo->UnRef();
    }

    CancelPendingRequests(ctx, PendingRequests);
}

NKikimr::NMetrics::TResourceMetrics* TVolumeActor::GetResourceMetrics()
{
    return Executor()->GetResourceMetrics();
}

bool TVolumeActor::CheckReadWriteBlockRange(const TBlockRange64& range) const
{
    return TBlockRange64::WithLength(0, State->GetBlocksCount())
        .Contains(range);
}

////////////////////////////////////////////////////////////////////////////////

void TVolumeActor::HandlePoisonPill(
    const TEvents::TEvPoisonPill::TPtr&,
    const TActorContext& ctx)
{
    CancelRequests(ctx);
    NCloud::Send<TEvents::TEvPoisonPill>(ctx, Tablet());
    BecomeAux(ctx, STATE_ZOMBIE);
}

void TVolumeActor::HandlePoisonTaken(
    const TEvents::TEvPoisonTaken::TPtr& ev,
    const TActorContext& ctx)
{
    LOG_INFO(ctx, TBlockStoreComponents::VOLUME,
        "[%lu] Partition %s stopped",
        TabletID(),
        ev->Sender.ToString().c_str());

    if (auto* callback = OnPartitionStopped.FindPtr(ev->Cookie)) {
        std::invoke(*callback, ctx);
        OnPartitionStopped.erase(ev->Cookie);
    }

    if (WaitForPartitions.empty()) {
        return;
    }

    StoppedPartitions.insert(ev->Sender);

    auto it = WaitForPartitions.begin();

    for (; it != WaitForPartitions.end(); ++it) {
        auto& [part, handler] = *it;

        if (!StoppedPartitions.erase(part)) {
            break;
        }

        if (handler) {
            std::invoke(handler, ctx, NProto::TError());
        }
    }

    const auto removed = std::distance(WaitForPartitions.begin(), it);

    WaitForPartitions.erase(WaitForPartitions.begin(), it);

    if (WaitForPartitions.empty()) {
        StoppedPartitions.clear();
    }

    TStringStream waitList;
    for (const auto& [p, _]: WaitForPartitions) {
        waitList << p.ToString() << " ";
    }

    TStringStream stoppedList;
    for (const auto& p: StoppedPartitions) {
        stoppedList << p.ToString() << " ";
    }

    LOG_INFO(ctx, TBlockStoreComponents::VOLUME,
        "[%lu] Partitions removed from the wait list: %lu. W: [ %s], S: [ %s]",
        TabletID(),
        removed,
        waitList.Str().c_str(),
        stoppedList.Str().c_str());
}

void TVolumeActor::HandleUpdateThrottlerState(
    const TEvVolumePrivate::TEvUpdateThrottlerState::TPtr& ev,
    const TActorContext& ctx)
{
    UpdateLeakyBucketCountersScheduled = false;

    UpdateLeakyBucketCounters(ctx);
    ScheduleRegularUpdates(ctx);

    if (!State) {
        return;
    }

    const auto mediaKind = static_cast<NCloud::NProto::EStorageMediaKind>(
        GetNewestConfig().GetStorageMediaKind());
    if ((mediaKind == NCloud::NProto::EStorageMediaKind::STORAGE_MEDIA_HDD
            || mediaKind == NCloud::NProto::EStorageMediaKind::STORAGE_MEDIA_HYBRID)
            && ctx.Now() - Config->GetThrottlerStateWriteInterval() >= LastThrottlerStateWrite)
    {
        auto requestInfo = CreateRequestInfo(
            ev->Sender,
            ev->Cookie,
            ev->Get()->CallContext);

        ExecuteTx<TWriteThrottlerState>(
            ctx,
            std::move(requestInfo),
            TVolumeDatabase::TThrottlerStateInfo{
                State->GetThrottlingPolicy().GetCurrentBoostBudget().MilliSeconds()
            });
    }
}

void TVolumeActor::HandleUpdateCounters(
    const TEvVolumePrivate::TEvUpdateCounters::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    UpdateCountersScheduled = false;

    UpdateCounters(ctx);
    ScheduleRegularUpdates(ctx);

    if (State) {
        State->AccessMountHistory().CleanupHistoryIfNeeded(
            ctx.Now() - Config->GetVolumeHistoryDuration());

        auto requestInfo = CreateRequestInfo(
            ev->Sender,
            ev->Cookie,
            ev->Get()->CallContext);

        ExecuteTx<TCleanupHistory>(
            ctx,
            std::move(requestInfo),
            ctx.Now() - Config->GetVolumeHistoryDuration(),
            Config->GetVolumeHistoryCleanupItemCount());
    }
}

void TVolumeActor::HandleServerConnected(
    const TEvTabletPipe::TEvServerConnected::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    LOG_DEBUG(ctx, TBlockStoreComponents::VOLUME,
        "[%lu] Pipe client %s (server %s) connected to volume %s",
        TabletID(),
        ToString(msg->ClientId).data(),
        ToString(msg->ServerId).data(),
        (State ? State->GetDiskId().Quote().data() : "<empty>"));
}

void TVolumeActor::HandleServerDisconnected(
    const TEvTabletPipe::TEvServerDisconnected::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();
    const auto now = ctx.Now();

    LOG_DEBUG(ctx, TBlockStoreComponents::VOLUME,
        "[%lu] Pipe client %s (server %s) disconnected from volume %s at %s",
        TabletID(),
        ToString(msg->ClientId).data(),
        ToString(msg->ServerId).data(),
        (State ? State->GetDiskId().Quote().data() : "<empty>"),
        now.ToString().data());

    OnServicePipeDisconnect(ctx, msg->ServerId, now);
}

void TVolumeActor::HandleServerDestroyed(
    const TEvTabletPipe::TEvServerDestroyed::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();
    const auto now = ctx.Now();

    LOG_DEBUG(ctx, TBlockStoreComponents::VOLUME,
        "[%lu] Pipe client %s's server %s got destroyed for volume %s at %s",
        TabletID(),
        ToString(msg->ClientId).data(),
        ToString(msg->ServerId).data(),
        (State ? State->GetDiskId().Quote().data() : "<empty>"),
        now.ToString().data());

    OnServicePipeDisconnect(ctx, msg->ServerId, now);
}

void TVolumeActor::HandleGetDrTabletInfoResponse(
    const TEvDiskRegistryProxy::TEvGetDrTabletInfoResponse::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    Y_UNUSED(ctx);
    const auto* msg = ev->Get();

    if (HasError(msg->GetError())) {
        auto request =
            std::make_unique<TEvDiskRegistryProxy::TEvGetDrTabletInfoRequest>();
        TActivationContext::Schedule(
            DRTabletIdRequestRetryInterval,
            new IEventHandle(
                MakeDiskRegistryProxyServiceId(),
                SelfId(),
                request.release()));
        return;
    }

    DiskRegistryTabletId = msg->TabletId;
}

void TVolumeActor::ResetServicePipes(const TActorContext& ctx)
{
    for (const auto& actor: State->ClearPipeServerIds(ctx.Now())) {
        NCloud::Send<TEvents::TEvPoisonPill>(ctx, actor);
    }
}

void TVolumeActor::HandleTabletMetrics(
    NKikimr::TEvLocal::TEvTabletMetrics::TPtr& ev,
    const TActorContext& ctx)
{
    auto* resourceMetrics = GetResourceMetrics();
    if (resourceMetrics != nullptr) {
        const auto* msg = ev->Get();
        const auto& metrics = msg->ResourceValues;
        bool changed = false;
        if (metrics.HasCPU()) {
            resourceMetrics->CPU.Increment(metrics.GetCPU(), ctx.Now());
            changed = true;
        }
        if (metrics.HasNetwork()) {
            resourceMetrics->Network.Increment(metrics.GetNetwork(), ctx.Now());
            changed = true;
        }
        if (metrics.HasStorage()) {
            resourceMetrics->StorageUser.Increment(metrics.GetStorage());
            changed = true;
        }
        if (metrics.GroupReadThroughputSize() > 0) {
            for (const auto &v: metrics.GetGroupReadThroughput()) {
                auto id = std::make_pair(v.GetChannel(), v.GetGroupID());
                resourceMetrics->ReadThroughput[id].Increment(
                    v.GetThroughput(),
                    ctx.Now());
            }
            changed = true;
        }
        if (metrics.GroupWriteThroughputSize() > 0) {
            for (const auto &v: metrics.GetGroupWriteThroughput()) {
                auto id = std::make_pair(v.GetChannel(), v.GetGroupID());
                resourceMetrics->WriteThroughput[id].Increment(
                    v.GetThroughput(),
                    ctx.Now());
            }
            changed = true;
        }
        if (metrics.GroupReadIopsSize() > 0) {
            for (const auto &v: metrics.GetGroupReadIops()) {
                auto id = std::make_pair(v.GetChannel(), v.GetGroupID());
                resourceMetrics->ReadIops[id].Increment(
                    v.GetIops(),
                    ctx.Now());
            }
            changed = true;
        }
        if (metrics.GroupWriteIopsSize() > 0) {
            for (const auto &v: metrics.GetGroupWriteIops()) {
                auto id = std::make_pair(v.GetChannel(), v.GetGroupID());
                resourceMetrics->WriteIops[id].Increment(
                    v.GetIops(),
                    ctx.Now());
            }
            changed = true;
        }
        if (changed) {
            resourceMetrics->TryUpdate(ctx);
        }
    }
}

void TVolumeActor::HandleGetStorageConfig(
    const TEvVolume::TEvGetStorageConfigRequest::TPtr& ev,
    const TActorContext& ctx)
{
    auto response = std::make_unique<TEvVolume::TEvGetStorageConfigResponse>();
    *response->Record.MutableStorageConfig() = Config->GetStorageConfigProto();

    NCloud::Reply(
        ctx,
        *ev,
        std::move(response));
}

bool TVolumeActor::HandleRequests(STFUNC_SIG)
{
    switch (ev->GetTypeRewrite()) {
        BLOCKSTORE_VOLUME_REQUESTS(BLOCKSTORE_HANDLE_REQUEST, TEvVolume)
        BLOCKSTORE_VOLUME_REQUESTS_PRIVATE(
            BLOCKSTORE_HANDLE_REQUEST,
            TEvVolumePrivate)
        BLOCKSTORE_VOLUME_REQUESTS_FWD_SERVICE(
            BLOCKSTORE_HANDLE_REQUEST,
            TEvService)

        BLOCKSTORE_VOLUME_HANDLED_RESPONSES(
            BLOCKSTORE_HANDLE_RESPONSE,
            TEvVolume)
        BLOCKSTORE_VOLUME_HANDLED_RESPONSES_FWD_SERVICE(
            BLOCKSTORE_HANDLE_RESPONSE,
            TEvService)

        default:
            return false;
    }

    return true;
}

bool TVolumeActor::RejectRequests(STFUNC_SIG)
{
    switch (ev->GetTypeRewrite()) {
        BLOCKSTORE_VOLUME_REQUESTS(BLOCKSTORE_REJECT_REQUEST, TEvVolume)
        BLOCKSTORE_VOLUME_REQUESTS_PRIVATE(
            BLOCKSTORE_REJECT_REQUEST,
            TEvVolumePrivate)
        BLOCKSTORE_VOLUME_REQUESTS_FWD_SERVICE(
            BLOCKSTORE_REJECT_REQUEST,
            TEvService)

        BLOCKSTORE_VOLUME_HANDLED_RESPONSES(
            BLOCKSTORE_IGNORE_RESPONSE,
            TEvVolume)
        BLOCKSTORE_VOLUME_HANDLED_RESPONSES_FWD_SERVICE(
            BLOCKSTORE_IGNORE_RESPONSE,
            TEvService)

        default:
            return false;
    }

    return true;
}

////////////////////////////////////////////////////////////////////////////////

STFUNC(TVolumeActor::StateBoot)
{
    UpdateActorStatsSampled(ActorContext());
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);

        IgnoreFunc(TEvTabletPipe::TEvServerConnected);
        IgnoreFunc(TEvTabletPipe::TEvServerDisconnected);

        HFunc(TEvVolumePrivate::TEvUpdateCounters, HandleUpdateCounters);
        HFunc(
            TEvVolumePrivate::TEvUpdateThrottlerState,
            HandleUpdateThrottlerState);
        HFunc(
            TEvVolumePrivate::TEvUpdateReadWriteClientInfo,
            HandleUpdateReadWriteClientInfo);
        HFunc(
            TEvVolumePrivate::TEvRemoveExpiredVolumeParams,
            HandleRemoveExpiredVolumeParams);

        BLOCKSTORE_HANDLE_REQUEST(WaitReady, TEvVolume)

        default:
            if (!RejectRequests(ev)) {
                StateInitImpl(ev, SelfId());
            }
            break;
    }
}

STFUNC(TVolumeActor::StateInit)
{
    UpdateActorStatsSampled(ActorContext());
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);

        IgnoreFunc(TEvTabletPipe::TEvServerConnected);
        IgnoreFunc(TEvTabletPipe::TEvServerDisconnected);

        HFunc(
            TEvVolumePrivate::TEvProcessUpdateVolumeConfig,
            HandleProcessUpdateVolumeConfig);
        HFunc(
            TEvBlockStore::TEvUpdateVolumeConfig,
            HandleUpdateVolumeConfig);
        HFunc(
            TEvVolumePrivate::TEvAllocateDiskIfNeeded,
            HandleAllocateDiskIfNeeded);
        HFunc(
            TEvDiskRegistry::TEvAllocateDiskResponse,
            HandleAllocateDiskResponse);

        HFunc(TEvVolumePrivate::TEvUpdateCounters, HandleUpdateCounters);
        HFunc(
            TEvVolumePrivate::TEvUpdateThrottlerState,
            HandleUpdateThrottlerState);
        HFunc(
            TEvVolumePrivate::TEvUpdateReadWriteClientInfo,
            HandleUpdateReadWriteClientInfo);
        HFunc(
            TEvVolumePrivate::TEvRemoveExpiredVolumeParams,
            HandleRemoveExpiredVolumeParams);

        HFunc(
            TEvVolumePrivate::TEvUpdateShadowDiskStateRequest,
            HandleUpdateShadowDiskState);

        HFunc(
            TEvDiskRegistryProxy::TEvGetDrTabletInfoResponse,
            HandleGetDrTabletInfoResponse);

        BLOCKSTORE_HANDLE_REQUEST(WaitReady, TEvVolume)

        default:
            if (!RejectRequests(ev) && !HandleDefaultEvents(ev, SelfId())) {
                HandleUnexpectedEvent(ev, TBlockStoreComponents::VOLUME);
            }
            break;
    }
}

STFUNC(TVolumeActor::StateWork)
{
    UpdateActorStatsSampled(ActorContext());
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);
        HFunc(TEvents::TEvPoisonTaken, HandlePoisonTaken);
        HFunc(TEvents::TEvWakeup, HandleWakeup);

        HFunc(TEvTabletPipe::TEvServerConnected, HandleServerConnected);
        HFunc(TEvTabletPipe::TEvServerDisconnected, HandleServerDisconnected);
        HFunc(TEvTabletPipe::TEvServerDestroyed, HandleServerDestroyed);

        HFunc(TEvBootstrapper::TEvStatus, HandleTabletStatus);
        HFunc(
            TEvVolumePrivate::TEvProcessUpdateVolumeConfig,
            HandleProcessUpdateVolumeConfig);
        HFunc(TEvBlockStore::TEvUpdateVolumeConfig, HandleUpdateVolumeConfig);
        HFunc(
            TEvVolumePrivate::TEvAllocateDiskIfNeeded,
            HandleAllocateDiskIfNeeded);

        HFunc(TEvVolumePrivate::TEvUpdateCounters, HandleUpdateCounters);
        HFunc(
            TEvVolumePrivate::TEvUpdateThrottlerState,
            HandleUpdateThrottlerState);
        HFunc(
            TEvVolumePrivate::TEvUpdateReadWriteClientInfo,
            HandleUpdateReadWriteClientInfo);
        HFunc(
            TEvVolumePrivate::TEvRemoveExpiredVolumeParams,
            HandleRemoveExpiredVolumeParams);

        HFunc(
            TEvVolume::TEvDiskRegistryBasedPartitionCounters,
            HandleDiskRegistryBasedPartCounters);
        HFunc(TEvStatsService::TEvVolumePartCounters, HandlePartCounters);
        HFunc(TEvVolumePrivate::TEvPartStatsSaved, HandlePartStatsSaved);
        HFunc(
            TEvVolumePrivate::TEvWriteOrZeroCompleted,
            HandleWriteOrZeroCompleted);
        HFunc(
            TEvDiskRegistry::TEvAcquireDiskResponse,
            HandleAcquireDiskResponse);
        HFunc(
            TEvVolumePrivate::TEvAcquireDiskIfNeeded,
            HandleAcquireDiskIfNeeded);
        HFunc(TEvVolume::TEvReacquireDisk, HandleReacquireDisk);
        HFunc(TEvVolume::TEvRdmaUnavailable, HandleRdmaUnavailable);
        HFunc(
            TEvDiskRegistry::TEvReleaseDiskResponse,
            HandleReleaseDiskResponse);
        HFunc(
            TEvDiskRegistry::TEvAllocateDiskResponse,
            HandleAllocateDiskResponse);

        HFunc(
            TEvVolumePrivate::TEvRetryStartPartition,
            HandleRetryStartPartition);

        HFunc(TEvHiveProxy::TEvBootExternalResponse, HandleBootExternalResponse);
        HFunc(TEvPartition::TEvWaitReadyResponse, HandleWaitReadyResponse);
        HFunc(TEvPartition::TEvBackpressureReport, HandleBackpressureReport);
        HFunc(TEvPartition::TEvGarbageCollectorCompleted, HandleGarbageCollectorCompleted);

        HFunc(TEvLocal::TEvTabletMetrics, HandleTabletMetrics);

        HFunc(TEvVolume::TEvPreparePartitionMigrationRequest, HandlePreparePartitionMigration);

        HFunc(TEvVolume::TEvUpdateMigrationState, HandleUpdateMigrationState);
        HFunc(TEvVolume::TEvUpdateResyncState, HandleUpdateResyncState);
        HFunc(TEvVolume::TEvResyncFinished, HandleResyncFinished);

        HFunc(
            TEvPartitionCommonPrivate::TEvLongRunningOperation,
            HandleLongRunningBlobOperation);

        HFunc(
            TEvVolumePrivate::TEvUpdateShadowDiskStateRequest,
            HandleUpdateShadowDiskState);

        HFunc(
            TEvDiskRegistryProxy::TEvGetDrTabletInfoResponse,
            HandleGetDrTabletInfoResponse);

        default:
            if (!HandleRequests(ev) && !HandleDefaultEvents(ev, SelfId())) {
                HandleUnexpectedEvent(ev, TBlockStoreComponents::VOLUME);
            }
            break;
    }
}

STFUNC(TVolumeActor::StateZombie)
{
    UpdateActorStatsSampled(ActorContext());
    switch (ev->GetTypeRewrite()) {
        IgnoreFunc(TEvTabletPipe::TEvServerConnected);
        IgnoreFunc(TEvTabletPipe::TEvServerDisconnected);

        HFunc(TEvTablet::TEvTabletDead, HandleTabletDead);

        IgnoreFunc(TEvVolumePrivate::TEvUpdateCounters);
        IgnoreFunc(TEvVolumePrivate::TEvUpdateThrottlerState);
        IgnoreFunc(TEvVolumePrivate::TEvUpdateReadWriteClientInfo);
        IgnoreFunc(TEvVolumePrivate::TEvRemoveExpiredVolumeParams);

        IgnoreFunc(TEvStatsService::TEvVolumePartCounters);

        IgnoreFunc(TEvPartition::TEvWaitReadyResponse);

        IgnoreFunc(TEvents::TEvPoisonPill);
        IgnoreFunc(TEvents::TEvPoisonTaken);

        IgnoreFunc(TEvLocal::TEvTabletMetrics);

        IgnoreFunc(TEvBootstrapper::TEvStatus);

        IgnoreFunc(TEvPartitionCommonPrivate::TEvLongRunningOperation);

        IgnoreFunc(TEvVolumePrivate::TEvUpdateShadowDiskStateRequest);

        IgnoreFunc(TEvDiskRegistryProxy::TEvGetDrTabletInfoResponse);

        default:
            if (!RejectRequests(ev)) {
                HandleUnexpectedEvent(ev, TBlockStoreComponents::VOLUME);
            }
            break;
    }
}

////////////////////////////////////////////////////////////////////////////////

TString DescribeAllocation(const NProto::TAllocateDiskResponse& record)
{
    auto outputDevice = [] (const auto& device, TStringBuilder& out) {
        out << "("
            << device.GetDeviceUUID() << " "
            << device.GetBlocksCount() << " "
            << device.GetBlockSize()
        << ") ";
    };

    auto outputDevices = [=] (const auto& devices, TStringBuilder& out) {
        out << "[";
        if (devices.size() != 0) {
            out << " ";
        }
        for (const auto& device: devices) {
            outputDevice(device, out);
        }
        out << "]";
    };

    TStringBuilder result;
    result << "Devices ";
    outputDevices(record.GetDevices(), result);

    result << " Migrations [";
    if (record.MigrationsSize() != 0) {
        result << " ";
    }
    for (const auto& m: record.GetMigrations()) {
        result << m.GetSourceDeviceId() << " -> ";
        outputDevice(m.GetTargetDevice(), result);
    }
    result << "]";

    result << " Replicas [";
    if (record.ReplicasSize() != 0) {
        result << " ";
    }
    for (const auto& replica: record.GetReplicas()) {
        outputDevices(replica.GetDevices(), result);
        result << " ";
    }
    result << "]";

    result << " FreshDeviceIds [";
    if (record.DeviceReplacementUUIDsSize() != 0) {
        result << " ";
    }
    for (const auto& deviceId: record.GetDeviceReplacementUUIDs()) {
        result << deviceId << " ";
    }
    result << "]";

    return result;
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NCloud::NBlockStore::NStorage
