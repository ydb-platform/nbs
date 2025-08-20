#include "volume_actor.h"

#include "volume_database.h"

#include <cloud/blockstore/libs/storage/api/bootstrapper.h>
#include <cloud/blockstore/libs/storage/api/service.h>
#include <cloud/blockstore/libs/storage/api/stats_service.h>
#include <cloud/blockstore/libs/storage/api/undelivered.h>
#include <cloud/blockstore/libs/storage/core/monitoring_utils.h>
#include <cloud/blockstore/libs/storage/core/proto_helpers.h>
#include <cloud/blockstore/libs/storage/service/service_events_private.h>   // TODO: invalid reference
#include <cloud/blockstore/libs/storage/volume/model/volume_throttler_logger.h>

#include <cloud/storage/core/libs/throttling/tablet_throttler.h>
#include <cloud/storage/core/libs/throttling/tablet_throttler_logger.h>

#include <contrib/ydb/core/base/tablet_pipe.h>
#include <contrib/ydb/core/node_whiteboard/node_whiteboard.h>
#include <contrib/ydb/core/tablet/tablet_counters_aggregator.h>

#include <library/cpp/lwtrace/log_shuttle.h>
#include <library/cpp/lwtrace/protos/lwtrace.pb.h>
#include <library/cpp/lwtrace/signature.h>

#include <util/stream/str.h>
#include <util/string/builder.h>

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

const TString VolumeTransactions[] = {
#define TRANSACTION_NAME(name, ...) #name,
    BLOCKSTORE_VOLUME_TRANSACTIONS(TRANSACTION_NAME)
#undef TRANSACTION_NAME
        "Total",
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
        EVolumeStartMode startMode,
        TString diskId)
    : TActor(&TThis::StateBoot)
    , TTabletBase(owner, std::move(storage), &TransactionTimeTracker)
    , GlobalStorageConfig(config)
    , Config(std::move(config))
    , DiagnosticsConfig(std::move(diagnosticsConfig))
    , ProfileLog(std::move(profileLog))
    , BlockDigestGenerator(std::move(blockDigestGenerator))
    , TraceSerializer(std::move(traceSerializer))
    , RdmaClient(std::move(rdmaClient))
    , EndpointEventHandler(std::move(endpointEventHandler))
    , StartMode(startMode)
    , LogTitle(TabletID(), std::move(diskId), StartTime)
    , ThrottlerLogger(
          TabletID(),
          [this](ui32 opType, TDuration time)
          {
              UpdateDelayCounter(
                  static_cast<TVolumeThrottlingPolicy::EOpType>(opType),
                  time);
          })
    , TransactionTimeTracker(VolumeTransactions)
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
    ALOG_ERROR(
        TBlockStoreComponents::VOLUME,
        LogTitle.GetWithTime().c_str()
            << " IGNORING message type# " << ev->GetTypeRewrite()
            << " from Sender# " << ToString(ev->Sender) << " in StateBoot");
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
    const auto prevState = CurrentState;
    CurrentState = state;

    LOG_INFO(
        ctx,
        TBlockStoreComponents::VOLUME,
        "%s Switched state %s -> %s (system: %s, user: %s, executor: %s)",
        LogTitle.GetWithTime().c_str(),
        GetStateName(prevState).Quote().c_str(),
        GetStateName(state).Quote().c_str(),
        ToString(Tablet()).c_str(),
        ToString(SelfId()).c_str(),
        ToString(ExecutorID()).c_str());

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
    auto& tp = State->AccessThrottlingPolicy();

    const auto splittedUsedQuota = tp.TakeSplittedUsedQuota();
    const auto usedIopsQuotaPercentage = splittedUsedQuota.Iops * 100.0;
    const auto usedBandwidthQuotaPercentage =
        splittedUsedQuota.Bandwidth * 100.0;

    cumulative.UsedIopsQuota.Increment(std::round(usedIopsQuotaPercentage));
    cumulative.UsedBandwidthQuota.Increment(
        std::round(usedBandwidthQuotaPercentage));

    const auto currentSpentBudgetSharePercentage =
        100.0 * tp.CalculateCurrentSpentBudgetShare(ctx.Now());

    const auto currentRate = static_cast<ui64>(std::round(
        Min((Config->GetCalculateSplittedUsedQuotaMetric()
                 ? usedIopsQuotaPercentage + usedBandwidthQuotaPercentage
                 : currentSpentBudgetSharePercentage),
            100.0)));
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

    LogTitle.SetGeneration(Executor()->Generation());

    LOG_INFO(
        ctx,
        TBlockStoreComponents::VOLUME,
        "%s Activated executor",
        LogTitle.GetWithTime().c_str());

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
    OnDiskRegistryBasedPartitionStopped(
        ctx,
        TActorId(),
        0,   // 0 means that it's over, need to call all callbacks.
        MakeError(E_REJECTED, "tablet is shutting down"));
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

const TString& TVolumeActor::GetDiskId() const
{
    return State->GetConfig().GetDiskId();
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
    LOG_INFO(
        ctx,
        TBlockStoreComponents::VOLUME,
        "%s Partition %s stopped",
        LogTitle.GetWithTime().c_str(),
        ev->Sender.ToString().c_str());

    OnDiskRegistryBasedPartitionStopped(
        ctx,
        ev->Sender,
        ev->Cookie,
        NProto::TError());
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
    UpdateCountersScheduled = false;

    // if we use pull scheme we must send request to the partitions
    // to collect statistics
    if (Config->GetUsePullSchemeForVolumeStatistics() &&
        State && !State->IsDiskRegistryMediaKind() &&
        GetVolumeStatus() != EStatus::STATUS_INACTIVE)
    {
        ScheduleRegularUpdates(ctx);
        SendStatisticRequests(ctx);
        return;
    }

    UpdateCounters(ctx);
    ScheduleRegularUpdates(ctx);
    CleanupHistory(ctx, ev->Sender, ev->Cookie, ev->Get()->CallContext);
}

void TVolumeActor::HandleServerConnected(
    const TEvTabletPipe::TEvServerConnected::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    LOG_DEBUG(
        ctx,
        TBlockStoreComponents::VOLUME,
        "%s Pipe client %s server %s connected to volume",
        LogTitle.GetWithTime().c_str(),
        ToString(msg->ClientId).c_str(),
        ToString(msg->ServerId).c_str());
}

void TVolumeActor::HandleServerDisconnected(
    const TEvTabletPipe::TEvServerDisconnected::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();
    const auto now = ctx.Now();

    LOG_DEBUG(
        ctx,
        TBlockStoreComponents::VOLUME,
        "%s Pipe client %s server %s disconnected from volume",
        LogTitle.GetWithTime().c_str(),
        ToString(msg->ClientId).c_str(),
        ToString(msg->ServerId).c_str());

    OnServicePipeDisconnect(ctx, msg->ServerId, now);
}

void TVolumeActor::HandleServerDestroyed(
    const TEvTabletPipe::TEvServerDestroyed::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();
    const auto now = ctx.Now();

    LOG_INFO(
        ctx,
        TBlockStoreComponents::VOLUME,
        "%s Pipe client %s server %s got destroyed for volume",
        LogTitle.GetWithTime().c_str(),
        ToString(msg->ClientId).c_str(),
        ToString(msg->ServerId).c_str());

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

void TVolumeActor::HandleTakeVolumeRequestId(
    const TEvVolumePrivate::TEvTakeVolumeRequestIdRequest::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    if (!VolumeRequestIdGenerator->CanAdvance()) {
        auto resp =
            std::make_unique<TEvVolumePrivate::TEvTakeVolumeRequestIdResponse>(
                MakeError(
                    E_REJECTED,
                    "volume request id overflow, restarting tablet"));
        NCloud::Reply(ctx, *ev, std::move(resp));

        NCloud::Send(ctx, SelfId(), std::make_unique<TEvents::TEvPoisonPill>());
        LOG_WARN(
            ctx,
            TBlockStoreComponents::VOLUME,
            "%s Sent PoisonPill to volume because of volume request id overflow",
            LogTitle.GetWithTime().c_str());
        return;
    }

    auto resp =
        std::make_unique<TEvVolumePrivate::TEvTakeVolumeRequestIdResponse>(
            VolumeRequestIdGenerator->Advance());
    NCloud::Reply(ctx, *ev, std::move(resp));
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
            TEvVolumePrivate::TEvUpdateFollowerStateRequest,
            HandleUpdateFollowerState);

        HFunc(
            TEvDiskRegistryProxy::TEvGetDrTabletInfoResponse,
            HandleGetDrTabletInfoResponse);

        BLOCKSTORE_HANDLE_REQUEST(WaitReady, TEvVolume)

        default:
            if (!RejectRequests(ev) && !HandleDefaultEvents(ev, SelfId())) {
                HandleUnexpectedEvent(
                    ev,
                    TBlockStoreComponents::VOLUME,
                    __PRETTY_FUNCTION__);
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
        HFunc(TEvVolume::TEvScrubberCounters, HandleScrubberCounters);
        HFunc(
            TEvVolumePrivate::TEvWriteOrZeroCompleted,
            HandleWriteOrZeroCompleted);
        HFunc(
            TEvDiskRegistry::TEvAcquireDiskResponse,
            HandleAcquireDiskResponse);
        HFunc(
            TEvVolumePrivate::TEvDevicesAcquireFinished,
            HandleDevicesAcquireFinished);
        HFunc(
            TEvVolumePrivate::TEvAcquireDiskIfNeeded,
            HandleAcquireDiskIfNeeded);
        HFunc(TEvVolume::TEvReacquireDisk, HandleReacquireDisk);
        HFunc(TEvVolume::TEvRdmaUnavailable, HandleRdmaUnavailable);
        HFunc(
            TEvDiskRegistry::TEvReleaseDiskResponse,
            HandleReleaseDiskResponse);
        HFunc(
            TEvVolumePrivate::TEvDevicesReleaseFinished,
            HandleDevicesReleasedFinished);
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

        HFunc(TEvVolume::TEvPreparePartitionMigrationRequest, HandlePreparePartitionMigration);

        HFunc(TEvVolume::TEvUpdateMigrationState, HandleUpdateMigrationState);
        HFunc(TEvVolume::TEvUpdateResyncState, HandleUpdateResyncState);
        HFunc(TEvVolume::TEvResyncFinished, HandleResyncFinished);

        HFunc(
            TEvDiskRegistry::TEvAddOutdatedLaggingDevicesResponse,
            HandleAddOutdatedLaggingDevicesResponse);
        HFunc(
            TEvVolumePrivate::TEvReportOutdatedLaggingDevicesToDR,
            HandleReportLaggingDevicesToDR);
        HFunc(
            TEvVolumePrivate::TEvDeviceTimedOutRequest,
            HandleDeviceTimedOut);
        HFunc(
            TEvVolumePrivate::TEvUpdateLaggingAgentMigrationState,
            HandleUpdateLaggingAgentMigrationState);
        HFunc(
            TEvVolumePrivate::TEvLaggingAgentMigrationFinished,
            HandleLaggingAgentMigrationFinished);

        HFunc(
            TEvPartitionCommonPrivate::TEvLongRunningOperation,
            HandleLongRunningBlobOperation);

        HFunc(
            TEvVolumePrivate::TEvUpdateShadowDiskStateRequest,
            HandleUpdateShadowDiskState);

        HFunc(
            TEvVolumePrivate::TEvUpdateFollowerStateRequest,
            HandleUpdateFollowerState);
        HFunc(
            TEvVolumePrivate::TEvCreateLinkFinished,
            HandleCreateLinkFinished);
        HFunc(
            TEvVolumePrivate::TEvLinkOnFollowerDestroyed,
            HandleLinkOnFollowerDestroyed);

        HFunc(
            TEvDiskRegistryProxy::TEvGetDrTabletInfoResponse,
            HandleGetDrTabletInfoResponse);

        HFunc(
            TEvPartitionCommonPrivate::TEvPartCountersCombined,
            HandlePartCountersCombined);

        HFunc(
            TEvVolumeThrottlingManager::TEvVolumeThrottlingConfigNotification,
            HandleUpdateVolatileThrottlingConfig);

        IgnoreFunc(TEvLocal::TEvTabletMetrics);

        default:
            if (!HandleRequests(ev) && !HandleDefaultEvents(ev, SelfId())) {
                HandleUnexpectedEvent(
                    ev,
                    TBlockStoreComponents::VOLUME,
                    __PRETTY_FUNCTION__);
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

        IgnoreFunc(TEvVolumePrivate::TEvAllocateDiskIfNeeded);
        IgnoreFunc(TEvVolumePrivate::TEvUpdateCounters);
        IgnoreFunc(TEvVolumePrivate::TEvUpdateThrottlerState);
        IgnoreFunc(TEvVolumePrivate::TEvUpdateReadWriteClientInfo);
        IgnoreFunc(TEvVolumePrivate::TEvRemoveExpiredVolumeParams);
        IgnoreFunc(TEvVolumePrivate::TEvReportOutdatedLaggingDevicesToDR);
        IgnoreFunc(TEvVolumePrivate::TEvDeviceTimedOutRequest);
        IgnoreFunc(TEvVolumePrivate::TEvAcquireDiskIfNeeded);
        IgnoreFunc(TEvVolumePrivate::TEvUpdateLaggingAgentMigrationState);
        IgnoreFunc(TEvVolumePrivate::TEvLaggingAgentMigrationFinished);

        IgnoreFunc(TEvStatsService::TEvVolumePartCounters);

        IgnoreFunc(TEvPartition::TEvWaitReadyResponse);

        HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);
        HFunc(TEvents::TEvPoisonTaken, HandlePoisonTaken);
        HFunc(TEvTablet::TEvTabletStop, HandleTabletStop);

        IgnoreFunc(TEvLocal::TEvTabletMetrics);

        IgnoreFunc(TEvBootstrapper::TEvStatus);

        IgnoreFunc(TEvPartitionCommonPrivate::TEvLongRunningOperation);

        IgnoreFunc(TEvVolumePrivate::TEvUpdateShadowDiskStateRequest);

        IgnoreFunc(TEvDiskRegistryProxy::TEvGetDrTabletInfoResponse);

        IgnoreFunc(TEvDiskRegistry::TEvAddOutdatedLaggingDevicesResponse);

        IgnoreFunc(TEvVolumePrivate::TEvCreateLinkFinished);
        IgnoreFunc(TEvVolumePrivate::TEvLinkOnFollowerDestroyed);
        IgnoreFunc(TEvVolumePrivate::TEvUpdateFollowerStateRequest);
        IgnoreFunc(TEvVolume::TEvLinkLeaderVolumeToFollowerRequest);
        IgnoreFunc(TEvVolume::TEvUnlinkLeaderVolumeFromFollowerRequest);
        IgnoreFunc(TEvVolume::TEvUpdateLinkOnFollowerResponse);

        IgnoreFunc(TEvPartitionCommonPrivate::TEvPartCountersCombined);

        default:
            if (!RejectRequests(ev)) {
                HandleUnexpectedEvent(
                    ev,
                    TBlockStoreComponents::VOLUME,
                    __PRETTY_FUNCTION__);
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
