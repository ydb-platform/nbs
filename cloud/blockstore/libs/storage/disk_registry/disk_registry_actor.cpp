#include "disk_registry_actor.h"

#include <cloud/blockstore/libs/diagnostics/critical_events.h>
#include <cloud/blockstore/libs/storage/api/disk_agent.h>

#include <contrib/ydb/core/base/appdata.h>
#include <contrib/ydb/core/base/tablet_pipe.h>
#include <contrib/ydb/core/mon/mon.h>
#include <contrib/ydb/core/node_whiteboard/node_whiteboard.h>

#include <util/datetime/base.h>
#include <util/stream/file.h>
#include <util/string/join.h>
#include <util/system/file.h>

#include <google/protobuf/util/json_util.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

using namespace NKikimr;

////////////////////////////////////////////////////////////////////////////////

const TDiskRegistryActor::TStateInfo TDiskRegistryActor::States[STATE_MAX] = {
    { "Boot",     (IActor::TReceiveFunc)&TDiskRegistryActor::StateBoot     },
    { "Init",     (IActor::TReceiveFunc)&TDiskRegistryActor::StateInit     },
    { "Work",     (IActor::TReceiveFunc)&TDiskRegistryActor::StateWork     },
    { "Restore",  (IActor::TReceiveFunc)&TDiskRegistryActor::StateRestore  },
    { "ReadOnly", (IActor::TReceiveFunc)&TDiskRegistryActor::StateReadOnly },
    { "Zombie",   (IActor::TReceiveFunc)&TDiskRegistryActor::StateZombie   },
};

TDiskRegistryActor::TDiskRegistryActor(
        const TActorId& owner,
        TTabletStorageInfoPtr storage,
        TStorageConfigPtr config,
        TDiagnosticsConfigPtr diagnosticsConfig,
        TLogbrokerServicePtr logbrokerService,
        NNotify::IServicePtr notifyService,
        ILoggingServicePtr logging)
    : TActor(&TThis::StateBoot)
    , TTabletBase(owner, std::move(storage))
    , Config(std::move(config))
    , DiagnosticsConfig(std::move(diagnosticsConfig))
    , LogbrokerService(std::move(logbrokerService))
    , NotifyService(std::move(notifyService))
    , Logging(std::move(logging))
{}

TDiskRegistryActor::~TDiskRegistryActor()
{}

TString TDiskRegistryActor::GetStateName(ui32 state)
{
    if (state < STATE_MAX) {
        return States[state].Name;
    }
    return "<unknown>";
}

void TDiskRegistryActor::ScheduleMakeBackup(
    const NActors::TActorContext& ctx,
    TInstant lastBackupTs)
{
    const auto backupDirPath = Config->GetDiskRegistryBackupDirPath();

    if (backupDirPath.empty()) {
        LOG_WARN(ctx, TBlockStoreComponents::DISK_REGISTRY,
            "Path for backups was not specified");
        return;
    }

    const auto backupPeriod = Config->GetDiskRegistryBackupPeriod()
        - (ctx.Now() - lastBackupTs);

    LOG_DEBUG_S(ctx, TBlockStoreComponents::DISK_REGISTRY,
        "Schedule backup at " << backupPeriod.ToDeadLine(ctx.Now()));

    TString hostPrefix = Config->GetDiskRegistryCountersHost();
    if (!hostPrefix.empty()) {
        hostPrefix  += "-";
    }
    auto request =
        std::make_unique<TEvDiskRegistry::TEvBackupDiskRegistryStateRequest>();
    request->Record.SetBackupLocalDB(true);
    request->Record.SetBackupFilePath(TStringBuilder()
        << backupDirPath << "/" + hostPrefix << FormatIsoLocal(ctx.Now()) << ".json");

    ctx.Schedule(
        backupPeriod,
        std::make_unique<IEventHandle>(
            ctx.SelfID,
            ctx.SelfID,
            request.release()));
}

void TDiskRegistryActor::ScheduleCleanup(const TActorContext& ctx)
{
    const auto recyclingPeriod = Config->GetNonReplicatedDiskRecyclingPeriod();

    LOG_DEBUG_S(ctx, TBlockStoreComponents::DISK_REGISTRY,
        "Schedule cleanup at " << recyclingPeriod.ToDeadLine(ctx.Now()));

    auto request = std::make_unique<TEvDiskRegistryPrivate::TEvCleanupDisksRequest>();

    ctx.Schedule(
        recyclingPeriod,
        std::make_unique<IEventHandle>(
            ctx.SelfID,
            ctx.SelfID,
            request.release()));
}

void TDiskRegistryActor::BecomeAux(const TActorContext& ctx, EState state)
{
    Y_DEBUG_ABORT_UNLESS(state < STATE_MAX);

    Become(States[state].Func);
    CurrentState = state;

    LOG_DEBUG(ctx, TBlockStoreComponents::DISK_REGISTRY,
        "[%lu] Switched to state %s (system: %s, user: %s, executor: %s)",
        TabletID(),
        States[state].Name.data(),
        ToString(Tablet()).data(),
        ToString(SelfId()).data(),
        ToString(ExecutorID()).data());

    ReportTabletState(ctx);
}

void TDiskRegistryActor::ReportTabletState(const TActorContext& ctx)
{
    auto service = NNodeWhiteboard::MakeNodeWhiteboardServiceId(SelfId().NodeId());

    auto request = std::make_unique<NNodeWhiteboard::TEvWhiteboard::TEvTabletStateUpdate>(
        TabletID(),
        CurrentState);

    NCloud::Send(ctx, service, std::move(request));
}

void TDiskRegistryActor::DefaultSignalTabletActive(const TActorContext&)
{
    // must be empty
}

void TDiskRegistryActor::OnActivateExecutor(const TActorContext& ctx)
{
    RegisterCounters(ctx);

    if (!Executor()->GetStats().IsFollower) {
        ExecuteTx<TInitSchema>(ctx);
    } else {
        SignalTabletActive(ctx);
    }

    BecomeAux(ctx, STATE_INIT);
}

bool TDiskRegistryActor::OnRenderAppHtmlPage(
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

void TDiskRegistryActor::BeforeDie(const NActors::TActorContext& ctx)
{
    UnregisterCounters(ctx);
    KillActors(ctx);
    CancelPendingRequests(ctx, PendingRequests);

    for (auto& [diskId, requestInfos]: PendingDiskDeallocationRequests) {
        ReplyToPendingDeallocations(
            ctx,
            requestInfos,
            MakeTabletIsDeadError(E_REJECTED, __LOCATION__));
    }
    PendingDiskDeallocationRequests.clear();
}

void TDiskRegistryActor::OnDetach(const TActorContext& ctx)
{
    Counters = nullptr;

    BeforeDie(ctx);
    Die(ctx);
}

void TDiskRegistryActor::OnTabletDead(
    TEvTablet::TEvTabletDead::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    BeforeDie(ctx);
    Die(ctx);
}

void TDiskRegistryActor::RegisterCounters(const TActorContext& ctx)
{
    if (!Counters) {
        auto counters = CreateDiskRegistryCounters();

        // LAME: ownership transferred to executor
        Counters = counters.get();
        Executor()->RegisterExternalTabletCounters(counters.release());

        // only aggregated statistics will be reported by default
        // (you can always turn on per-tablet statistics on monitoring page)
        // TabletCountersAddTablet(TabletID(), ctx);

        ScheduleWakeup(ctx);
    }

    if (auto counters = AppData(ctx)->Counters) {
        ComponentGroup = counters
            ->GetSubgroup("counters", "blockstore")
            ->GetSubgroup("component", "disk_registry");

        if (Config->GetDiskRegistryCountersHost()) {
            ComponentGroup = ComponentGroup->GetSubgroup(
                "host",
                Config->GetDiskRegistryCountersHost());
        }
    }
}

void TDiskRegistryActor::ScheduleWakeup(const TActorContext& ctx)
{
    ctx.Schedule(UpdateCountersInterval, new TEvents::TEvWakeup());
}

void TDiskRegistryActor::UpdateCounters(const TActorContext& ctx)
{
    if (State) {
        State->PublishCounters(ctx.Now());
    }
}

void TDiskRegistryActor::UpdateActorStats(const TActorContext& ctx)
{
    if (Counters) {
        auto& actorQueue = Counters->Percentile()[TDiskRegistryCounters::PERCENTILE_COUNTER_Actor_ActorQueue];
        auto& mailboxQueue = Counters->Percentile()[TDiskRegistryCounters::PERCENTILE_COUNTER_Actor_MailboxQueue];

        auto actorQueues = ctx.CountMailboxEvents(1001);
        actorQueue.IncrementFor(actorQueues.first);
        mailboxQueue.IncrementFor(actorQueues.second);
    }
}

void TDiskRegistryActor::KillActors(const TActorContext& ctx)
{
    for (auto& actor: Actors) {
        NCloud::Send<TEvents::TEvPoisonPill>(ctx, actor);
    }
}

void TDiskRegistryActor::UnregisterCounters(const TActorContext& ctx)
{
    auto counters = AppData(ctx)->Counters;

    if (counters) {
        counters
            ->GetSubgroup("counters", "blockstore")
            ->RemoveSubgroup("component", "disk_registry");
    }
}

void TDiskRegistryActor::ScheduleDiskRegistryAgentListExpiredParamsCleanup(
    const NActors::TActorContext& ctx)
{
    ctx.Schedule(
        Config->GetAgentListExpiredParamsCleanupInterval(),
        new TEvDiskRegistryPrivate::TEvDiskRegistryAgentListExpiredParamsCleanup());
}

////////////////////////////////////////////////////////////////////////////////

void TDiskRegistryActor::HandlePoisonPill(
    const TEvents::TEvPoisonPill::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    NCloud::Send<TEvents::TEvPoisonPill>(ctx, Tablet());
    BecomeAux(ctx, STATE_ZOMBIE);
}

void TDiskRegistryActor::HandleWakeup(
    const TEvents::TEvWakeup::TPtr& ev,
    const TActorContext& ctx)
{
    ProcessAutomaticallyReplacedDevices(ctx);
    HandleWakeupReadOnly(ev, ctx);
}

void TDiskRegistryActor::HandleWakeupReadOnly(
    const TEvents::TEvWakeup::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    UpdateCounters(ctx);
    ScheduleWakeup(ctx);
}

bool TDiskRegistryActor::HandleRequests(STFUNC_SIG)
{
    switch (ev->GetTypeRewrite()) {
        BLOCKSTORE_DISK_REGISTRY_REQUESTS(BLOCKSTORE_HANDLE_REQUEST, TEvDiskRegistry)
        BLOCKSTORE_DISK_REGISTRY_REQUESTS_FWD_SERVICE(BLOCKSTORE_HANDLE_REQUEST, TEvService)
        BLOCKSTORE_DISK_REGISTRY_REQUESTS_PRIVATE(BLOCKSTORE_HANDLE_REQUEST, TEvDiskRegistryPrivate)

        default:
            return false;
    }

    return true;
}

bool TDiskRegistryActor::RejectRequests(STFUNC_SIG)
{
    switch (ev->GetTypeRewrite()) {
        BLOCKSTORE_DISK_REGISTRY_REQUESTS(BLOCKSTORE_REJECT_REQUEST, TEvDiskRegistry)
        BLOCKSTORE_DISK_REGISTRY_REQUESTS_FWD_SERVICE(BLOCKSTORE_REJECT_REQUEST, TEvService)
        BLOCKSTORE_DISK_REGISTRY_REQUESTS_PRIVATE(BLOCKSTORE_REJECT_REQUEST, TEvDiskRegistryPrivate)

        default:
            return false;
    }

    return true;
}

////////////////////////////////////////////////////////////////////////////////

void TDiskRegistryActor::HandleBackupDiskRegistryStateResponse(
    const TEvDiskRegistry::TEvBackupDiskRegistryStateResponse::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    auto* msg = ev->Get();

    if (HasError(msg->GetError())) {
        LOG_ERROR(ctx, TBlockStoreComponents::DISK_REGISTRY,
            "Backup error %s",
            msg->GetError().GetMessage().c_str());
    } else {
        const TString filePath = msg->Record.GetBackupFilePath();
        if (!filePath.empty()) {
            try {
                if (!msg->Record.GetBackup().GetConfig().ByteSize()) {
                    LOG_WARN(
                        ctx, TBlockStoreComponents::DISK_REGISTRY,
                        "The backup file is not created "
                        "because the configuration is empty");
                } else {
                    TProtoStringType str;
                    google::protobuf::util::MessageToJsonString(
                        msg->Record,
                        &str);
                    TFileOutput(filePath).Write(str.c_str());
                }
            } catch(...) {
                LOG_ERROR_S(ctx, TBlockStoreComponents::DISK_REGISTRY,
                    ReportDiskRegistryBackupFailed(
                        TStringBuilder()
                        << "Can't create backup file "
                        << filePath.Quote()
                        << " : " << CurrentExceptionMessage().Quote()));
            }
        }
    }

    ScheduleMakeBackup(ctx, ctx.Now());
}

void TDiskRegistryActor::HandleServerConnected(
    const TEvTabletPipe::TEvServerConnected::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ctx);
    auto* msg = ev->Get();
    auto [it, inserted] = ServerToAgentId.emplace(msg->ServerId, TString());
    Y_DEBUG_ABORT_UNLESS(inserted);
}

void TDiskRegistryActor::HandleServerDisconnected(
    const TEvTabletPipe::TEvServerDisconnected::TPtr& ev,
    const TActorContext& ctx)
{
    Y_ABORT_UNLESS(State);

    auto* msg = ev->Get();

    auto it = ServerToAgentId.find(msg->ServerId);
    if (it == ServerToAgentId.end()) {
        return;
    }

    const auto& agentId = it->second;

    if (agentId) {
        auto& info = AgentRegInfo[agentId];
        info.Connected = false;

        LOG_WARN_S(ctx, TBlockStoreComponents::DISK_REGISTRY,
            "Agent " << agentId.Quote()
            << " disconnected, SeqNo=" << info.SeqNo);

        ScheduleRejectAgent(ctx, agentId, info.SeqNo);
        State->OnAgentDisconnected(ctx.Now(), agentId);
    }

    ServerToAgentId.erase(it);
}

void TDiskRegistryActor::ScheduleRejectAgent(
    const NActors::TActorContext& ctx,
    TString agentId,
    ui64 seqNo)
{
    auto timeout = State->GetRejectAgentTimeout(ctx.Now(), agentId);

    if (!timeout) {
        return;
    }

    auto deadline = timeout.ToDeadLine(ctx.Now());
    LOG_INFO_S(ctx, TBlockStoreComponents::DISK_REGISTRY,
        "Schedule reject agent " << agentId.Quote() << ": " << ctx.Now()
        << " -> " << deadline);

    auto request = std::make_unique<TEvDiskRegistryPrivate::TEvAgentConnectionLost>(
        std::move(agentId), seqNo);

    ctx.Schedule(deadline, request.release());
}

void TDiskRegistryActor::ProcessInitialAgentRejectionPhase(
    const TActorContext& ctx)
{
    LOG_INFO_S(
        ctx,
        TBlockStoreComponents::DISK_REGISTRY,
        "Process the initial agents rejection phase");

    ui32 expectedToBeOnline = 0;
    TVector<TString> agentsToReject;

    for (const auto& agent: State->GetAgents()) {
        if (agent.GetState() == NProto::AGENT_STATE_UNAVAILABLE) {
            continue;
        }
        ++expectedToBeOnline;

        const auto& agentId = agent.GetAgentId();

        if (!AgentRegInfo.contains(agentId)) {
            agentsToReject.push_back(agentId);
        }
    }

    if (agentsToReject.empty() || !expectedToBeOnline) {
        return;
    }

    const double k =
        100.0 * static_cast<double>(agentsToReject.size()) / expectedToBeOnline;

    if (k > Config->GetDiskRegistryInitialAgentRejectionThreshold()) {
        ReportDiskRegistryInitialAgentRejectionThresholdExceeded(
            TStringBuilder()
            << "Too many agents haven't reconnected: " << agentsToReject.size()
            << "/" << expectedToBeOnline);
        return;
    }

    for (const auto& agentId: agentsToReject) {
        NCloud::Send(
            ctx,
            ctx.SelfID,
            std::make_unique<TEvDiskRegistryPrivate::TEvAgentConnectionLost>(
                agentId,
                0));
    }
}

void TDiskRegistryActor::HandleAgentConnectionLost(
    const TEvDiskRegistryPrivate::TEvAgentConnectionLost::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    if (msg->AgentId.empty()) {
        ProcessInitialAgentRejectionPhase(ctx);
        return;
    }

    auto it = AgentRegInfo.find(msg->AgentId);
    if (it != AgentRegInfo.end() && msg->SeqNo < it->second.SeqNo) {
        LOG_DEBUG_S(ctx, TBlockStoreComponents::DISK_REGISTRY,
            "Agent " << msg->AgentId.Quote() << " is connected: "
            << msg->SeqNo << " < SeqNo " << it->second.SeqNo);

        return;
    }

    LOG_WARN_S(ctx, TBlockStoreComponents::DISK_REGISTRY,
        "Reject agent " << msg->AgentId.Quote());

    auto request =
        std::make_unique<TEvDiskRegistry::TEvChangeAgentStateRequest>();
    request->Record.SetAgentId(msg->AgentId);
    request->Record.SetAgentState(NProto::AGENT_STATE_UNAVAILABLE);
    request->Record.SetReason("connection lost");

    NCloud::Send(ctx, ctx.SelfID, std::move(request));
}

void TDiskRegistryActor::HandleAgentConnectionLostReadOnly(
    const TEvDiskRegistryPrivate::TEvAgentConnectionLost::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    LOG_INFO_S(ctx, TBlockStoreComponents::DISK_REGISTRY,
        "Rescheduling EvAgentConnectionLost, AgentId=" << msg->AgentId.Quote());
    ctx.Schedule(TDuration::Seconds(10), ev->Release().Release());
}

void TDiskRegistryActor::ScheduleSwitchAgentDisksToReadOnly(
    const NActors::TActorContext& ctx,
    TString agentId)
{
    // Switching disk of unknown agent will result in error.
    if (State->GetAgentState(agentId).Empty()) {
        return;
    }
    auto timeout = Config->GetNonReplicatedDiskSwitchToReadOnlyTimeout();

    auto deadline = timeout.ToDeadLine(ctx.Now());
    LOG_INFO_S(ctx, TBlockStoreComponents::DISK_REGISTRY,
        "Scheduling switch to ReadOnly for disks associated with agent "
        << agentId << "  " << ctx.Now() << " -> " << deadline);

    auto request = std::make_unique<
        TEvDiskRegistryPrivate::TEvSwitchAgentDisksToReadOnlyRequest>(
        std::move(agentId));

    // ctx.Schedule does not set `sender`, but we need `sender` to be able to
    // intercept TEvSwitchAgentDisksToReadOnlyResponse in tests
    ctx.Schedule(
        deadline,
        std::make_unique<IEventHandle>(
            ctx.SelfID,   // recipient
            ctx.SelfID,   // sender
            request.release()));
}

void TDiskRegistryActor::HandleSwitchAgentDisksToReadOnlyReshedule(
    const TEvDiskRegistryPrivate::TEvSwitchAgentDisksToReadOnlyRequest::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    LOG_INFO_S(ctx, TBlockStoreComponents::DISK_REGISTRY,
        "Rescheduling EvSwitchAgentDisksToReadOnlyRequest, AgentId=" << msg->AgentId.Quote());
    ctx.Schedule(TDuration::Seconds(10), ev->Release().Release());
}

void TDiskRegistryActor::HandleOperationCompleted(
    const TEvDiskRegistryPrivate::TEvOperationCompleted::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    Y_UNUSED(ctx);

    Actors.erase(ev->Sender);
}

////////////////////////////////////////////////////////////////////////////////

STFUNC(TDiskRegistryActor::StateBoot)
{
    UpdateActorStatsSampled(ActorContext());
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);
        HFunc(TEvents::TEvWakeup, HandleWakeup);

        HFunc(TEvTabletPipe::TEvServerConnected, HandleServerConnected);
        HFunc(TEvTabletPipe::TEvServerDisconnected, HandleServerDisconnected);
        HFunc(TEvDiskRegistryPrivate::TEvAgentConnectionLost,
            HandleAgentConnectionLostReadOnly);
        HFunc(TEvDiskRegistryPrivate::TEvSwitchAgentDisksToReadOnlyRequest,
            HandleSwitchAgentDisksToReadOnlyReshedule);

        BLOCKSTORE_HANDLE_REQUEST(WaitReady, TEvDiskRegistry)

        default:
            StateInitImpl(ev, SelfId());
            break;
    }
}

STFUNC(TDiskRegistryActor::StateInit)
{
    UpdateActorStatsSampled(ActorContext());
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);
        HFunc(TEvents::TEvWakeup, HandleWakeup);

        HFunc(TEvTabletPipe::TEvServerConnected, HandleServerConnected);
        HFunc(TEvTabletPipe::TEvServerDisconnected, HandleServerDisconnected);
        HFunc(TEvDiskRegistryPrivate::TEvAgentConnectionLost,
            HandleAgentConnectionLostReadOnly);
        HFunc(TEvDiskRegistryPrivate::TEvSwitchAgentDisksToReadOnlyRequest,
            HandleSwitchAgentDisksToReadOnlyReshedule);

        IgnoreFunc(
            TEvDiskRegistryPrivate::TEvSwitchAgentDisksToReadOnlyResponse);

        BLOCKSTORE_HANDLE_REQUEST(WaitReady, TEvDiskRegistry)

        default:
            if (!RejectRequests(ev) && !HandleDefaultEvents(ev, SelfId())) {
                HandleUnexpectedEvent(
                    ev,
                    TBlockStoreComponents::DISK_REGISTRY,
                    __PRETTY_FUNCTION__);
            }
            break;
    }
}

STFUNC(TDiskRegistryActor::StateWork)
{
    UpdateActorStatsSampled(ActorContext());
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);
        HFunc(TEvents::TEvWakeup, HandleWakeup);

        HFunc(TEvTabletPipe::TEvServerConnected, HandleServerConnected);
        HFunc(TEvTabletPipe::TEvServerDisconnected, HandleServerDisconnected);
        HFunc(TEvDiskRegistryPrivate::TEvAgentConnectionLost,
            HandleAgentConnectionLost);

        IgnoreFunc(TEvDiskRegistry::TEvReleaseDiskResponse);
        IgnoreFunc(TEvDiskRegistry::TEvUnregisterAgentResponse);

        IgnoreFunc(TEvDiskRegistry::TEvChangeAgentStateResponse);
        IgnoreFunc(TEvDiskRegistry::TEvChangeDeviceStateResponse);
        IgnoreFunc(TEvDiskAgent::TEvDisableConcreteAgentResponse);

        IgnoreFunc(TEvDiskRegistryPrivate::TEvCleanupDevicesResponse);

        IgnoreFunc(TEvDiskRegistryPrivate::TEvRestoreDiskRegistryPartRequest);
        IgnoreFunc(TEvDiskRegistryPrivate::TEvRestoreDiskRegistryPartResponse);

        IgnoreFunc(TEvDiskAgent::TEvAcquireDevicesResponse);

        IgnoreFunc(
            TEvDiskRegistryPrivate::TEvSwitchAgentDisksToReadOnlyResponse);

        HFunc(TEvDiskRegistry::TEvBackupDiskRegistryStateResponse,
            HandleBackupDiskRegistryStateResponse);

        HFunc(TEvDiskRegistryPrivate::TEvCleanupDisksResponse,
            HandleCleanupDisksResponse);

        HFunc(TEvDiskRegistryPrivate::TEvSecureEraseResponse,
            HandleSecureEraseResponse);

        HFunc(TEvDiskRegistryPrivate::TEvDestroyBrokenDisksResponse,
            HandleDestroyBrokenDisksResponse);

        HFunc(TEvDiskRegistryPrivate::TEvStartMigrationResponse,
            HandleStartMigrationResponse);

        HFunc(TEvDiskRegistryPrivate::TEvNotifyDisksResponse,
            HandleNotifyDisksResponse);

        HFunc(TEvDiskRegistryPrivate::TEvNotifyUsersResponse,
            HandleNotifyUsersResponse);

        HFunc(TEvDiskRegistryPrivate::TEvPublishDiskStatesResponse,
            HandlePublishDiskStatesResponse);

        HFunc(TEvDiskRegistryPrivate::TEvOperationCompleted,
            HandleOperationCompleted);

        HFunc(TEvDiskRegistryPrivate::TEvUpdateVolumeConfigResponse,
            HandleUpdateVolumeConfigResponse);

        HFunc(TEvDiskRegistryPrivate::TEvRestoreDiskRegistryValidationResponse,
            HandleRestoreDiskRegistryValidationResponse);

        HFunc(TEvDiskAgent::TEvEnableAgentDeviceResponse,
            HandleEnableDeviceResponse);

        HFunc(
            TEvDiskRegistryPrivate::TEvDiskRegistryAgentListExpiredParamsCleanup,
            TDiskRegistryActor::HandleDiskRegistryAgentListExpiredParamsCleanup);

        default:
            if (!HandleRequests(ev) && !HandleDefaultEvents(ev, SelfId())) {
                HandleUnexpectedEvent(
                    ev,
                    TBlockStoreComponents::DISK_REGISTRY,
                    __PRETTY_FUNCTION__);
            }
            break;
    }
}

STFUNC(TDiskRegistryActor::StateRestore)
{
    UpdateActorStatsSampled(ActorContext());
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvents::TEvWakeup, HandleWakeupReadOnly);

        HFunc(TEvTabletPipe::TEvServerConnected, HandleServerConnected);
        HFunc(TEvTabletPipe::TEvServerDisconnected, HandleServerDisconnected);
        HFunc(TEvDiskRegistryPrivate::TEvAgentConnectionLost,
            HandleAgentConnectionLostReadOnly);
        HFunc(TEvDiskRegistryPrivate::TEvSwitchAgentDisksToReadOnlyRequest,
            HandleSwitchAgentDisksToReadOnlyReshedule);

        HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);
        HFunc(TEvTablet::TEvTabletDead, HandleTabletDead);
        HFunc(NMon::TEvRemoteHttpInfo, RenderHtmlPage);

        HFunc(
            TEvDiskRegistry::TEvBackupDiskRegistryStateRequest,
            HandleBackupDiskRegistryState);
        HFunc(
            TEvDiskRegistry::TEvRestoreDiskRegistryStateRequest,
            HandleRestoreDiskRegistryState);
        HFunc(
            TEvDiskRegistryPrivate::TEvRestoreDiskRegistryPartRequest,
            HandleRestoreDiskRegistryPart);
        HFunc(
            TEvDiskRegistryPrivate::TEvRestoreDiskRegistryPartResponse,
            HandleRestoreDiskRegistryPartResponse);

        HFunc(
            TEvDiskRegistryPrivate::TEvRestoreDiskRegistryValidationResponse,
            HandleRestoreDiskRegistryValidationResponse);

        HFunc(
            TEvDiskRegistryPrivate::TEvDiskRegistryAgentListExpiredParamsCleanup,
            TDiskRegistryActor::HandleDiskRegistryAgentListExpiredParamsCleanupReadOnly);

        IgnoreFunc(TEvDiskRegistryPrivate::TEvSwitchAgentDisksToReadOnlyResponse);

        default:
            if (!RejectRequests(ev)) {
                LogUnexpectedEvent(
                    ev,
                    TBlockStoreComponents::DISK_REGISTRY,
                    __PRETTY_FUNCTION__);
            }
            break;
    }
}

STFUNC(TDiskRegistryActor::StateReadOnly)
{
    UpdateActorStatsSampled(ActorContext());
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvents::TEvWakeup, HandleWakeupReadOnly);

        HFunc(TEvTabletPipe::TEvServerConnected, HandleServerConnected);
        HFunc(TEvTabletPipe::TEvServerDisconnected, HandleServerDisconnected);
        HFunc(TEvDiskRegistryPrivate::TEvAgentConnectionLost,
            HandleAgentConnectionLostReadOnly);
        HFunc(TEvDiskRegistryPrivate::TEvSwitchAgentDisksToReadOnlyRequest,
            HandleSwitchAgentDisksToReadOnlyReshedule);

        HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);
        HFunc(TEvTablet::TEvTabletDead, HandleTabletDead);
        HFunc(NMon::TEvRemoteHttpInfo, RenderHtmlPage);

        HFunc(TEvDiskRegistry::TEvWaitReadyRequest, HandleWaitReady);

        HFunc(TEvDiskRegistry::TEvSetWritableStateRequest,
            HandleSetWritableState);

        HFunc(
            TEvDiskRegistry::TEvBackupDiskRegistryStateRequest,
            HandleBackupDiskRegistryState);
        HFunc(TEvDiskRegistry::TEvBackupDiskRegistryStateResponse,
            HandleBackupDiskRegistryStateResponse);

        HFunc(
            TEvDiskRegistry::TEvRestoreDiskRegistryStateRequest,
            HandleRestoreDiskRegistryState);
        HFunc(
            TEvDiskRegistryPrivate::TEvRestoreDiskRegistryValidationResponse,
            HandleRestoreDiskRegistryValidationResponse);
        HFunc(
            TEvDiskRegistry::TEvDescribeDiskRequest,
            HandleDescribeDisk);
        HFunc(
            TEvDiskRegistry::TEvDescribeConfigRequest,
            HandleDescribeConfig);
        HFunc(
            TEvService::TEvDescribePlacementGroupRequest,
            HandleDescribePlacementGroup);
        HFunc(
            TEvService::TEvQueryAvailableStorageRequest,
            HandleQueryAvailableStorage);

        HFunc(
            TEvDiskRegistryPrivate::TEvDiskRegistryAgentListExpiredParamsCleanup,
            TDiskRegistryActor::HandleDiskRegistryAgentListExpiredParamsCleanupReadOnly);

        HFunc(TEvDiskRegistryPrivate::TEvCleanupDisksResponse,
            HandleCleanupDisksResponse);

        IgnoreFunc(
            TEvDiskRegistryPrivate::TEvSwitchAgentDisksToReadOnlyResponse);

        default:
            if (!RejectRequests(ev)) {
                LogUnexpectedEvent(
                    ev,
                    TBlockStoreComponents::DISK_REGISTRY,
                    __PRETTY_FUNCTION__);
            }
            break;
    }
}

STFUNC(TDiskRegistryActor::StateZombie)
{
    UpdateActorStatsSampled(ActorContext());
    switch (ev->GetTypeRewrite()) {
        IgnoreFunc(TEvents::TEvPoisonPill);
        IgnoreFunc(TEvents::TEvWakeup);

        HFunc(TEvTablet::TEvTabletDead, HandleTabletDead);

        IgnoreFunc(TEvTabletPipe::TEvServerConnected);
        IgnoreFunc(TEvTabletPipe::TEvServerDisconnected);
        IgnoreFunc(TEvDiskRegistryPrivate::TEvAgentConnectionLost);

        IgnoreFunc(
            TEvDiskRegistryPrivate::TEvDiskRegistryAgentListExpiredParamsCleanup);

        IgnoreFunc(
            TEvDiskRegistryPrivate::TEvSwitchAgentDisksToReadOnlyResponse);

        default:
            if (!RejectRequests(ev)) {
                HandleUnexpectedEvent(
                    ev,
                    TBlockStoreComponents::DISK_REGISTRY,
                    __PRETTY_FUNCTION__);
            }
            break;
    }
}

////////////////////////////////////////////////////////////////////////////////

bool ToLogicalBlocks(NProto::TDeviceConfig& device, ui32 logicalBlockSize)
{
    const auto blockSize = device.GetBlockSize();
    if (logicalBlockSize % blockSize != 0) {
        ReportDiskRegistryLogicalPhysicalBlockSizeMismatch();

        return false;
    }

    device.SetBlocksCount(device.GetBlocksCount() * blockSize / logicalBlockSize);
    device.SetBlockSize(logicalBlockSize);

    return true;
}

////////////////////////////////////////////////////////////////////////////////

void TDiskRegistryActor::OnDiskAcquired(
    TVector<TAgentAcquireDevicesCachedRequest> sentAcquireRequests)
{
    for (auto& sentRequest: sentAcquireRequests) {
        TCachedAcquireRequests& cachedRequests =
            State->GetAcquireCacheByAgentId()[sentRequest.AgentId];
        TCachedAcquireKey key{
            sentRequest.Request.GetDiskId(),
            sentRequest.Request.GetHeaders().GetClientId()};
        cachedRequests[key] = std::move(sentRequest);
    }
}

void TDiskRegistryActor::OnDiskReleased(
    const TVector<TAgentReleaseDevicesCachedRequest>& sentReleaseRequests)
{
    auto& acquireCacheByAgentId = State->GetAcquireCacheByAgentId();
    for (const auto& [agentId, releaseRequest]: sentReleaseRequests) {
        auto it = acquireCacheByAgentId.find(agentId);
        if (it == acquireCacheByAgentId.end()) {
            continue;
        }
        TCachedAcquireRequests& cachedRequests = it->second;
        TCachedAcquireKey key{
            releaseRequest.GetDiskId(),
            releaseRequest.GetHeaders().GetClientId()};
        cachedRequests.erase(key);
        if (cachedRequests.empty()) {
            acquireCacheByAgentId.erase(it);
        }
    }
}

void TDiskRegistryActor::OnDiskDeallocated(const TDiskId& diskId)
{
    auto& acquireCacheByAgentId = State->GetAcquireCacheByAgentId();
    for (auto& [_, request]: acquireCacheByAgentId) {
        EraseNodesIf(
            request,
            [&diskId](const auto& item)
            { return item.first.DiskId == diskId; });
    }
    EraseNodesIf(
        acquireCacheByAgentId,
        [](const auto& item) { return item.second.empty(); });
}

void TDiskRegistryActor::SendCachedAcquireRequestsToAgent(
    const TActorContext& ctx,
    const NProto::TAgentConfig& config)
{
    if (Config->GetNonReplicatedVolumeDirectAcquireEnabled()) {
        return;
    }
    auto& acquireCacheByAgentId = State->GetAcquireCacheByAgentId();
    auto cacheIt = acquireCacheByAgentId.find(config.GetAgentId());
    if (cacheIt == acquireCacheByAgentId.end()) {
        return;
    }
    // Since we will send all of the requests and they are non-copyable, just
    // extract whole container.
    TCachedAcquireRequests agentAcquireRequestCache =
        std::move(cacheIt->second);
    acquireCacheByAgentId.erase(cacheIt);

    TDuration lifetimeThreshold =
        Config->GetCachedAcquireRequestLifetime();
    TInstant now = ctx.Now();

    for (auto& [_, request]: agentAcquireRequestCache) {
        // If it is an old enough request, then we probably shouldn't send it.
        if (now - request.RequestTime > lifetimeThreshold) {
            continue;
        }

        TVector<NProto::TDeviceConfig> diskDevices;
        NProto::TError error =
            State->GetDiskDevices(request.Request.GetDiskId(), diskDevices);
        // Something happened with the disk from the request. Skip it.
        if (HasError(error) || diskDevices.empty()) {
            continue;
        }

        TSet<TString> diskAgentDeviceUUIDs;
        for (const auto& device: diskDevices) {
            if (config.GetAgentId() == device.GetAgentId()) {
                diskAgentDeviceUUIDs.insert(device.GetDeviceUUID());
            }
        }

        bool diskDevicesChanged = false;
        for (const auto& deviceUUID: request.Request.GetDeviceUUIDs()) {
            if (!diskAgentDeviceUUIDs.contains(deviceUUID)) {
                diskDevicesChanged = true;
                break;
            }
        }
        // We shouldn't send the request if all of the devices from the request
        // do not belong to the disk.
        if (diskDevicesChanged) {
            continue;
        }

        LOG_DEBUG(
            ctx,
            TBlockStoreComponents::DISK_REGISTRY,
            "[%lu] Send cached AcquireDisk request DiskId=%s to node #%d, "
            "AgentId=%s. Devices: [%s]",
            TabletID(),
            request.Request.GetDiskId().c_str(),
            config.GetNodeId(),
            config.GetAgentId().c_str(),
            JoinSeq(", ", request.Request.GetDeviceUUIDs()).c_str());

        auto requestEv =
            std::make_unique<TEvDiskAgent::TEvAcquireDevicesRequest>();
        requestEv->Record = std::move(request.Request);

        NCloud::Send(
            ctx,
            MakeDiskAgentServiceId(config.GetNodeId()),
            std::move(requestEv),
            config.GetNodeId());
    }
}

}   // namespace NCloud::NBlockStore::NStorage
