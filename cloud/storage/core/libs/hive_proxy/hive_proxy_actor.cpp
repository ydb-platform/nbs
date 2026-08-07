#include "hive_proxy_actor.h"

#include "tablet_boot_info_backup.h"

#include <cloud/storage/core/libs/common/verify.h>

#include <contrib/ydb/core/base/appdata.h>

namespace NCloud::NStorage {

using namespace NActors;

using namespace NKikimr;

namespace {

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<NTabletPipe::IClientCache> CreateTabletPipeClientCache(
    const THiveProxyConfig& config)
{
    NTabletPipe::TClientConfig clientConfig;
    clientConfig.RetryPolicy = {
        .RetryLimitCount = config.PipeClientRetryCount,
        .MinRetryTime = config.PipeClientMinRetryTime,
        .MaxRetryTime = config.HiveLockExpireTimeout
    };

    return std::unique_ptr<NTabletPipe::IClientCache>(
        NTabletPipe::CreateUnboundedClientCache(clientConfig));
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

THiveProxyActor::THiveProxyActor(
        THiveProxyConfig config,
        NMonitoring::TDynamicCounterPtr counters)
    : ClientCache(CreateTabletPipeClientCache(config))
    , PoisonPillHelper(this)
    , LockExpireTimeout(config.HiveLockExpireTimeout)
    , LogComponent(config.LogComponent)
    , RuntimeFallbackEnabled(!!config.FallbackModeProvider)
    , TabletBootInfoBackupFilePath(config.TabletBootInfoBackupFilePath)
    , UseBinaryFormatForTabletBootInfoBackup(config.UseBinaryFormatForTabletBootInfoBackup)
    , HiveTabletId(config.TenantHiveTabletId)
    , Counters(std::move(counters))
{}

THiveProxyActor::THiveProxyActor(THiveProxyConfig config)
    : THiveProxyActor(std::move(config), {})
{}

void THiveProxyActor::Bootstrap(const TActorContext& ctx)
{
    TThis::Become(&TThis::StateWork);

    if (TabletBootInfoBackupFilePath) {
        auto cache = std::make_unique<TTabletBootInfoBackup>(
            LogComponent,
            TabletBootInfoBackupFilePath,
            UseBinaryFormatForTabletBootInfoBackup,
            false /* readOnlyMode */
        );
        TabletBootInfoBackup = ctx.Register(
            cache.release(), TMailboxType::HTSwap, AppData()->IOPoolId);
        PoisonPillHelper.TakeOwnership(ctx, TabletBootInfoBackup);
    }
    if (Counters) {
        HiveReconnectTimeCounter = Counters->GetCounter("HiveReconnectTime", true);
    }

    if (!HiveTabletId) {
        HiveTabletId = AppData(ctx)->DomainsInfo->GetHive();
    }
}

////////////////////////////////////////////////////////////////////////////////

void THiveProxyActor::SendRequest(
    const TActorContext& ctx,
    IEventBase* request)
{
    TrackHiveClient(ctx, ClientCache->Send(ctx, HiveTabletId, request));
    if (HiveDisconnected) {
        HiveReconnectStartCycles = GetCycleCount();
    }
}

TActorId THiveProxyActor::PrepareHiveClient(const TActorContext& ctx)
{
    auto clientId = ClientCache->Prepare(ctx, HiveTabletId);
    TrackHiveClient(ctx, clientId);
    return clientId;
}

void THiveProxyActor::TrackHiveClient(
    const TActorContext& ctx,
    TActorId clientId)
{
    if (!RuntimeFallbackEnabled || clientId == HiveClient) {
        return;
    }

    if (HiveClient) {
        PoisonPillHelper.ReleaseOwnership(ctx, HiveClient);
    }

    HiveClient = clientId;
    PoisonPillHelper.TakeOwnership(ctx, HiveClient);
}

void THiveProxyActor::ReleaseHiveClient(
    const TActorContext& ctx,
    TActorId clientId)
{
    if (!RuntimeFallbackEnabled || clientId != HiveClient) {
        return;
    }

    HiveClient = {};
    PoisonPillHelper.ReleaseOwnership(ctx, clientId);
}

void THiveProxyActor::SendLockRequest(
    const TActorContext& ctx, ui64 tabletId, bool reconnect)
{
    auto hiveRequest =
        std::make_unique<TEvHive::TEvLockTabletExecution>(tabletId);
    hiveRequest->Record.SetMaxReconnectTimeout(
        LockExpireTimeout.MilliSeconds());
    hiveRequest->Record.SetReconnect(reconnect);
    SendRequest(ctx, hiveRequest.release());
}

void THiveProxyActor::SendUnlockRequest(
    const TActorContext& ctx, ui64 tabletId)
{
    auto hiveRequest =
        std::make_unique<TEvHive::TEvUnlockTabletExecution>(tabletId);
    SendRequest(ctx, hiveRequest.release());
}

void THiveProxyActor::SendGetTabletStorageInfoRequest(
    const TActorContext& ctx,
    ui64 tabletId)
{
    auto hiveRequest =
        std::make_unique<TEvHive::TEvGetTabletStorageInfo>(tabletId);
    SendRequest(ctx, hiveRequest.release());
}

void THiveProxyActor::SendLockReply(
    const TActorContext& ctx,
    TLockState* state,
    const NProto::TError& error)
{
    STORAGE_VERIFY(
        state->LockRequest,
        TWellKnownEntityTypes::TABLET,
        state->TabletId);
    auto response =
        std::make_unique<TEvHiveProxy::TEvLockTabletResponse>(error);
    NCloud::Reply(ctx, state->LockRequest, std::move(response));
    state->LockRequest.Drop();
}

void THiveProxyActor::SendUnlockReply(
    const TActorContext& ctx,
    TLockState* state,
    const NProto::TError& error)
{
    STORAGE_VERIFY(
        state->UnlockRequest,
        TWellKnownEntityTypes::TABLET,
        state->TabletId);
    auto response =
        std::make_unique<TEvHiveProxy::TEvUnlockTabletResponse>(error);
    NCloud::Reply(ctx, state->UnlockRequest, std::move(response));
    state->UnlockRequest.Drop();
}

void THiveProxyActor::SendLockLostNotification(
    const TActorContext& ctx,
    TLockState* state,
    const NProto::TError& error)
{
    NCloud::Send<TEvHiveProxy::TEvTabletLockLost>(
        ctx,
        state->Owner,
        state->Cookie,
        error,
        state->TabletId);
}

void THiveProxyActor::AddTabletMetrics(
    ui64 tabletId,
    const TTabletStats& tabletData,
    NKikimrHive::TEvTabletMetrics& record)
{
    auto& metrics = *record.AddTabletMetrics();
    metrics.SetTabletID(tabletId);
    if (tabletData.SlaveID != 0) {
        metrics.SetFollowerID(tabletData.SlaveID);
    }
    metrics.MutableResourceUsage()->MergeFrom(tabletData.ResourceValues);
}

void THiveProxyActor::ScheduleSendTabletMetrics(const TActorContext& ctx)
{
    auto& state = HiveState;
    if (!state.ScheduledSendTabletMetrics) {
        ctx.Schedule(BatchTimeout, new TEvHiveProxyPrivate::TEvSendTabletMetrics());
        state.ScheduledSendTabletMetrics = true;
    }
}

void THiveProxyActor::SendTabletMetrics(
    const TActorContext& ctx,
    bool resend)
{
    auto& state = HiveState;
    state.ScheduledSendTabletMetrics = false;
    TAutoPtr<TEvHive::TEvTabletMetrics> event = MakeHolder<TEvHive::TEvTabletMetrics>();
    NKikimrHive::TEvTabletMetrics& record = event->Record;
    for (auto& prTabletId: state.UpdatedTabletMetrics) {
        AddTabletMetrics(prTabletId.first, prTabletId.second, record);
        prTabletId.second.OnSendStats(resend);
    }
    if (record.TabletMetricsSize() > 0) {
        SendRequest(ctx, event.Release());
    }
}

////////////////////////////////////////////////////////////////////////////////

void THiveProxyActor::HandleConnect(
    TEvTabletPipe::TEvClientConnected::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    Y_DEBUG_ABORT_UNLESS(msg->TabletId == HiveTabletId);

    if (!ClientCache->OnConnect(ev)) {
        ReleaseHiveClient(ctx, msg->ClientId);
        // Connect to hive failed
        auto error = MakeKikimrError(msg->Status, TStringBuilder()
            << "Connect to hive " << HiveTabletId << " failed");
        HandleConnectionError(ctx, error, true);
    } else if (HiveReconnectStartCycles) {
        if (HiveReconnectTimeCounter) {
            HiveReconnectTimeCounter->Add(
                CyclesToDuration(
                    GetCycleCount() - HiveReconnectStartCycles).MicroSeconds());
        }
        HiveReconnectStartCycles = 0;
        HiveDisconnected = false;
    }
}

void THiveProxyActor::HandleDisconnect(
    TEvTabletPipe::TEvClientDestroyed::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    Y_DEBUG_ABORT_UNLESS(msg->TabletId == HiveTabletId);

    ClientCache->OnDisconnect(ev);
    ReleaseHiveClient(ctx, msg->ClientId);

    auto error = MakeError(E_REJECTED, TStringBuilder()
        << "Disconnected from hive " << HiveTabletId);
    HandleConnectionError(ctx, error, false);
}

void THiveProxyActor::HandleConnectionError(
    const TActorContext& ctx,
    const NProto::TError& error,
    bool connectFailed)
{
    Y_UNUSED(error);
    Y_UNUSED(connectFailed);

    HiveDisconnected = true;

    LOG_ERROR_S(
        ctx,
        LogComponent,
        "Pipe to hive " << HiveTabletId << " has been reset ");

    // Hive is a tablet, so it should eventually get up
    // Re-send all outstanding requests
    auto& states = HiveState;
    for (auto& kv: states.LockStates) {
        ui64 tabletId = kv.first;
        auto* state = &kv.second;
        if (state->Phase == PHASE_LOCKED) {
            // Link failed while locked, need to reconnect
            state->Phase = PHASE_RECONNECT;
        }
        if (state->Phase == PHASE_LOCKING ||
            state->Phase == PHASE_RECONNECT)
        {
            SendLockRequest(
                ctx,
                tabletId,
                state->Phase == PHASE_RECONNECT);
        } else if (state->Phase == PHASE_UNLOCKING) {
            // Hive is a tablet, keep retrying requests
            SendUnlockRequest(ctx, tabletId);
        }
    }

    // SendNextCreateOrLookupRequest() won't send any requests after the
    // first undelivery. Reject and hope that clients will retry.
    for (auto& [_, queue]: states.CreateRequests) {
        while (!queue.empty()) {
            TCreateOrLookupRequest request = std::move(queue.front());
            queue.pop_front();

            std::unique_ptr<IEventBase> response;
            auto error =
                MakeError(E_REJECTED, "Pipe to hive has been reset.");
            if (request.IsLookup) {
                response =
                    std::make_unique<TEvHiveProxy::TEvLookupTabletResponse>(
                        error);
            } else {
                response =
                    std::make_unique<TEvHiveProxy::TEvCreateTabletResponse>(
                        error);
            }
            NCloud::Reply(ctx, request, std::move(response));
        }
    }
    states.CreateRequests.clear();

    for (auto& kv: states.GetInfoRequests) {
        ui64 tabletId = kv.first;
        auto& requests = kv.second;
        if (!requests.empty()) {
            SendGetTabletStorageInfoRequest(ctx, tabletId);
        }
    }

    if (!states.ScheduledSendTabletMetrics) {
        SendTabletMetrics(
            ctx,
            true   // resend
        );
    }

    if (!states.Actors.empty()) {
        auto clientId = PrepareHiveClient(ctx);
        if (!HiveReconnectStartCycles) {
            HiveReconnectStartCycles = GetCycleCount();
        }
        for (const auto& actorId: states.Actors) {
            NCloud::Send<TEvHiveProxyPrivate::TEvChangeTabletClient>(
                ctx,
                actorId,
                0,
                clientId);
        }
    }
}

void THiveProxyActor::HandleLockTabletExecutionLost(
    const TEvHive::TEvLockTabletExecutionLost::TPtr& ev,
    const TActorContext& ctx)
{
    ui64 tabletId = ev->Get()->Record.GetTabletID();
    auto& states = HiveState;
    auto* state = states.LockStates.FindPtr(tabletId);
    if (!state || state->Phase == PHASE_LOCKING) {
        // Unexpected notification, ignore
        LOG_WARN_S(ctx, LogComponent,
            "Unexpected lock lost notification from hive " << HiveTabletId
                << " for tablet " << tabletId);
        return;
    }

    if (state->Phase == PHASE_RECONNECT || state->Phase == PHASE_UNLOCKING) {
        // Ignore notification while in these states, since outgoing requests
        // would confirm the state of lock anyway.
        return;
    }

    STORAGE_VERIFY(
        state->Phase == PHASE_LOCKED,
        TWellKnownEntityTypes::TABLET,
        tabletId);
    SendLockLostNotification(
        ctx,
        state,
        MakeError(E_REJECTED, "Lock lost upon HIVE notification"));
    states.LockStates.erase(tabletId);
}

bool THiveProxyActor::HandleRequests(STFUNC_SIG)
{
    switch (ev->GetTypeRewrite()) {
        STORAGE_HIVE_PROXY_REQUESTS(STORAGE_HANDLE_REQUEST, TEvHiveProxy)

        default:
            return false;
    }

    return true;
}

void THiveProxyActor::HandleRequestFinished(
    const TEvHiveProxyPrivate::TEvRequestFinished::TPtr& ev,
    const TActorContext& ctx)
{
    HiveState.Actors.erase(ev->Sender);
    PoisonPillHelper.ReleaseOwnership(ctx, ev->Sender);
}

void THiveProxyActor::RejectPendingRequests(const TActorContext& ctx)
{
    const auto error = MakeError(E_REJECTED, "HiveProxy is shutting down");

    for (auto& [_, state]: HiveState.LockStates) {
        if (state.LockRequest) {
            SendLockReply(ctx, &state, error);
        } else {
            SendLockLostNotification(ctx, &state, error);
        }

        if (state.UnlockRequest) {
            SendUnlockReply(ctx, &state, error);
        }
    }
    HiveState.LockStates.clear();

    for (auto& [_, requests]: HiveState.GetInfoRequests) {
        while (!requests.empty()) {
            auto response =
                std::make_unique<TEvHiveProxy::TEvGetStorageInfoResponse>(
                    error,
                    nullptr);
            NCloud::Reply(ctx, requests.front(), std::move(response));
            requests.pop_front();
        }
    }
    HiveState.GetInfoRequests.clear();

    for (auto& [_, requests]: HiveState.CreateRequests) {
        while (!requests.empty()) {
            auto request = std::move(requests.front());
            requests.pop_front();

            std::unique_ptr<IEventBase> response;
            if (request.IsLookup) {
                response =
                    std::make_unique<TEvHiveProxy::TEvLookupTabletResponse>(
                        error);
            } else {
                response =
                    std::make_unique<TEvHiveProxy::TEvCreateTabletResponse>(
                        error);
            }
            NCloud::Reply(ctx, request, std::move(response));
        }
    }
    HiveState.CreateRequests.clear();
}

void THiveProxyActor::HandlePoisonPill(
    const TEvents::TEvPoisonPill::TPtr& ev,
    const TActorContext& ctx)
{
    RejectPendingRequests(ctx);
    if (!RuntimeFallbackEnabled || !HiveClient) {
        ClientCache->Shutdown(ctx, HiveTabletId);
    }
    TThis::Become(&TThis::StateShutdown);
    PoisonPillHelper.HandlePoisonPill(ev, ctx);
}

void THiveProxyActor::HandleConnectDuringShutdown(
    TEvTabletPipe::TEvClientConnected::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();
    if (msg->Status != NKikimrProto::OK) {
        ReleaseHiveClient(ctx, msg->ClientId);
    }
}

void THiveProxyActor::HandleDisconnectDuringShutdown(
    TEvTabletPipe::TEvClientDestroyed::TPtr& ev,
    const TActorContext& ctx)
{
    ReleaseHiveClient(ctx, ev->Get()->ClientId);
}

void THiveProxyActor::HandlePoisonTaken(
    const TEvents::TEvPoisonTaken::TPtr& ev,
    const TActorContext& ctx)
{
    PoisonPillHelper.HandlePoisonTaken(ev, ctx);
}

void THiveProxyActor::Poison(const TActorContext& ctx)
{
    Die(ctx);
}

void THiveProxyActor::HandleTabletMetrics(
    const TEvLocal::TEvTabletMetrics::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    const auto& metrics = msg->ResourceValues;
    auto& state = HiveState;
    auto& tabletData = state.UpdatedTabletMetrics[msg->TabletId];
    tabletData.SlaveID = msg->FollowerId;

    bool hasChanges = false;

    if (metrics.HasCPU()) {
        tabletData.ResourceValues.SetCPU(metrics.GetCPU());
        hasChanges = true;
    }
    if (metrics.HasMemory()) {
        tabletData.ResourceValues.SetMemory(metrics.GetMemory());
        hasChanges = true;
    }
    if (metrics.HasNetwork()) {
        tabletData.ResourceValues.SetNetwork(metrics.GetNetwork());
        hasChanges = true;
    }
    if (metrics.HasStorage()) {
        tabletData.ResourceValues.SetStorage(metrics.GetStorage());
        hasChanges = true;
    }
    if (metrics.GroupReadThroughputSize() > 0) {
        tabletData.ResourceValues.ClearGroupReadThroughput();
        for (const auto& v: metrics.GetGroupReadThroughput()) {
            tabletData.ResourceValues.AddGroupReadThroughput()->CopyFrom(v);
        }
        hasChanges = true;
    }
    if (metrics.GroupWriteThroughputSize() > 0) {
        tabletData.ResourceValues.ClearGroupWriteThroughput();
        for (const auto& v: metrics.GetGroupWriteThroughput()) {
            tabletData.ResourceValues.AddGroupWriteThroughput()->CopyFrom(v);
        }
        hasChanges = true;
    }
    if (metrics.GroupReadIopsSize() > 0) {
        tabletData.ResourceValues.ClearGroupReadIops();
        for (const auto& v: metrics.GetGroupReadIops()) {
            tabletData.ResourceValues.AddGroupReadIops()->CopyFrom(v);
        }
        hasChanges = true;
    }
    if (metrics.GroupWriteIopsSize() > 0) {
        tabletData.ResourceValues.ClearGroupWriteIops();
        for (const auto& v: metrics.GetGroupWriteIops()) {
            tabletData.ResourceValues.AddGroupWriteIops()->CopyFrom(v);
        }
        hasChanges = true;
    }

    if (!hasChanges) {
        return;
    }

    tabletData.OnUpdateStats();

    if (!state.ScheduledSendTabletMetrics) {
        ScheduleSendTabletMetrics(ctx);
    }
}

void THiveProxyActor::HandleSendTabletMetrics(
    const TEvHiveProxyPrivate::TEvSendTabletMetrics::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);
    SendTabletMetrics(
        ctx,
        false      // resend
    );
}

void THiveProxyActor::HandleMetricsResponse(
    const TEvLocal::TEvTabletMetricsAck::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();
    auto size = msg->Record.TabletIdSize();
    Y_DEBUG_ABORT_UNLESS(msg->Record.FollowerIdSize() == size);

    auto& state = HiveState;
    for (size_t i = 0; i < size; ++i) {
        auto uit = state.UpdatedTabletMetrics.find(msg->Record.GetTabletId(i));
        if (uit != state.UpdatedTabletMetrics.end()) {
            uit->second.OnHiveAck();
            if (uit->second.IsEmpty()) {
                state.UpdatedTabletMetrics.erase(uit);
            } else {
                if (!state.ScheduledSendTabletMetrics) {
                    ScheduleSendTabletMetrics(ctx);
                }
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

STFUNC(THiveProxyActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvTabletPipe::TEvClientConnected, HandleConnect);
        HFunc(TEvTabletPipe::TEvClientDestroyed, HandleDisconnect);
        HFunc(TEvHive::TEvLockTabletExecutionResult,
            HandleLockTabletExecutionResult);
        HFunc(TEvHive::TEvLockTabletExecutionLost,
            HandleLockTabletExecutionLost);
        HFunc(TEvHive::TEvUnlockTabletExecutionResult,
            HandleUnlockTabletExecutionResult);
        HFunc(TEvHive::TEvGetTabletStorageInfoRegistered,
            HandleGetTabletStorageInfoRegistered);
        HFunc(TEvHive::TEvGetTabletStorageInfoResult,
            HandleGetTabletStorageInfoResult);

        HFunc(TEvHive::TEvCreateTabletReply, HandleCreateTabletReply);
        HFunc(TEvHive::TEvTabletCreationResult, HandleTabletCreation);

        HFunc(TEvLocal::TEvTabletMetrics, HandleTabletMetrics)
        HFunc(TEvLocal::TEvTabletMetricsAck, HandleMetricsResponse)
        IgnoreFunc(TEvLocal::TEvReconnect);
        HFunc(TEvHiveProxyPrivate::TEvSendTabletMetrics, HandleSendTabletMetrics);

        HFunc(TEvHiveProxyPrivate::TEvRequestFinished, HandleRequestFinished);

        HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);
        HFunc(TEvents::TEvPoisonTaken, HandlePoisonTaken);

        default:
            if (!HandleRequests(ev)) {
                HandleUnexpectedEvent(ev, LogComponent, __PRETTY_FUNCTION__);
            }
            break;
    }
}

////////////////////////////////////////////////////////////////////////////////

STFUNC(THiveProxyActor::StateShutdown)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvTabletPipe::TEvClientConnected, HandleConnectDuringShutdown);
        HFunc(
            TEvTabletPipe::TEvClientDestroyed,
            HandleDisconnectDuringShutdown);
        HFunc(TEvHiveProxyPrivate::TEvRequestFinished, HandleRequestFinished);
        HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);
        HFunc(TEvents::TEvPoisonTaken, HandlePoisonTaken);

        STORAGE_HIVE_PROXY_REQUESTS(STORAGE_REJECT_REQUEST, TEvHiveProxy)

        default:
            break;
    }
}

////////////////////////////////////////////////////////////////////////////////

void THiveProxyActor::HandleBackupTabletBootInfos(
    const TEvHiveProxy::TEvBackupTabletBootInfosRequest::TPtr& ev,
    const TActorContext& ctx)
{
    if (TabletBootInfoBackup) {
        ctx.Send(ev->Forward(TabletBootInfoBackup));
    } else {
        auto response =
            std::make_unique<TEvHiveProxy::TEvBackupTabletBootInfosResponse>(
                MakeError(S_FALSE));
        NCloud::Reply(ctx, *ev, std::move(response));
    }
}

void THiveProxyActor::HandleListTabletBootInfoBackups(
    const TEvHiveProxy::TEvListTabletBootInfoBackupsRequest::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    if (TabletBootInfoBackup) {
        ctx.Send(ev->Forward(TabletBootInfoBackup));
    } else {
        auto response = std::make_unique<
            TEvHiveProxy::TEvListTabletBootInfoBackupsResponse>(
            MakeError(S_FALSE));
        NCloud::Reply(ctx, *ev, std::move(response));
    }
}

void THiveProxyActor::HandleGetTabletBootInfos(
    const TEvHiveProxy::TEvGetTabletBootInfosRequest::TPtr& ev,
    const TActorContext& ctx)
{
    if (TabletBootInfoBackup) {
        ctx.Send(ev->Forward(TabletBootInfoBackup));
    } else {
        auto response =
            std::make_unique<TEvHiveProxy::TEvGetTabletBootInfosResponse>(
                MakeError(E_PRECONDITION_FAILED));
        NCloud::Reply(ctx, *ev, std::move(response));
    }
}

}   // namespace NCloud::NStorage
