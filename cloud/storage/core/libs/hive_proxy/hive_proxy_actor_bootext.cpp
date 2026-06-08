#include "hive_proxy_actor.h"

#include <contrib/ydb/core/mind/local.h>

#include <algorithm>

namespace NCloud::NStorage {

using namespace NActors;

using namespace NKikimr;

namespace {

////////////////////////////////////////////////////////////////////////////////

TString PrintChannels(const TVector<TTabletChannelInfo>& channels)
{
    TStringBuilder res;
    res << "[";
    for (size_t i = 0; i < channels.size(); i++) {
        if (i != 0) {
            res << ", ";
        }
        res << channels[i].ToString();
    }
    res << "]";
    return res;
}

std::unique_ptr<TEvHiveProxy::TEvBootExternalResponse> MakeBootExternalResponse(
    const NProto::TError& error)
{
    return std::make_unique<TEvHiveProxy::TEvBootExternalResponse>(error);
}

std::unique_ptr<TEvHiveProxy::TEvBootExternalResponse> MakeBootExternalResponse(
    const TEvHiveProxyPrivate::TBootExternalCompleted& msg)
{
    if (HasError(msg.Error)) {
        return MakeBootExternalResponse(msg.Error);
    }
    return std::make_unique<TEvHiveProxy::TEvBootExternalResponse>(
        msg.StorageInfo,
        msg.SuggestedGeneration,
        msg.BootMode,
        msg.SlaveId);
}

////////////////////////////////////////////////////////////////////////////////

// Actor that works to a single HIVE and Tablet on behalf of THiveProxyActor.
//
// Lifetime is owned by THiveProxyActor — the actor dies after sending the
// result. The set of waiters lives in
// THiveProxyActor::HiveStates[..].InflightBootRequests, so a late
// TEvBootExternalRequest is attached to the still-alive actor without a race
// against the actor's death.
class TBootRequestActor final
    : public TActorBootstrapped<TBootRequestActor>
{
private:
    const TActorId Owner;
    const int LogComponent;
    const ui64 Hive;
    const ui64 TabletId;
    const ui32 BootGeneration;
    TActorId ClientId;
    TActorId TabletBootInfoBackup;

public:
    TBootRequestActor(
            const TActorId& owner,
            int logComponent,
            ui64 hive,
            ui64 tabletId,
            ui32 bootGeneration,
            TActorId clientId,
            TActorId tabletBootInfoBackup)
        : Owner(owner)
        , LogComponent(logComponent)
        , Hive(hive)
        , TabletId(tabletId)
        , BootGeneration(bootGeneration)
        , ClientId(clientId)
        , TabletBootInfoBackup(std::move(tabletBootInfoBackup))
    {}

    void Bootstrap(const TActorContext& ctx);

private:
    template<class... TArgs>
    void NotifyAndDie(const TActorContext& ctx, TArgs&&... args);

    void HandleChangeTabletClient(
        const TEvHiveProxyPrivate::TEvChangeTabletClient::TPtr& ev,
        const TActorContext& ctx);

    void HandleBoot(
        const NKikimr::TEvLocal::TEvBootTablet::TPtr& ev,
        const TActorContext& ctx);

    void HandleError(
        const NKikimr::TEvHive::TEvBootTabletReply::TPtr& ev,
        const TActorContext& ctx);

    void HandlePoisonPill(
        const TEvents::TEvPoisonPill::TPtr& ev,
        const TActorContext& ctx);

    STFUNC(StateWork);
};

////////////////////////////////////////////////////////////////////////////////

void TBootRequestActor::Bootstrap(const TActorContext& ctx)
{
    NKikimr::NTabletPipe::SendData(
        ctx,
        ClientId,
        new NKikimr::TEvHive::TEvInitiateTabletExternalBoot(TabletId));

    Become(&TThis::StateWork);
}

template<class... TArgs>
void TBootRequestActor::NotifyAndDie(const TActorContext& ctx, TArgs&&... args)
{
    NCloud::Send<TEvHiveProxyPrivate::TEvBootExternalCompleted>(
        ctx,
        Owner,
        0,   // cookie
        std::forward<TArgs>(args)...);
    Die(ctx);
}

void TBootRequestActor::HandleChangeTabletClient(
    const TEvHiveProxyPrivate::TEvChangeTabletClient::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    ClientId = msg->ClientId;

    LOG_ERROR(
        ctx,
        LogComponent,
        "Pipe to Hive has been reset. New client: %s", ToString(ClientId).c_str());

    Bootstrap(ctx);
}

void TBootRequestActor::HandleBoot(
    const NKikimr::TEvLocal::TEvBootTablet::TPtr& ev,
    const TActorContext& ctx)
{
    using EBootMode = TEvHiveProxy::TEvBootExternalResponse::EBootMode;

    const auto* msg = ev->Get();

    TTabletStorageInfoPtr storageInfo =
        TabletStorageInfoFromProto(msg->Record.GetInfo());
    ui64 suggestedGeneration = msg->Record.GetSuggestedGeneration();

    LOG_DEBUG(
        ctx,
        LogComponent,
        "[%s] Booting tablet with channels: %s",
        ToString(storageInfo->TabletID).c_str(),
        PrintChannels(storageInfo->Channels).c_str());

    if (TabletBootInfoBackup) {
        auto updateRequest =
            std::make_unique<TEvHiveProxyPrivate::TEvUpdateTabletBootInfoBackupRequest>(
                storageInfo,
                suggestedGeneration
            );
        NCloud::Send(ctx, TabletBootInfoBackup, std::move(updateRequest));
    }

    EBootMode bootMode;
    switch (msg->Record.GetBootMode()) {
        case NKikimrLocal::BOOT_MODE_LEADER:
            bootMode = EBootMode::MASTER;
            break;
        case NKikimrLocal::BOOT_MODE_FOLLOWER:
            bootMode = EBootMode::SLAVE;
            break;
        default:
            LOG_ERROR(ctx, LogComponent,
                "Received unexpected BootMode=%u from hive",
                msg->Record.GetBootMode());
            bootMode = EBootMode::MASTER;
            break;
    }

    NotifyAndDie(
        ctx,
        Hive,
        TabletId,
        BootGeneration,
        std::move(storageInfo),
        suggestedGeneration,
        bootMode,
        msg->Record.GetFollowerId());
}

void TBootRequestActor::HandleError(
    const NKikimr::TEvHive::TEvBootTabletReply::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    NotifyAndDie(
        ctx,
        Hive,
        TabletId,
        BootGeneration,
        MakeKikimrError(msg->Record.GetStatus(), "External boot failed"));
}

void TBootRequestActor::HandlePoisonPill(
    const TEvents::TEvPoisonPill::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    LOG_DEBUG(ctx, LogComponent, "[%lu] poisoned by hive proxy", TabletId);

    // No report — the hive proxy is the sender and already knows about.
    Die(ctx);
}

////////////////////////////////////////////////////////////////////////////////

STFUNC(TBootRequestActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvHiveProxyPrivate::TEvChangeTabletClient, HandleChangeTabletClient);

        HFunc(NKikimr::TEvLocal::TEvBootTablet, HandleBoot);
        HFunc(NKikimr::TEvHive::TEvBootTabletReply, HandleError);

        HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);

        default:
            HandleUnexpectedEvent(ev, LogComponent, __PRETTY_FUNCTION__);
            break;
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void THiveProxyActor::HandleBootExternal(
    const TEvHiveProxy::TEvBootExternalRequest::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    const ui64 tabletId = msg->TabletId;
    const ui64 hive = GetHive(ctx, tabletId);

    auto& states = HiveStates[hive];
    auto& inflight = states.InflightBootRequests[tabletId];

    const auto now = ctx.Now();
    const TInstant deadline =
        msg->RequestTimeout ? now + msg->RequestTimeout : TInstant::Zero();

    inflight.AddWaiter(TRequestInfo(ev->Sender, ev->Cookie), deadline);

    if (!inflight.BootRequestActor) {
        inflight.Generation = ++NextBootGeneration;
        auto clientId = ClientCache->Prepare(ctx, hive);
        inflight.BootRequestActor = NCloud::Register<TBootRequestActor>(
            ctx,
            SelfId(),
            LogComponent,
            hive,
            tabletId,
            inflight.Generation,
            clientId,
            TabletBootInfoBackup);
        states.Actors.insert(inflight.BootRequestActor);

        LOG_INFO(
            ctx,
            LogComponent,
            "[%lu] Started new TEvBootExternalRequest actor (hive=%lu, "
            "generation=%lu)",
            tabletId,
            hive,
            inflight.Generation);
    } else {
        inflight.NoWaitersDeadline = TInstant::Zero();

        LOG_INFO(
            ctx,
            LogComponent,
            "[%lu] Attached new TEvBootExternalRequest waiter to in-flight "
            "request (total waiters: %lu)",
            tabletId,
            inflight.Waiters.size());
    }

    ScheduleBootExternalTimeoutIfNeeded(ctx, inflight, hive, tabletId);
}

void THiveProxyActor::ScheduleBootExternalTimeoutIfNeeded(
    const TActorContext& ctx,
    TInflightBootRequest& inflight,
    ui64 hive,
    ui64 tabletId)
{
    // The next wake up moment for this entry: while clients are
    // waiting it is the nearest waiter deadline; once the last waiter is gone
    // it is the no-waiters deadline.
    const TInstant target = inflight.Waiters.empty()
                                ? inflight.NoWaitersDeadline
                                : inflight.NearestDeadline;

    if (target == TInstant::Zero()) {
        return;
    }

    if (inflight.NextWakeupAt != TInstant::Zero() &&
        inflight.NextWakeupAt <= target)
    {
        // Already have an equal or earlier wakeup scheduled — it will fire and
        // we'll re-evaluate then.
        return;
    }

    const auto now = ctx.Now();
    const auto delay = target > now ? target - now : TDuration::Zero();

    inflight.NextWakeupAt = target;
    ctx.Schedule(
        delay,
        new TEvHiveProxyPrivate::TEvBootExternalTimeout(hive, tabletId));
}

void THiveProxyActor::HandleBootExternalCompleted(
    const TEvHiveProxyPrivate::TEvBootExternalCompleted::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();
    const ui64 tabletId = msg->TabletId;
    const ui64 hive = msg->Hive;

    auto* states = HiveStates.FindPtr(hive);
    if (!states) {
        LOG_WARN(
            ctx,
            LogComponent,
            "[%lu] can't find HiveStates with hive=%lu",
            tabletId,
            hive);
        Y_DEBUG_ABORT_UNLESS(false);
        return;
    }

    auto it = states->InflightBootRequests.find(tabletId);
    if (it == states->InflightBootRequests.end()) {
        // Expected under the idle-timeout race: the request was already
        // completed or torn down and its entry erased while this completion
        // was still queued. Drop it — neither abort nor mis-deliver.
        LOG_DEBUG(
            ctx,
            LogComponent,
            "[%lu] stale boot completed, no inflight entry, hive=%lu, "
            "generation=%lu",
            tabletId,
            hive,
            msg->InflightGeneration);
        return;
    }

    // Reply to all waiters of the inflight boot request,
    // drop the inflight entry from HiveStates[hive] and stop
    // tracking the actor.
    auto& inflight = it->second;

    if (inflight.Generation != msg->InflightGeneration) {
        // A completion from a previous request's actor raced with a new
        // one for the same tabletId (e.g. idle teardown + fresh request).
        // Ignore it: the new waiters keep waiting for their own answer.
        LOG_DEBUG(
            ctx,
            LogComponent,
            "[%lu] stale boot completed, generation %lu != %lu, hive=%lu",
            tabletId,
            msg->InflightGeneration,
            inflight.Generation,
            hive);
        return;
    }

    for (const auto& waiter: inflight.Waiters) {
        auto response = MakeBootExternalResponse(*msg);
        NCloud::Reply(ctx, waiter.Request, std::move(response));
    }

    if (inflight.BootRequestActor) {
        states->Actors.erase(inflight.BootRequestActor);
    }

    states->InflightBootRequests.erase(it);
}

void THiveProxyActor::HandleBootExternalTimeout(
    const TEvHiveProxyPrivate::TEvBootExternalTimeout::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();
    const ui64 tabletId = msg->TabletId;
    const ui64 hive = msg->Hive;

    auto* states = HiveStates.FindPtr(hive);
    if (!states) {
        LOG_WARN(
            ctx,
            LogComponent,
            "[%lu] can't find HiveStates with hive=%lu",
            tabletId,
            hive);
        Y_DEBUG_ABORT_UNLESS(false);
        return;
    }

    auto it = states->InflightBootRequests.find(tabletId);
    if (it == states->InflightBootRequests.end()) {
        // Expected: the boot already completed (HIVE replied) and the entry was
        // erased, but the scheduled TEvBootExternalTimeout cannot be cancelled
        // and fires afterwards. Ignore the stale timer.
        LOG_DEBUG(
            ctx,
            LogComponent,
            "[%lu] stale boot timeout, no inflight entry, hive=%lu",
            tabletId,
            hive);
        return;
    }

    auto& inflight = it->second;

    const auto now = ctx.Now();

    if (inflight.NextWakeupAt && now < inflight.NextWakeupAt) {
        // Stale wakeup (a smaller deadline was added after this timer was
        // armed, or the entry has been refreshed since).
        return;
    }
    inflight.NextWakeupAt = TInstant::Zero();

    auto expired = std::stable_partition(
        inflight.Waiters.begin(),
        inflight.Waiters.end(),
        [now](const TBootExternalWaiterInfo& w) { return !w.IsExpired(now); });

    for (auto it2 = expired; it2 != inflight.Waiters.end(); ++it2) {
        auto response = MakeBootExternalResponse(
            MakeError(E_REJECTED, "External boot timed out"));
        NCloud::Reply(ctx, it2->Request, std::move(response));
    }
    inflight.Waiters.erase(expired, inflight.Waiters.end());

    inflight.RecomputeNearestDeadline();

    if (inflight.Waiters.empty()) {
        // No clients are waiting anymore. Keep the actor (and its
        // still-pending TEvInitiateTabletExternalBoot) alive for a
        // ExternalBootRequestIdleTimeout so a fresh retry storm for the same
        // tabletId still collapses onto the in-flight HIVE call instead of
        // issuing a new one — this is the whole point of the dedup.
        // If the window elapses with no new waiter, stop the idle actor so
        // the entry doesn't live forever and a pipe reset doesn't re-boot
        // the tablet with nobody waiting.
        if (ExternalBootRequestIdleTimeout == TDuration::Zero()) {
            // keep the actor until HIVE replies (success or error)
            // via HandleBootExternalCompleted.
        } else if (inflight.NoWaitersDeadline == TInstant::Zero()) {
            inflight.NoWaitersDeadline = now + ExternalBootRequestIdleTimeout;
        } else if (now >= inflight.NoWaitersDeadline) {
            LOG_INFO(
                ctx,
                LogComponent,
                "[%lu] timeout of the idle TEvBootExternalRequest actor is "
                "finished, stopping it (hive=%lu)",
                tabletId,
                hive);
            if (inflight.BootRequestActor) {
                NCloud::Send<TEvents::TEvPoisonPill>(
                    ctx,
                    inflight.BootRequestActor);
                states->Actors.erase(inflight.BootRequestActor);
            }
            states->InflightBootRequests.erase(it);
            return;
        }
    } else {
        inflight.NoWaitersDeadline = TInstant::Zero();
    }

    ScheduleBootExternalTimeoutIfNeeded(ctx, inflight, hive, tabletId);
}

}   // namespace NCloud::NStorage
