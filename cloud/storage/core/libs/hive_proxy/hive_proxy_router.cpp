#include "hive_proxy_router.h"

#include "hive_proxy_actor.h"
#include "hive_proxy_fallback_actor.h"

#include <contrib/ydb/core/base/appdata.h>

namespace NCloud::NStorage {

using namespace NActors;

////////////////////////////////////////////////////////////////////////////////

THiveProxyRouter::THiveProxyRouter(
    THiveProxyConfig config,
    NMonitoring::TDynamicCounterPtr counters)
    : Config(std::move(config))
    , Counters(std::move(counters))
{}

void THiveProxyRouter::Bootstrap(const NActors::TActorContext& ctx)
{
    TThis::Become(&TThis::StateWork);

    CreateActiveActor(ctx, Config.FallbackModeProvider());
    ScheduleCheckFallbackMode(ctx);
}

////////////////////////////////////////////////////////////////////////////////

void THiveProxyRouter::CreateActiveActor(
    const NActors::TActorContext& ctx,
    bool fallbackMode)
{
    std::unique_ptr<IActor> actor;

    CurrentFallbackMode = fallbackMode;
    if (CurrentFallbackMode) {
        actor = std::make_unique<THiveProxyFallbackActor>(Config);
    } else {
        actor = std::make_unique<THiveProxyActor>(Config, Counters);
    }

    ActiveActor = ctx.Register(actor.release());
}

void THiveProxyRouter::ScheduleCheckFallbackMode(
    const NActors::TActorContext& ctx)
{
    ctx.Schedule(
        CheckInterval,
        new TEvHiveProxyPrivate::TEvCheckFallbackMode());
}

void THiveProxyRouter::SwitchToFallback(const NActors::TActorContext& ctx)
{
    SwitchingToFallback = true;
    TThis::Become(&TThis::StateSwitching);
    ctx.Send(ActiveActor, new TEvents::TEvPoisonPill());
}

void THiveProxyRouter::HandleActiveActorPoisonTaken(
    const TEvents::TEvPoisonTaken::TPtr& ev,
    const TActorContext& ctx)
{
    if (ev->Sender != ActiveActor) {
        LOG_WARN(
            ctx,
            Config.LogComponent,
            "HiveProxyRouter: ignoring TEvPoisonTaken from unexpected actor %s",
            ev->Sender.ToString().c_str());
        return;
    }

    ActiveActor = {};

    if (Poisoner) {
        ctx.Send(
            Poisoner->Sender,
            std::make_unique<TEvents::TEvPoisonTaken>(),
            0,   // flags
            Poisoner->Cookie);
        Die(ctx);
        return;
    }

    // A normal-to-fallback transition is intentionally latched. Returning to
    // normal mode is only allowed after a service restart.
    SwitchingToFallback = false;
    CreateActiveActor(ctx, true);
    TThis::Become(&TThis::StateWork);
    ScheduleCheckFallbackMode(ctx);
}

void THiveProxyRouter::HandlePoisonPill(
    const TEvents::TEvPoisonPill::TPtr& ev,
    const TActorContext& ctx)
{
    if (Poisoner) {
        return;
    }

    Poisoner = TPoisoner{
        .Sender = ev->Sender,
        .Cookie = ev->Cookie,
    };

    if (SwitchingToFallback) {
        return;
    }

    TThis::Become(&TThis::StateSwitching);
    ctx.Send(ActiveActor, new TEvents::TEvPoisonPill());
}

void THiveProxyRouter::HandleCheckFallbackMode(
    const TEvHiveProxyPrivate::TEvCheckFallbackMode::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    Y_UNUSED(ev);

    const bool fallbackMode = Config.FallbackModeProvider();

    if (!CurrentFallbackMode && fallbackMode) {
        LOG_INFO(
            ctx,
            Config.LogComponent,
            "HiveProxyRouter: switching from normal to fallback mode");

        SwitchToFallback(ctx);
        return;
    }

    if (CurrentFallbackMode && !fallbackMode) {
        LOG_WARN(
            ctx,
            Config.LogComponent,
            "HiveProxyRouter: normal mode requested while running in fallback "
            "mode; service restart is required");
        return;
    }

    ScheduleCheckFallbackMode(ctx);
}

////////////////////////////////////////////////////////////////////////////////

STFUNC(THiveProxyRouter::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(
            TEvHiveProxyPrivate::TEvCheckFallbackMode,
            HandleCheckFallbackMode);
        HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);

        default:
            Send(ev->Forward(ActiveActor));
            break;
    }
}

////////////////////////////////////////////////////////////////////////////////

STFUNC(THiveProxyRouter::StateSwitching)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvents::TEvPoisonTaken, HandleActiveActorPoisonTaken);
        HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);

        STORAGE_HIVE_PROXY_REQUESTS(STORAGE_REJECT_REQUEST, TEvHiveProxy)

        default:
            LogUnexpectedEvent(ev, Config.LogComponent, __PRETTY_FUNCTION__);
            break;
    }
}

}   // namespace NCloud::NStorage
