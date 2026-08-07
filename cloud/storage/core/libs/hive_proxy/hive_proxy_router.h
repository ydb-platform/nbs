#pragma once

#include "public.h"

#include "hive_proxy_events_private.h"

#include <cloud/storage/core/libs/api/hive_proxy.h>
#include <cloud/storage/core/libs/kikimr/helpers.h>

#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>
#include <contrib/ydb/library/actors/core/events.h>
#include <contrib/ydb/library/actors/core/hfunc.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>

#include <optional>

namespace NCloud::NStorage {

////////////////////////////////////////////////////////////////////////////////

class THiveProxyRouter final
    : public NActors::TActorBootstrapped<THiveProxyRouter>
{
private:
    struct TPoisoner
    {
        NActors::TActorId Sender;
        ui64 Cookie = 0;
    };

    static constexpr TDuration CheckInterval = TDuration::Seconds(5);

    THiveProxyConfig Config;
    NMonitoring::TDynamicCounterPtr Counters;

    NActors::TActorId ActiveActor;
    bool CurrentFallbackMode = false;
    bool SwitchingToFallback = false;
    std::optional<TPoisoner> Poisoner;

public:
    THiveProxyRouter(
        THiveProxyConfig config,
        NMonitoring::TDynamicCounterPtr counters);

    void Bootstrap(const NActors::TActorContext& ctx);

private:
    STFUNC(StateWork);
    STFUNC(StateSwitching);

    void CreateActiveActor(
        const NActors::TActorContext& ctx,
        bool fallbackMode);
    void ScheduleCheckFallbackMode(const NActors::TActorContext& ctx);

    void SwitchToFallback(const NActors::TActorContext& ctx);

    void HandleActiveActorPoisonTaken(
        const NActors::TEvents::TEvPoisonTaken::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandlePoisonPill(
        const NActors::TEvents::TEvPoisonPill::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleCheckFallbackMode(
        const TEvHiveProxyPrivate::TEvCheckFallbackMode::TPtr& ev,
        const NActors::TActorContext& ctx);

    STORAGE_HIVE_PROXY_REQUESTS(STORAGE_IMPLEMENT_REQUEST, TEvHiveProxy)
};

}   // namespace NCloud::NStorage
