#pragma once

#include "public.h"

#include "hive_proxy_events_private.h"

#include <cloud/storage/core/libs/api/hive_proxy.h>
#include <cloud/storage/core/libs/actors/poison_pill_helper.h>
#include <cloud/storage/core/libs/common/public.h>
#include <cloud/storage/core/libs/kikimr/helpers.h>

#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>
#include <contrib/ydb/library/actors/core/events.h>
#include <contrib/ydb/library/actors/core/hfunc.h>

namespace NCloud::NStorage {

////////////////////////////////////////////////////////////////////////////////

class THiveProxyFallbackActor final
    : public NActors::TActorBootstrapped<THiveProxyFallbackActor>
    , public IMortalActor
{
private:
    const THiveProxyConfig Config;
    TPoisonPillHelper PoisonPillHelper;

    NActors::TActorId TabletBootInfoBackup;

public:
    explicit THiveProxyFallbackActor(THiveProxyConfig config);

    void Bootstrap(const NActors::TActorContext& ctx);

private:
    STFUNC(StateWork);
    STFUNC(StateShutdown);

    void Poison(const NActors::TActorContext& ctx) override;

    void HandlePoisonPill(
        const NActors::TEvents::TEvPoisonPill::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandlePoisonTaken(
        const NActors::TEvents::TEvPoisonTaken::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleRequestFinished(
        const TEvHiveProxyPrivate::TEvRequestFinished::TPtr& ev,
        const NActors::TActorContext& ctx);

    bool HandleRequests(STFUNC_SIG);

    STORAGE_HIVE_PROXY_REQUESTS(STORAGE_IMPLEMENT_REQUEST, TEvHiveProxy)
};

}   // namespace NCloud::NStorage
