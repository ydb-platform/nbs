#pragma once

#include "public.h"

#include "hive_proxy_events_private.h"

#include <cloud/storage/core/libs/api/hive_proxy.h>
#include <cloud/storage/core/libs/common/public.h>
#include <cloud/storage/core/libs/kikimr/helpers.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/hfunc.h>

namespace NCloud::NStorage {

////////////////////////////////////////////////////////////////////////////////

class THiveProxyFallbackActor final
    : public NActors::TActorBootstrapped<THiveProxyFallbackActor>
{
private:
    const THiveProxyConfig Config;

    NActors::TActorId TabletBootInfoBackup;

public:
    explicit THiveProxyFallbackActor(THiveProxyConfig config);

    void Bootstrap(const NActors::TActorContext& ctx);

private:
    STFUNC(StateWork);

    bool HandleRequests(STFUNC_SIG);

    STORAGE_HIVE_PROXY_REQUESTS(STORAGE_IMPLEMENT_REQUEST, TEvHiveProxy)
};

}   // namespace NCloud::NStorage
