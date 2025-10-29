#pragma once

#include "public.h"

#include <cloud/storage/core/libs/api/ss_proxy.h>
#include <cloud/storage/core/libs/common/public.h>
#include <cloud/storage/core/libs/kikimr/helpers.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/hfunc.h>

namespace NCloud::NStorage {

////////////////////////////////////////////////////////////////////////////////

class TSSProxyFallbackActor final
    : public NActors::TActorBootstrapped<TSSProxyFallbackActor>
{
private:
    const TSSProxyConfig Config;

    NActors::TActorId PathDescriptionBackup;

public:
    explicit TSSProxyFallbackActor(TSSProxyConfig config);

    void Bootstrap(const NActors::TActorContext& ctx);

private:
    STFUNC(StateWork);

    bool HandleRequests(STFUNC_SIG);

    STORAGE_SS_PROXY_REQUESTS(STORAGE_IMPLEMENT_REQUEST, TEvSSProxy)
};

}   // namespace NCloud::NStorage
