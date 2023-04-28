#pragma once

#include "public.h"

#include <cloud/blockstore/libs/kikimr/helpers.h>
#include <cloud/blockstore/libs/storage/api/ss_proxy.h>
#include <cloud/blockstore/libs/storage/core/public.h>
#include <cloud/blockstore/libs/storage/core/request_info.h>

#include <library/cpp/actors/core/actor_bootstrapped.h>
#include <library/cpp/actors/core/events.h>
#include <library/cpp/actors/core/hfunc.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

class TSSProxyFallbackActor final
    : public NActors::TActorBootstrapped<TSSProxyFallbackActor>
{
private:
    const TStorageConfigPtr Config;
    IFileIOServicePtr FileIOService;

    NActors::TActorId PathDescriptionCache;

public:
    TSSProxyFallbackActor(
        TStorageConfigPtr config,
        IFileIOServicePtr fileIO);

    void Bootstrap(const NActors::TActorContext& ctx);

private:
    STFUNC(StateWork);

    bool HandleRequests(STFUNC_SIG);

    BLOCKSTORE_SS_PROXY_REQUESTS(BLOCKSTORE_IMPLEMENT_REQUEST, TEvSSProxy)
};

}   // namespace NCloud::NBlockStore::NStorage
