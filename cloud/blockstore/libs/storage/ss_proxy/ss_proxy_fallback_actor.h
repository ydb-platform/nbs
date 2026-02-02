#pragma once

#include "public.h"

#include <cloud/blockstore/libs/kikimr/helpers.h>
#include <cloud/blockstore/libs/storage/api/ss_proxy.h>
#include <cloud/blockstore/libs/storage/core/public.h>
#include <cloud/blockstore/libs/storage/core/request_info.h>

#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>
#include <contrib/ydb/library/actors/core/events.h>
#include <contrib/ydb/library/actors/core/hfunc.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

class TSSProxyFallbackActor final
    : public NActors::TActorBootstrapped<TSSProxyFallbackActor>
{
private:
    const TStorageConfigPtr Config;

    NActors::TActorId PathDescriptionBackup;
    NActors::TActorId StorageSSProxy;

public:
    explicit TSSProxyFallbackActor(TStorageConfigPtr config);

    void Bootstrap(const NActors::TActorContext& ctx);

private:
    STFUNC(StateWork);

    bool HandleRequests(STFUNC_SIG);

    void HandleDescribeScheme(
        const TEvStorageSSProxy::TEvDescribeSchemeRequest::TPtr& ev,
        const NActors::TActorContext& ctx);
    void HandleModifyScheme(
        const TEvStorageSSProxy::TEvModifySchemeRequest::TPtr& ev,
        const NActors::TActorContext& ctx);

    BLOCKSTORE_SS_PROXY_REQUESTS(BLOCKSTORE_IMPLEMENT_REQUEST, TEvSSProxy)
};

}   // namespace NCloud::NBlockStore::NStorage
