#pragma once

#include "public.h"

#include <cloud/filestore/libs/storage/api/ss_proxy.h>
#include <cloud/filestore/libs/storage/core/public.h>
#include <cloud/filestore/libs/storage/core/request_info.h>

#include <cloud/storage/core/libs/kikimr/helpers.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/log.h>

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

class TSSProxyFallbackActor final
    : public NActors::TActorBootstrapped<TSSProxyFallbackActor>
{
private:
    const TStorageConfigPtr Config;

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

    FILESTORE_SS_PROXY_REQUESTS(FILESTORE_IMPLEMENT_REQUEST, TEvSSProxy)
};

}   // namespace NCloud::NFileStore::NStorage
