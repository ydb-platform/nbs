#pragma once

#include "public.h"

#include <cloud/filestore/libs/storage/api/ss_proxy.h>
#include <cloud/filestore/libs/storage/core/public.h>
#include <cloud/filestore/libs/storage/core/request_info.h>

#include <cloud/storage/core/libs/kikimr/helpers.h>

#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>
#include <contrib/ydb/library/actors/core/events.h>
#include <contrib/ydb/library/actors/core/hfunc.h>
#include <contrib/ydb/library/actors/core/log.h>

#include <util/generic/hash.h>
#include <util/generic/deque.h>

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

class TSSProxyActor final
    : public NActors::TActorBootstrapped<TSSProxyActor>
{
private:
    const TStorageConfigPtr Config;

    NActors::TActorId StorageSSProxyActor;

public:
    explicit TSSProxyActor(TStorageConfigPtr config);

    void Bootstrap(const NActors::TActorContext& ctx);

private:
    bool HandleRequests(STFUNC_SIG);

    FILESTORE_SS_PROXY_REQUESTS(FILESTORE_IMPLEMENT_REQUEST, TEvSSProxy)

    STFUNC(StateWork);
};

}   // namespace NCloud::NFileStore::NStorage
