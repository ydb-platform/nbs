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

#include <util/generic/hash.h>
#include <util/generic/deque.h>

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

class TSSProxyActor final
    : public NActors::TActorBootstrapped<TSSProxyActor>
{
private:
    const TStorageConfigPtr Config;

    NActors::TActorId StorageSSProxy;

public:
    explicit TSSProxyActor(TStorageConfigPtr config);

    void Bootstrap(const NActors::TActorContext& ctx);

private:
    bool HandleRequests(STFUNC_SIG);

    void HandleDescribeScheme(
        const TEvStorageSSProxy::TEvDescribeSchemeRequest::TPtr& ev,
        const NActors::TActorContext& ctx);
    void HandleModifyScheme(
        const TEvStorageSSProxy::TEvModifySchemeRequest::TPtr& ev,
        const NActors::TActorContext& ctx);
    void HandleBackupPathDescriptions(
        const TEvStorageSSProxy::TEvBackupPathDescriptionsRequest::TPtr& ev,
        const NActors::TActorContext& ctx);

    FILESTORE_SS_PROXY_REQUESTS(FILESTORE_IMPLEMENT_REQUEST, TEvSSProxy)

    STFUNC(StateWork);
};

}   // namespace NCloud::NFileStore::NStorage
