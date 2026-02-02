#pragma once

#include "public.h"

#include <cloud/blockstore/libs/kikimr/helpers.h>
#include <cloud/blockstore/libs/storage/api/ss_proxy.h>
#include <cloud/blockstore/libs/storage/core/public.h>
#include <cloud/blockstore/libs/storage/core/request_info.h>

#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>
#include <contrib/ydb/library/actors/core/events.h>
#include <contrib/ydb/library/actors/core/hfunc.h>
#include <contrib/ydb/library/actors/core/log.h>

#include <util/generic/hash.h>
#include <util/generic/deque.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

class TSSProxyActor final
    : public NActors::TActorBootstrapped<TSSProxyActor>
{

private:
    const TStorageConfigPtr Config;
    NActors::TActorId PathDescriptionBackup;

    NActors::TActorId StorageSSProxyActor;
public:
    explicit TSSProxyActor(TStorageConfigPtr config);

    void Bootstrap(const NActors::TActorContext& ctx);

private:
    void SendWaitTxRequest(
        const NActors::TActorContext& ctx,
        ui64 schemeShard,
        ui64 txId);

    void OnConnectionError(
        const NActors::TActorContext& ctx,
        const NProto::TError& error,
        ui64 schemeShard);

private:
    STFUNC(StateWork);

    void HandleDescribeScheme(
        const TEvStorageSSProxy::TEvDescribeSchemeRequest::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleModifyScheme(
        const TEvStorageSSProxy::TEvModifySchemeRequest::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleWaitSchemeTx(
        const TEvStorageSSProxy::TEvWaitSchemeTxRequest::TPtr& ev,
        const NActors::TActorContext& ctx);

    bool HandleRequests(STFUNC_SIG);

    BLOCKSTORE_SS_PROXY_REQUESTS(BLOCKSTORE_IMPLEMENT_REQUEST, TEvSSProxy)
};

////////////////////////////////////////////////////////////////////////////////

NProto::TError GetErrorFromPreconditionFailed(const NProto::TError& error);
NProto::TError TranslateTxProxyError(NProto::TError error);

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<TEvStorageSSProxy::TEvModifySchemeRequest> CreateModifySchemeRequestForAlterVolume(
    TString path,
    ui64 pathId,
    ui64 version,
    const NKikimrBlockStore::TVolumeConfig& volumeConfig);

}   // namespace NCloud::NBlockStore::NStorage
