#pragma once

#include "public.h"

#include <cloud/filestore/libs/storage/api/ss_proxy.h>
#include <cloud/filestore/libs/storage/core/public.h>
#include <cloud/filestore/libs/storage/core/request_info.h>

#include <cloud/storage/core/libs/kikimr/helpers.h>

#include <ydb/core/tablet/tablet_pipe_client_cache.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>

#include <library/cpp/actors/core/actor.h>
#include <library/cpp/actors/core/events.h>
#include <library/cpp/actors/core/hfunc.h>
#include <library/cpp/actors/core/log.h>

#include <util/generic/hash.h>
#include <util/generic/deque.h>

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

class TSSProxyActor final
    : public NActors::TActor<TSSProxyActor>
{
    struct TSchemeShardState
    {
        NActors::TActorId ReplyProxy;
        THashMap<ui64, TDeque<TRequestInfoPtr>> TxToRequests;
    };

private:
    const TStorageConfigPtr Config;
    std::unique_ptr<NKikimr::NTabletPipe::IClientCache> ClientCache;
    THashMap<ui64, TSchemeShardState> SchemeShardStates;

public:
    TSSProxyActor(TStorageConfigPtr config);

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
    void HandleConnect(
        NKikimr::TEvTabletPipe::TEvClientConnected::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleDisconnect(
        NKikimr::TEvTabletPipe::TEvClientDestroyed::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleTxRegistered(
        const NKikimr::NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletionRegistered::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleTxResult(
        const NKikimr::NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletionResult::TPtr& ev,
        const NActors::TActorContext& ctx);

    bool HandleRequests(STFUNC_SIG);

    FILESTORE_SS_PROXY_REQUESTS(FILESTORE_IMPLEMENT_REQUEST, TEvSSProxy)

    STFUNC(StateWork);
};

}   // namespace NCloud::NFileStore::NStorage
