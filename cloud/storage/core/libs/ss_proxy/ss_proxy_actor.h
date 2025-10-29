#pragma once

#include "public.h"

#include <cloud/storage/core/libs/api/ss_proxy.h>
#include <cloud/storage/core/libs/kikimr/helpers.h>

#include <ydb/core/tablet/tablet_pipe_client_cache.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/log.h>

#include <util/generic/hash.h>
#include <util/generic/deque.h>

namespace NCloud::NStorage {

////////////////////////////////////////////////////////////////////////////////

class TSSProxyActor final
    : public NActors::TActorBootstrapped<TSSProxyActor>
{
public:
    struct TRequestInfo
    {
        NActors::TActorId Sender;
        ui64 Cookie = 0;

        TRequestInfo() = default;
        TRequestInfo(const TRequestInfo&) = default;
        TRequestInfo& operator=(const TRequestInfo&) = default;

        TRequestInfo(NActors::TActorId sender, ui64 cookie)
            : Sender(sender)
            , Cookie(cookie)
        {}
    };

private:
    struct TSchemeShardState
    {
        NActors::TActorId ReplyProxy;
        THashMap<ui64, TDeque<TRequestInfo>> TxToRequests;
    };

private:
    const TSSProxyConfig Config;

    NActors::TActorId PathDescriptionBackup;

    std::unique_ptr<NKikimr::NTabletPipe::IClientCache> ClientCache;
    THashMap<ui64, TSchemeShardState> SchemeShardStates;

public:
    explicit TSSProxyActor(TSSProxyConfig config);

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

    STORAGE_SS_PROXY_REQUESTS(STORAGE_IMPLEMENT_REQUEST, TEvSSProxy)

    STFUNC(StateWork);
};

}   // namespace NCloud::NStorage
