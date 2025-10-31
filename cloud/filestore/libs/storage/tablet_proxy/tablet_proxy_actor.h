#pragma once

#include "public.h"

#include <cloud/filestore/libs/storage/api/ss_proxy.h>
#include <cloud/filestore/libs/storage/api/tablet_proxy.h>
#include <cloud/filestore/libs/storage/core/config.h>
#include <cloud/filestore/libs/storage/core/request_info.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/kikimr/helpers.h>

#include <ydb/core/tablet/tablet_pipe_client_cache.h>
#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/core/protos/filestore_config.pb.h>

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/log.h>

#include <util/datetime/base.h>
#include <util/generic/deque.h>
#include <util/generic/string.h>
#include <util/generic/hash.h>

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

class TIndexTabletProxyActor final
    : public NActors::TActor<TIndexTabletProxyActor>
{
    enum EConnectionState
    {
        INITIAL = 0,
        RESOLVING = 1,
        STARTED = 2,
        STOPPED = 3,
        FAILED = 4,
    };

    struct TConnection
    {
        const ui64 Id;
        const TString FileSystemId;

        EConnectionState State = INITIAL;
        ui64 TabletId = 0;
        ui32 Generation = 0;
        NProto::TError Error;
        TString Path;

        TDeque<NActors::IEventHandlePtr> Requests;

        TConnection(ui64 id, TString fileSystemId)
            : Id(id)
            , FileSystemId(std::move(fileSystemId))
        {}
    };

    struct TActiveRequest
    {
        const ui64 ConnectionId;
        NActors::IEventHandlePtr Request;

        TActiveRequest(ui64 connectionId, NActors::IEventHandlePtr request)
            : ConnectionId(connectionId)
            , Request(std::move(request))
        {}
    };

    using TActiveRequestMap = THashMap<ui64, TActiveRequest>;

private:
    const TStorageConfigPtr Config;

    ui64 ConnectionId = 0;
    THashMap<TString, TConnection> Connections;
    THashMap<ui64, TConnection*> ConnectionById;
    THashMap<ui64, TConnection*> ConnectionByTablet;

    ui64 RequestId = 0;
    TActiveRequestMap ActiveRequests;

    std::unique_ptr<NKikimr::NTabletPipe::IClientCache> ClientCache;

public:
    TIndexTabletProxyActor(TStorageConfigPtr config);

private:
    TConnection& CreateConnection(const TString& fileSystemId);

    void StartConnection(
        const NActors::TActorContext& ctx,
        TConnection& conn,
        ui64 tabletId,
        const TString& path);

    void DestroyConnection(
        const NActors::TActorContext& ctx,
        TConnection& conn,
        const NProto::TError& error);

    void OnConnectionError(
        const NActors::TActorContext& ctx,
        TConnection& conn,
        const NProto::TError& error);

    void ProcessPendingRequests(
        const NActors::TActorContext& ctx,
        TConnection& conn);

    void CancelActiveRequests(TConnection& conn);

    void PostponeRequest(
        const NActors::TActorContext& ctx,
        TConnection& conn,
        NActors::IEventHandlePtr ev);

    template <typename TMethod>
    void ForwardRequest(
        const NActors::TActorContext& ctx,
        TConnection& conn,
        const typename TMethod::TRequest::TPtr& ev);

    void DescribeFileStore(
        const NActors::TActorContext& ctx,
        TConnection& conn);

private:
    void HandleClientConnected(
        NKikimr::TEvTabletPipe::TEvClientConnected::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleClientDestroyed(
        NKikimr::TEvTabletPipe::TEvClientDestroyed::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleDescribeFileStoreResponse(
        const TEvSSProxy::TEvDescribeFileStoreResponse::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandlePoisonPill(
        const NActors::TEvents::TEvPoisonPill::TPtr& ev,
        const NActors::TActorContext& ctx);

    template <typename TMethod>
    void HandleRequest(
        const NActors::TActorContext& ctx,
        const typename TMethod::TRequest::TPtr& ev);

    void HandleResponse(
        const NActors::TActorContext& ctx,
        TAutoPtr<NActors::IEventHandle>& ev);

    bool HandleRequests(STFUNC_SIG);
    bool LogLateMessage(STFUNC_SIG);

    STFUNC(StateWork);
};

}   // namespace NCloud::NFileStore::NStorage
