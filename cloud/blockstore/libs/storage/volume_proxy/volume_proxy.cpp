#include "volume_proxy.h"

#include <cloud/blockstore/libs/kikimr/helpers.h>
#include <cloud/blockstore/libs/storage/api/service.h>
#include <cloud/blockstore/libs/storage/api/ss_proxy.h>
#include <cloud/blockstore/libs/storage/api/volume.h>
#include <cloud/blockstore/libs/storage/core/config.h>
#include <cloud/blockstore/libs/storage/core/probes.h>
#include <cloud/blockstore/libs/storage/core/proto_helpers.h>

#include <cloud/storage/core/libs/diagnostics/trace_serializer.h>

#include <contrib/ydb/core/tablet/tablet_pipe_client_cache.h>

#include <contrib/ydb/library/actors/core/actor.h>
#include <contrib/ydb/library/actors/core/hfunc.h>
#include <contrib/ydb/library/actors/core/log.h>
#include <library/cpp/lwtrace/shuttle.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

using namespace NKikimr;

LWTRACE_USING(BLOCKSTORE_STORAGE_PROVIDER);

namespace {

////////////////////////////////////////////////////////////////////////////////

Y_HAS_MEMBER(GetThrottlerDelay);

////////////////////////////////////////////////////////////////////////////////

constexpr TDuration PipeInactivityTimeout = TDuration::Minutes(1);

std::unique_ptr<NTabletPipe::IClientCache> CreateTabletPipeClientCache(
    const TStorageConfig& config)
{
    NTabletPipe::TClientConfig clientConfig;
    clientConfig.RetryPolicy = {
        .RetryLimitCount = config.GetPipeClientRetryCount(),
        .MinRetryTime = config.GetPipeClientMinRetryTime(),
        .MaxRetryTime = config.GetPipeClientMaxRetryTime()
    };

    return std::unique_ptr<NTabletPipe::IClientCache>(
        NTabletPipe::CreateUnboundedClientCache(clientConfig));
}

////////////////////////////////////////////////////////////////////////////////

class TVolumeProxyActor final
    : public TActor<TVolumeProxyActor>
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
        const TString DiskId;

        EConnectionState State = INITIAL;
        ui64 TabletId = 0;
        ui32 Generation = 0;
        NProto::TError Error;
        TString Path;

        TDeque<IEventHandlePtr> Requests;
        TInstant LastActivity;
        ui64 RequestsInflight = 0;
        bool ActivityCheckScheduled = false;

        TConnection(ui64 id, TString diskId)
            : Id(id)
            , DiskId(std::move(diskId))
        {}
    };

    struct TActiveRequest
    {
        const ui64 ConnectionId;
        IEventHandlePtr Request;
        TCallContextPtr CallContext;
        ui64 SendTime;

        TActiveRequest(
                ui64 connectionId,
                IEventHandlePtr request,
                TCallContextPtr callContext,
                ui64 sendTime)
            : ConnectionId(connectionId)
            , Request(std::move(request))
            , CallContext(std::move(callContext))
            , SendTime(sendTime)
        {}
    };

    using TActiveRequestMap = THashMap<ui64, TActiveRequest>;

private:
    const TStorageConfigPtr Config;
    const ITraceSerializerPtr TraceSerializer;

    ui64 ConnectionId = 0;
    THashMap<TString, TConnection> Connections;
    THashMap<ui64, TConnection*> ConnectionById;
    THashMap<ui64, TConnection*> ConnectionByTablet;

    struct TBaseTabletId
    {
        TBaseTabletId(ui64 tabletId, int refCount)
            : TabletId(tabletId)
            , RefCount(refCount)
        {}

        TInstant DisconnectTs;
        ui64 TabletId = 0;
        int RefCount = 0;
    };

    THashMap<TString, TBaseTabletId> BaseDiskIdToTabletId;

    ui64 RequestId = 0;
    TActiveRequestMap ActiveRequests;

    std::unique_ptr<NTabletPipe::IClientCache> ClientCache;

public:
    TVolumeProxyActor(
        TStorageConfigPtr config,
        ITraceSerializerPtr traceSerializer);

private:
    TConnection& CreateConnection(const TString& diskId);

    void StartConnection(
        const TActorContext& ctx,
        TConnection& conn,
        ui64 tabletId,
        const TString& path);

    void DestroyConnection(
        const TActorContext& ctx,
        TConnection& conn,
        const NProto::TError& error);

    void OnConnectionError(
        const TActorContext& ctx,
        TConnection& conn,
        const NProto::TError& error);

    void ProcessPendingRequests(
        const TActorContext& ctx,
        TConnection& conn);

    void CancelActiveRequests(
        const TActorContext& ctx,
        TConnection& conn);

    void PostponeRequest(
        const TActorContext& ctx,
        TConnection& conn,
        IEventHandlePtr ev);

    template <typename TMethod>
    void ForwardRequest(
        const TActorContext& ctx,
        TConnection& conn,
        const typename TMethod::TRequest::TPtr& ev);

    void DescribeVolume(
        const TActorContext& ctx,
        TConnection& conn);

private:
    void ScheduleConnectionShutdown(
        const TActorContext& ctx,
        TConnection& conn);

    STFUNC(StateWork);

    void HandleConnect(
        TEvTabletPipe::TEvClientConnected::TPtr& ev,
        const TActorContext& ctx);

    void HandleDisconnect(
        TEvTabletPipe::TEvClientDestroyed::TPtr& ev,
        const TActorContext& ctx);

    void HandleDescribeResponse(
        const TEvSSProxy::TEvDescribeVolumeResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandleWakeup(
        const TEvents::TEvWakeup::TPtr& ev,
        const TActorContext& ctx);

    void HandlePoisonPill(
        const TEvents::TEvPoisonPill::TPtr& ev,
        const TActorContext& ctx);

    template <typename TMethod>
    void HandleRequest(
        const TActorContext& ctx,
        const typename TMethod::TRequest::TPtr& ev);

    template <typename TMethod>
    void HandleResponse(
        const TActorContext& ctx,
        const typename TMethod::TResponse::TPtr& ev);

    bool HandleRequests(STFUNC_SIG);
    bool LogLateMessage(ui32 evType, const TActorContext& ctx);

    void HandleMapBaseDiskIdToTabletId(
        const TEvVolume::TEvMapBaseDiskIdToTabletId::TPtr& ev,
        const TActorContext& ctx);

    void HandleClearBaseDiskIdToTabletIdMapping(
        const TEvVolume::TEvClearBaseDiskIdToTabletIdMapping::TPtr& ev,
        const TActorContext& ctx);
};

////////////////////////////////////////////////////////////////////////////////

TVolumeProxyActor::TVolumeProxyActor(
        TStorageConfigPtr config,
        ITraceSerializerPtr traceSerializer)
    : TActor(&TThis::StateWork)
    , Config(std::move(config))
    , TraceSerializer(std::move(traceSerializer))
    , ClientCache(CreateTabletPipeClientCache(*Config))
{
}

TVolumeProxyActor::TConnection& TVolumeProxyActor::CreateConnection(
    const TString& diskId)
{
    if (TConnection* conn = Connections.FindPtr(diskId)) {
        return *conn;
    }

    ui64 connectionId = ++ConnectionId;

    auto ins = Connections.emplace(diskId, TConnection(connectionId, diskId));
    Y_ABORT_UNLESS(ins.second);

    ConnectionById[connectionId] = &ins.first->second;
    return ins.first->second;
}

void TVolumeProxyActor::StartConnection(
    const TActorContext& ctx,
    TConnection& conn,
    ui64 tabletId,
    const TString& path)
{
    LOG_DEBUG(ctx, TBlockStoreComponents::VOLUME_PROXY,
        "Volume with diskId %s and path %s resolved: %lu",
        conn.DiskId.Quote().data(),
        path.Quote().data(),
        tabletId);

    ConnectionByTablet[tabletId] = &conn;

    conn.State = STARTED;
    conn.TabletId = tabletId;
    conn.Path = path;
    ++conn.Generation;

    ProcessPendingRequests(ctx, conn);
}

void TVolumeProxyActor::DestroyConnection(
    const TActorContext& ctx,
    TConnection& conn,
    const NProto::TError& error)
{
    conn.State = STOPPED;
    conn.Error = error;

    ProcessPendingRequests(ctx, conn);

    ConnectionById.erase(conn.Id);
    ConnectionByTablet.erase(conn.TabletId);
    Connections.erase(conn.DiskId);
}

void TVolumeProxyActor::OnConnectionError(
    const TActorContext& ctx,
    TConnection& conn,
    const NProto::TError& error)
{
    LOG_WARN(ctx, TBlockStoreComponents::VOLUME_PROXY,
        "Connection to volume with diskId %s and path %s failed: %s",
        conn.DiskId.Quote().data(),
        conn.Path.Quote().data(),
        FormatError(error).data());

    // will wait for tablet to recover
    conn.State = FAILED;

    CancelActiveRequests(ctx, conn);
    ProcessPendingRequests(ctx, conn);
}

void TVolumeProxyActor::ProcessPendingRequests(
    const TActorContext&,
    TConnection& conn)
{
    auto requests = std::move(conn.Requests);

    for (auto& ev: requests) {
        TAutoPtr<IEventHandle> handle(ev.release());
        Receive(handle);
    }
}

void TVolumeProxyActor::CancelActiveRequests(
    const TActorContext& ctx,
    TConnection& conn)
{
    for (auto it = ActiveRequests.begin(); it != ActiveRequests.end(); ) {
        if (it->second.ConnectionId == conn.Id) {
            TAutoPtr<IEventHandle> handle(it->second.Request.release());
            Receive(handle);

            ActiveRequests.erase(it++);
        } else {
            ++it;
        }
    }
    conn.RequestsInflight = 0;
    conn.LastActivity = ctx.Now();
}

void TVolumeProxyActor::PostponeRequest(
    const TActorContext& ctx,
    TConnection& conn,
    IEventHandlePtr ev)
{
    Y_UNUSED(ctx);

    conn.Requests.emplace_back(std::move(ev));
}

template <typename TMethod>
void TVolumeProxyActor::ForwardRequest(
    const TActorContext& ctx,
    TConnection& conn,
    const typename TMethod::TRequest::TPtr& ev)
{
    auto* msg = ev->Get();

    auto clientId = ClientCache->Prepare(ctx, conn.TabletId);

    LOG_TRACE(ctx, TBlockStoreComponents::VOLUME_PROXY,
        "Forward request to volume: %lu (remote: %s)",
        conn.TabletId,
        ToString(clientId).data());

    ui64 requestId = ++RequestId;

    SetRequestGeneration(conn.Generation, *ev->Get());

    TraceSerializer->BuildTraceRequest(
        *msg->Record.MutableHeaders()->MutableInternal()->MutableTrace(),
        msg->CallContext->LWOrbit);

    auto event = std::make_unique<IEventHandle>(
        ev->Recipient,
        SelfId(),
        ev->ReleaseBase().Release(),
        0,          // flags
        requestId  // cookie
    );

    ActiveRequests.emplace(
        requestId,
        TActiveRequest(
            conn.Id,
            IEventHandlePtr(ev.Release()),
            msg->CallContext,
            GetCycleCount()));

    LWTRACK(
        RequestSentPipe,
        msg->CallContext->LWOrbit,
        TMethod::Name,
        msg->CallContext->RequestId);

    NCloud::PipeSend(ctx, clientId, std::move(event));
    ++conn.RequestsInflight;
}

void TVolumeProxyActor::DescribeVolume(
    const TActorContext& ctx,
    TConnection& conn)
{
    LOG_DEBUG(ctx, TBlockStoreComponents::VOLUME_PROXY,
        "Query volume for diskId: %s",
        conn.DiskId.Quote().data());

    NCloud::Send(
        ctx,
        MakeSSProxyServiceId(),
        std::make_unique<TEvSSProxy::TEvDescribeVolumeRequest>(conn.DiskId),
        conn.Id);
}

////////////////////////////////////////////////////////////////////////////////

void TVolumeProxyActor::ScheduleConnectionShutdown(
    const TActorContext& ctx,
    TConnection& conn)
{
    if (!conn.ActivityCheckScheduled &&
        conn.Requests.empty() &&
        !conn.RequestsInflight)
    {
        conn.ActivityCheckScheduled = true;
        ctx.Schedule(PipeInactivityTimeout, new TEvents::TEvWakeup(conn.Id));
    }
}

void TVolumeProxyActor::HandleWakeup(
    const TEvents::TEvWakeup::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    auto it = ConnectionById.find(msg->Tag);
    if (it == ConnectionById.end()) {
        // connection is already closed, nothing to do
        return;
    }
    TConnection* conn = it->second;
    conn->ActivityCheckScheduled = false;
    auto now = ctx.Now();

    if (conn->Requests.empty() &&
        !conn->RequestsInflight &&
        conn->LastActivity < now - PipeInactivityTimeout)
    {
        LOG_INFO(ctx, TBlockStoreComponents::VOLUME_PROXY,
            "Remove connection to tablet %lu for disk %s",
            conn->TabletId,
            conn->DiskId.Quote().data());
        ClientCache->Shutdown(ctx, conn->TabletId);
        ConnectionById.erase(conn->Id);
        ConnectionByTablet.erase(conn->TabletId);
        Connections.erase(conn->DiskId);
    } else {
        if (conn->LastActivity >= now - PipeInactivityTimeout) {
            auto timeEstimate = conn->LastActivity + PipeInactivityTimeout - now;
            ctx.Schedule(timeEstimate, new TEvents::TEvWakeup(conn->Id));
            conn->ActivityCheckScheduled = true;
        }
    }
}

void TVolumeProxyActor::HandleConnect(
    TEvTabletPipe::TEvClientConnected::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    auto it = ConnectionByTablet.find(msg->TabletId);
    if (it == ConnectionByTablet.end()) {
        return;
    }

    TConnection* conn = it->second;
    Y_ABORT_UNLESS(conn);

    if (!ClientCache->OnConnect(ev)) {
        auto error = MakeKikimrError(msg->Status, "Could not connect");
        LOG_ERROR(ctx, TBlockStoreComponents::VOLUME_PROXY,
            "Cannot connect to tablet %lu: %s",
            msg->TabletId,
            FormatError(error).data());

        if (auto it = BaseDiskIdToTabletId.find(conn->DiskId);
            it != BaseDiskIdToTabletId.end() && !it->second.DisconnectTs)
        {
            it->second.DisconnectTs = ctx.Now();
        }

        CancelActiveRequests(ctx, *conn);
        DestroyConnection(ctx, *conn, error);
        return;
    }

    if (auto it = BaseDiskIdToTabletId.find(conn->DiskId);
        it != BaseDiskIdToTabletId.end())
    {
        it->second.DisconnectTs = {};
    }

    if (conn->State == FAILED) {
        // Tablet recovered
        conn->State = STARTED;
        ++conn->Generation;
    }
}

void TVolumeProxyActor::HandleDisconnect(
    TEvTabletPipe::TEvClientDestroyed::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    auto it = ConnectionByTablet.find(msg->TabletId);
    if (it == ConnectionByTablet.end()) {
        return;
    }

    TConnection* conn = it->second;
    Y_ABORT_UNLESS(conn);

    ClientCache->OnDisconnect(ev);

    auto error = MakeError(E_REJECTED, "Connection broken");
    OnConnectionError(ctx, *conn, error);
}

void TVolumeProxyActor::HandleDescribeResponse(
    const TEvSSProxy::TEvDescribeVolumeResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    auto it = ConnectionById.find(ev->Cookie);
    if (it == ConnectionById.end()) {
        return;
    }

    TConnection* conn = it->second;
    Y_ABORT_UNLESS(conn);

    const auto& error = msg->GetError();
    if (FAILED(error.GetCode())) {
        LOG_ERROR(ctx, TBlockStoreComponents::VOLUME_PROXY,
            "Could not resolve path for volume %s: %s",
            conn->DiskId.Quote().data(),
            FormatError(error).data());

        DestroyConnection(ctx, *conn, error);
        return;
    }

    const auto& pathDescr = msg->PathDescription;
    const auto& volumeDescr = pathDescr.GetBlockStoreVolumeDescription();
    StartConnection(
        ctx,
        *conn,
        volumeDescr.GetVolumeTabletId(),
        msg->Path);

    if (auto it = BaseDiskIdToTabletId.find(conn->DiskId);
        it != BaseDiskIdToTabletId.end())
    {
        it->second.DisconnectTs = {};
    }
}

template <typename TMethod>
void TVolumeProxyActor::HandleRequest(
    const TActorContext& ctx,
    const typename TMethod::TRequest::TPtr& ev)
{
    // ActiveRequests contains IEventHandles without embedded messages,
    // only information about event type and sender. When we detect failure
    // in the pipe, we re-send all IEventHandle's from ActiveRequests
    // to ourself, enabling standard path for message processing.
    // So we need to handle the cases when message is re-sent or just came from
    // outside. We have to to check if message buffer is present
    // otherwise it is safe to use Get() to retrieve actual message.
    if (!ev->HasBuffer() && !ev->HasEvent()) {
        auto response = std::make_unique<typename TMethod::TResponse>(
            MakeTabletIsDeadError(E_REJECTED, __LOCATION__));

        NCloud::Reply(ctx, *ev, std::move(response));
        return;
    }

    const auto* msg = ev->Get();

    const TString& diskId = GetDiskId(*msg);

    TConnection& conn = CreateConnection(diskId);
    switch (conn.State) {
        case INITIAL:
        case FAILED:
        {
            auto itr = BaseDiskIdToTabletId.find(diskId);
            if (itr != BaseDiskIdToTabletId.end()) {
                auto deadline =
                    itr->second.DisconnectTs + Config->GetVolumeProxyCacheRetryDuration();
                if (!itr->second.DisconnectTs || deadline > ctx.Now()) {
                    PostponeRequest(ctx, conn, IEventHandlePtr(ev.Release()));
                    StartConnection(
                        ctx,
                        conn,
                        itr->second.TabletId,
                        "PartitionConfig");
                    break;
                }
            }

            conn.State = RESOLVING;
            DescribeVolume(ctx, conn);

            // pass-through
        }
        case RESOLVING:
            PostponeRequest(ctx, conn, IEventHandlePtr(ev.Release()));
            break;

        case STARTED:
            ForwardRequest<TMethod>(ctx, conn, ev);
            break;

        case STOPPED: {
            auto response = std::make_unique<typename TMethod::TResponse>(
                conn.Error);

            NCloud::Reply(ctx, *ev, std::move(response));
            break;
        }
    }
}

template <typename TMethod>
void TVolumeProxyActor::HandleResponse(
    const TActorContext& ctx,
    const typename TMethod::TResponse::TPtr& ev)
{
    auto it = ActiveRequests.find(ev->Cookie);
    if (it == ActiveRequests.end()) {
        // ActiveRequests are cleared upon connection reset
        LogLateMessage(ev->GetTypeRewrite(), ctx);
        return;
    }

    auto* msg = ev->Get();

    if (it->second.CallContext->LWOrbit.HasShuttles()) {
        TraceSerializer->HandleTraceInfo(
            msg->Record.GetTrace(),
            it->second.CallContext->LWOrbit,
            it->second.SendTime,
            GetCycleCount());
        msg->Record.ClearTrace();
    }

    using TProtoType = decltype(TMethod::TResponse::Record);
    if constexpr (THasGetThrottlerDelay<TProtoType>::value) {
        it->second.CallContext->AddTime(
            EProcessingStage::Postponed,
            TDuration::MicroSeconds(msg->Record.GetThrottlerDelay()));
        msg->Record.SetThrottlerDelay(0);
        it->second.CallContext->SetPossiblePostponeDuration(TDuration::Zero());
    }

    LWTRACK(
        ResponseReceivedPipe,
        it->second.CallContext->LWOrbit,
        TMethod::Name,
        it->second.CallContext->RequestId);

    // forward response to the caller
    std::unique_ptr<IEventHandle> event;
    if (ev->HasEvent()) {
        event = std::make_unique<IEventHandle>(
            it->second.Request->Sender,
            ev->Sender,
            ev->ReleaseBase().Release(),
            ev->Flags,
            it->second.Request->Cookie);
    } else {
        event = std::make_unique<IEventHandle>(
            ev->Type,
            ev->Flags,
            it->second.Request->Sender,
            ev->Sender,
            ev->ReleaseChainBuffer(),
            it->second.Request->Cookie);
    }

    auto* conn = ConnectionById[it->second.ConnectionId];
    Y_ABORT_UNLESS(conn);

    conn->LastActivity = ctx.Now();
    --conn->RequestsInflight;
    if (conn->Requests.empty() &&
        !conn->RequestsInflight) {
        ScheduleConnectionShutdown(ctx, *conn);
    }

    ctx.Send(event.release());
    ActiveRequests.erase(it);
}

void TVolumeProxyActor::HandleMapBaseDiskIdToTabletId(
    const TEvVolume::TEvMapBaseDiskIdToTabletId::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ctx);
    const auto* msg = ev->Get();
    auto [it, inserted] = BaseDiskIdToTabletId.try_emplace(
        msg->BaseDiskId, msg->BaseTabletId, 1);
    if (!inserted) {
        ++it->second.RefCount;
    }
}

void TVolumeProxyActor::HandleClearBaseDiskIdToTabletIdMapping(
    const TEvVolume::TEvClearBaseDiskIdToTabletIdMapping::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ctx);
    const auto* msg = ev->Get();
    auto itr = BaseDiskIdToTabletId.find(msg->BaseDiskId);
    if (itr != BaseDiskIdToTabletId.end()) {
        --itr->second.RefCount;
        if (itr->second.RefCount == 0) {
            BaseDiskIdToTabletId.erase(itr);
        }
    }
}

void TVolumeProxyActor::HandlePoisonPill(
    const TEvents::TEvPoisonPill::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);
    for (const auto& conn : Connections) {
        ClientCache->Shutdown(ctx, conn.second.TabletId);
    }
}

bool TVolumeProxyActor::HandleRequests(STFUNC_SIG)
{
    auto ctx(ActorContext());
#define BLOCKSTORE_HANDLE_METHOD(name, ns)                                     \
    case ns::TEv##name##Request::EventType: {                                  \
        auto* x = reinterpret_cast<ns::TEv##name##Request::TPtr*>(&ev);        \
        HandleRequest<ns::T##name##Method>(ctx, *x);                           \
        break;                                                                 \
    }                                                                          \
    case ns::TEv##name##Response::EventType: {                                 \
        auto* x = reinterpret_cast<ns::TEv##name##Response::TPtr*>(&ev);       \
        HandleResponse<ns::T##name##Method>(ctx, *x);                          \
        break;                                                                 \
    }                                                                          \
// BLOCKSTORE_HANDLE_METHOD

    switch (ev->GetTypeRewrite()) {
        BLOCKSTORE_VOLUME_REQUESTS(BLOCKSTORE_HANDLE_METHOD, TEvVolume)
        BLOCKSTORE_VOLUME_REQUESTS_FWD_SERVICE(BLOCKSTORE_HANDLE_METHOD, TEvService)

        default:
            return false;
    }

    return true;

#undef BLOCKSTORE_HANDLE_METHOD
}

bool TVolumeProxyActor::LogLateMessage(ui32 evType, const TActorContext& ctx)
{
#define BLOCKSTORE_LOG_MESSAGE(name, ns)                                       \
    case ns::TEv##name##Request::EventType: {                                  \
        LOG_ERROR(ctx, TBlockStoreComponents::VOLUME_PROXY,                    \
            "Late request : (0x%08X) %s request",                              \
            evType,                                                            \
            #name);                                                            \
        break;                                                                 \
    }                                                                          \
    case ns::TEv##name##Response::EventType: {                                 \
        LOG_DEBUG(ctx, TBlockStoreComponents::VOLUME_PROXY,                    \
          "Late response : (0x%08X) %s response",                              \
          evType,                                                              \
          #name);                                                              \
        break;                                                                 \
    }                                                                          \
// BLOCKSTORE_LOG_MESSAGE

    switch (evType) {
        BLOCKSTORE_VOLUME_REQUESTS(BLOCKSTORE_LOG_MESSAGE, TEvVolume)
        BLOCKSTORE_VOLUME_REQUESTS_FWD_SERVICE(BLOCKSTORE_LOG_MESSAGE, TEvService)

        default:
            return false;
    }

    return true;

#undef BLOCKSTORE_LOG_MESSAGE
}


STFUNC(TVolumeProxyActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvTabletPipe::TEvClientConnected, HandleConnect);
        HFunc(TEvTabletPipe::TEvClientDestroyed, HandleDisconnect);
        HFunc(TEvents::TEvWakeup, HandleWakeup);
        HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);
        HFunc(
            TEvVolume::TEvMapBaseDiskIdToTabletId,
            HandleMapBaseDiskIdToTabletId);
        HFunc(
            TEvVolume::TEvClearBaseDiskIdToTabletIdMapping,
            HandleClearBaseDiskIdToTabletIdMapping);

        HFunc(TEvSSProxy::TEvDescribeVolumeResponse, HandleDescribeResponse);

        default:
            if (!HandleRequests(ev)) {
                HandleUnexpectedEvent(
                    ev,
                    TBlockStoreComponents::VOLUME_PROXY,
                    __PRETTY_FUNCTION__);
            }
            break;
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IActorPtr CreateVolumeProxy(
    TStorageConfigPtr config,
    ITraceSerializerPtr traceSerializer)
{
    return std::make_unique<TVolumeProxyActor>(
        std::move(config),
        std::move(traceSerializer));
}

}   // namespace NCloud::NBlockStore::NStorage
