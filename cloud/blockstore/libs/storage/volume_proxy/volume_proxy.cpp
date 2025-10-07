#include "volume_proxy.h"

#include <cloud/blockstore/libs/diagnostics/critical_events.h>
#include <cloud/blockstore/libs/kikimr/helpers.h>
#include <cloud/blockstore/libs/storage/api/service.h>
#include <cloud/blockstore/libs/storage/api/ss_proxy.h>
#include <cloud/blockstore/libs/storage/api/volume.h>
#include <cloud/blockstore/libs/storage/core/config.h>
#include <cloud/blockstore/libs/storage/core/probes.h>
#include <cloud/blockstore/libs/storage/core/proto_helpers.h>
#include <cloud/blockstore/libs/storage/model/log_title.h>

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

// VolumeProxy discovers the volume specified in the DiskId field of request,
// establishes a connection to the tablet of this volume and redirects the
// request to it. VolumeProxy keeps the pipe to the volume tablet and ensures
// that the requests is sent to the tablet. When pipe to volume breaks the
// response "E_REJECTED Tablet is dead" sent to client. If the client stops
// sending messages to the volume, the connection is terminated after a
// PipeInactivityTimeout (1 minute).
//
// Additionally, VolumeProxy tracks the list of base disks tablets. The
// connection to the base disks occurs even if the describe returned an error,
// since the tablet id is already known.

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
        // Numeric connection ID. It is used to find connection by cookie.
        const ui64 Id;

        // ID of the disk that the user is sending requests to. It may differ
        // from the actual disk to which the connection is established if the
        // disk was copied, since a suffix is added to the name of the copied
        // disk.
        const TString DiskId;

        // Id of the disk that was found by the SchemeShard describe.
        // Note: If the disk was a copy the name of the real disk differs
        // from the DiskId.
        TString RealDiskId;

        // An exact name match is required. It is forbidden to connect to a
        // copied disk that has a name with a suffix.
        const bool RequireExactDiskIdMatch = false;

        EConnectionState State = INITIAL;
        bool IsConnectionToBaseDisk = false;
        ui64 TabletId = 0;

        // Generation of the connection established with the volume tablet.
        // Increases with each reconnection that occurs.
        ui32 Generation = 0;

        // An error that should be returned for all requests if the connection
        // to the tablet failed.
        NProto::TError Error;

        // Requests waiting for connection to be established to be sent to the
        // tablet.
        TDeque<IEventHandlePtr> Requests;

        // The time of receiving the last response from the tablet. It is used
        // to break unused connections.
        TInstant LastActivity;

        // The number of requests sent to the tablet, but the responses to which
        // have not yet been received.
        ui64 RequestsInflight = 0;

        // The flag indicates whether an inactivity timeout check was scheduled
        // to terminate the connection.
        bool ActivityCheckScheduled = false;

        TLogTitle LogTitle;

        TConnection(
            ui64 id,
            TString diskId,
            bool requireExactDiskIdMatch,
            bool temporaryServer)
            : Id(id)
            , DiskId(std::move(diskId))
            , RequireExactDiskIdMatch(requireExactDiskIdMatch)
            , LogTitle(
                  GetCycleCount(),
                  TLogTitle::TVolumeProxy{
                      .DiskId = DiskId,
                      .TemporaryServer = temporaryServer})
        {}

        void AdvanceGeneration()
        {
            ++Generation;
            LogTitle.SetGeneration(Generation);
        }
    };

    // TActiveRequest contains an IEventHandle without embedded message,
    // it only contains information about the event type and the sender.
    // This is enough to response "E_REJECTED Tablet is dead". And it doesn't
    // waste any extra memory.
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
    const bool TemporaryServer = false;

    ui64 ConnectionIdGenerator = 0;

    THashMap<ui64, TConnection> ConnectionById;
    // Mapping of logical DiskId to the connection. The DiskId of the real disk
    // to which the connection is established may differ and have the suffix
    // "-copy".
    THashMap<TString, TConnection*> ConnectionByDiskId;
    // Mapping of the DiskId to the connection with exact DiskId match.
    THashMap<TString, TConnection*> ConnectionByRealDiskId;
    THashMap<ui64, TConnection*> ConnectionByTablet;

    struct TBaseTabletId
    {
        const ui64 TabletId = 0;
        int RefCount = 0;
    };

    THashMap<TString, TBaseTabletId> BaseDiskIdToTabletId;

    ui64 RequestId = 0;
    TActiveRequestMap ActiveRequests;

    std::unique_ptr<NTabletPipe::IClientCache> ClientCache;

public:
    TVolumeProxyActor(
        TStorageConfigPtr config,
        ITraceSerializerPtr traceSerializer,
        bool temporaryServer);

private:
    TConnection& CreateConnection(const TString& diskId, bool exactDiskIdMatch);
    void EraseConnection(TConnection* conn);

    void StartConnection(
        const TActorContext& ctx,
        TConnection& conn,
        ui64 tabletId,
        const TString& path,
        const TString& realDiskId);

    void DestroyConnection(TConnection& conn, NProto::TError error);

    void OnDisconnect(const TActorContext& ctx, TConnection& conn);

    void ProcessPendingRequests(TConnection& conn);

    void CancelActiveRequests(const TActorContext& ctx, TConnection& conn);

    void PostponeRequest(TConnection& conn, IEventHandlePtr ev);

    TConnection* GetConnectionByTabletId(ui64 tabletId);
    TConnection* GetConnectionById(ui64 id);

    template <typename TMethod>
    void ForwardRequest(
        const TActorContext& ctx,
        TConnection& conn,
        const typename TMethod::TRequest::TPtr& ev);

    void DescribeVolume(const TActorContext& ctx, TConnection& conn);

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

    void HandleBaseDiskDescribeResponse(
        TConnection* conn,
        const NProto::TError& error,
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
        ITraceSerializerPtr traceSerializer,
        bool temporaryServer)
    : TActor(&TThis::StateWork)
    , Config(std::move(config))
    , TraceSerializer(std::move(traceSerializer))
    , TemporaryServer(temporaryServer)
    , ClientCache(CreateTabletPipeClientCache(*Config))
{
}

TVolumeProxyActor::TConnection& TVolumeProxyActor::CreateConnection(
    const TString& diskId,
    bool exactDiskIdMatch)
{
    if (exactDiskIdMatch) {
        if (TConnection** conn = ConnectionByRealDiskId.FindPtr(diskId)) {
            return **conn;
        }
    } else {
        if (TConnection** conn = ConnectionByDiskId.FindPtr(diskId)) {
            return **conn;
        }
    }

    const ui64 connectionId = ++ConnectionIdGenerator;

    auto [it, inserted] = ConnectionById.emplace(
        connectionId,
        TConnection(connectionId, diskId, exactDiskIdMatch, TemporaryServer));

    if (exactDiskIdMatch) {
        ConnectionByRealDiskId[diskId] = &it->second;
    } else {
        ConnectionByDiskId[diskId] = &it->second;
    }

    return it->second;
}

void TVolumeProxyActor::EraseConnection(TConnection* conn)
{
    auto removeFromMap =
        [conn](THashMap<TString, TConnection*>& map, const TString& key)
    {
        auto it = map.find(key);
        if (it != map.end() && it->second == conn) {
            map.erase(it);
        }
    };

    removeFromMap(ConnectionByDiskId, conn->DiskId);
    removeFromMap(ConnectionByDiskId, conn->RealDiskId);
    removeFromMap(ConnectionByRealDiskId, conn->DiskId);
    removeFromMap(ConnectionByRealDiskId, conn->RealDiskId);

    ConnectionByTablet.erase(conn->TabletId);
    ConnectionById.erase(conn->Id);
}

void TVolumeProxyActor::StartConnection(
    const TActorContext& ctx,
    TConnection& conn,
    ui64 tabletId,
    const TString& path,
    const TString& realDiskId)
{
    conn.LogTitle.SetTabletId(tabletId);
    conn.AdvanceGeneration();
    conn.RealDiskId = realDiskId;

    LOG_INFO(
        ctx,
        TBlockStoreComponents::VOLUME_PROXY,
        "%s Resolved TabletID for volume %s with path %s",
        conn.LogTitle.GetWithTime().c_str(),
        conn.RealDiskId.Quote().c_str(),
        path.Quote().c_str());

    if (!ConnectionByDiskId.contains(conn.RealDiskId)) {
        ConnectionByDiskId[conn.RealDiskId] = &conn;
    }

    if (!ConnectionByRealDiskId.contains(conn.RealDiskId)) {
        ConnectionByRealDiskId[conn.RealDiskId] = &conn;
    }

    ConnectionByTablet[tabletId] = &conn;

    conn.State = STARTED;
    conn.TabletId = tabletId;

    ProcessPendingRequests(conn);
}

void TVolumeProxyActor::DestroyConnection(
    TConnection& conn,
    NProto::TError error)
{
    conn.State = STOPPED;
    conn.Error = std::move(error);

    // Cancel all pending requests.
    ProcessPendingRequests(conn);

    EraseConnection(&conn);
}

void TVolumeProxyActor::OnDisconnect(
    const TActorContext& ctx,
    TConnection& conn)
{
    LOG_WARN(
        ctx,
        TBlockStoreComponents::VOLUME_PROXY,
        "%s Connection to volume failed",
        conn.LogTitle.GetWithTime().c_str());

    // will wait for tablet to recover
    conn.State = FAILED;

    CancelActiveRequests(ctx, conn);
    ProcessPendingRequests(conn);
}

void TVolumeProxyActor::ProcessPendingRequests(TConnection& conn)
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
    TConnection& conn,
    IEventHandlePtr ev)
{
    conn.Requests.emplace_back(std::move(ev));
}

TVolumeProxyActor::TConnection* TVolumeProxyActor::GetConnectionByTabletId(
    ui64 tabletId)
{
    auto it = ConnectionByTablet.find(tabletId);
    return it == ConnectionByTablet.end() ? nullptr : it->second;
}

TVolumeProxyActor::TConnection* TVolumeProxyActor::GetConnectionById(ui64 id)
{
    auto it = ConnectionById.find(id);
    return it == ConnectionById.end() ? nullptr : &it->second;
}

template <typename TMethod>
void TVolumeProxyActor::ForwardRequest(
    const TActorContext& ctx,
    TConnection& conn,
    const typename TMethod::TRequest::TPtr& ev)
{
    auto* msg = ev->Get();

    auto clientId = ClientCache->Prepare(ctx, conn.TabletId);

    LOG_TRACE(
        ctx,
        TBlockStoreComponents::VOLUME_PROXY,
        "%s Forward request to volume (remote: %s)",
        conn.LogTitle.GetWithTime().c_str(),
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
    LOG_DEBUG(
        ctx,
        TBlockStoreComponents::VOLUME_PROXY,
        "%s Query describe volume",
        conn.LogTitle.GetWithTime().c_str());

    NCloud::Send(
        ctx,
        MakeSSProxyServiceId(),
        std::make_unique<TEvSSProxy::TEvDescribeVolumeRequest>(
            conn.DiskId,
            conn.RequireExactDiskIdMatch),
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

    TConnection* conn = GetConnectionById(msg->Tag);
    if (!conn) {
        // connection is already closed, nothing to do
        return;
    }

    conn->ActivityCheckScheduled = false;
    auto now = ctx.Now();

    if (conn->Requests.empty() &&
        !conn->RequestsInflight &&
        conn->LastActivity < now - PipeInactivityTimeout)
    {
        LOG_INFO(
            ctx,
            TBlockStoreComponents::VOLUME_PROXY,
            "%s Remove connection",
            conn->LogTitle.GetWithTime().c_str());

        ClientCache->Shutdown(ctx, conn->TabletId);
        EraseConnection(conn);
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

    TConnection* conn = GetConnectionByTabletId(msg->TabletId);
    if (!conn) {
        return;
    }

    if (!ClientCache->OnConnect(ev)) {
        auto error = MakeKikimrError(msg->Status, "Could not connect");
        LOG_ERROR(
            ctx,
            TBlockStoreComponents::VOLUME_PROXY,
            "%s Cannot connect to tablet: %s",
            conn->LogTitle.GetWithTime().c_str(),
            FormatError(error).data());

        CancelActiveRequests(ctx, *conn);
        DestroyConnection(*conn, std::move(error));
        return;
    }

    if (conn->State == FAILED) {
        // Tablet recovered
        conn->State = STARTED;
        conn->AdvanceGeneration();

        LOG_INFO(
            ctx,
            TBlockStoreComponents::VOLUME_PROXY,
            "%s Tablet connection recovered",
            conn->LogTitle.GetWithTime().c_str());
    }
}

void TVolumeProxyActor::HandleDisconnect(
    TEvTabletPipe::TEvClientDestroyed::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    TConnection* conn = GetConnectionByTabletId(msg->TabletId);
    if (!conn) {
        return;
    }

    ClientCache->OnDisconnect(ev);

    OnDisconnect(ctx, *conn);
}

void TVolumeProxyActor::HandleBaseDiskDescribeResponse(
    TConnection* conn,
    const NProto::TError& error,
    const TActorContext& ctx)
{
    if (error.GetCode() == MAKE_SCHEMESHARD_ERROR(NKikimrScheme::StatusPathDoesNotExist)) {
        LOG_ERROR(
            ctx,
            TBlockStoreComponents::VOLUME_PROXY,
            "%s Could not resolve path for base disk volume. Error: %s",
            conn->LogTitle.GetWithTime().c_str(),
            FormatError(error).c_str());

        DestroyConnection(*conn, error);
        return;
    }

    StartConnection(
        ctx,
        *conn,
        conn->TabletId,
        "PartitionConfig",
        conn->DiskId);
}

void TVolumeProxyActor::HandleDescribeResponse(
    const TEvSSProxy::TEvDescribeVolumeResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    TConnection* conn = GetConnectionById(ev->Cookie);
    if (!conn) {
        return;
    }

    const auto& error = msg->GetError();

    if (conn->IsConnectionToBaseDisk) {
        HandleBaseDiskDescribeResponse(conn, error, ctx);
        return;
    }

    if (FAILED(error.GetCode())) {
        LOG_ERROR(
            ctx,
            TBlockStoreComponents::VOLUME_PROXY,
            "%s Could not resolve path for volume. Error: %s",
            conn->LogTitle.GetWithTime().c_str(),
            FormatError(error).c_str());

        DestroyConnection(*conn, error);
        return;
    }

    const auto& pathDescr = msg->PathDescription;
    const auto& volumeDescr = pathDescr.GetBlockStoreVolumeDescription();
    const auto& realDiskId = volumeDescr.GetName();

    if (conn->RequireExactDiskIdMatch && conn->DiskId != realDiskId) {
        ReportLogicalDiskIdMismatch(
            {{"DiskId", conn->DiskId}, {"RealDiskid", realDiskId}});

        DestroyConnection(
            *conn,
            MakeError(
                MAKE_SCHEMESHARD_ERROR(NKikimrScheme::StatusPathDoesNotExist),
                TStringBuilder() << "Copy " << realDiskId.Quote()
                                 << " of the disk " << conn->DiskId.Quote()
                                 << " was found, when an exact match of the "
                                    "DiskId was required. Pretend that we "
                                    "haven't found anything."));
        return;
    }

    StartConnection(
        ctx,
        *conn,
        volumeDescr.GetVolumeTabletId(),
        msg->Path,
        realDiskId);
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

    TConnection& conn = CreateConnection(
        diskId,
        msg->Record.GetHeaders().GetExactDiskIdMatch());
    switch (conn.State) {
        case INITIAL:
        case FAILED:
        {
            conn.State = RESOLVING;
            if (auto* baseDisk = BaseDiskIdToTabletId.FindPtr(diskId)) {
                Y_ABORT_UNLESS(baseDisk->TabletId,
                    "%s Base disk %s tablet id is not set",
                    conn.LogTitle.GetWithTime().c_str(),
                    diskId.c_str());

                conn.TabletId = baseDisk->TabletId;
                conn.IsConnectionToBaseDisk = true;
            }

            DescribeVolume(ctx, conn);
            PostponeRequest(conn, IEventHandlePtr(ev.Release()));
            break;
        }
        case RESOLVING:{
            PostponeRequest(conn, IEventHandlePtr(ev.Release()));
            break;
        }
        case STARTED: {
            ForwardRequest<TMethod>(ctx, conn, ev);
            break;
        }
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

    auto* conn = GetConnectionById(it->second.ConnectionId);
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
        msg->BaseDiskId,
        TBaseTabletId{.TabletId = msg->BaseTabletId, .RefCount = 1});
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

    for (const auto& [id, conn]: ConnectionById) {
        ClientCache->Shutdown(ctx, conn.TabletId);
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
    ITraceSerializerPtr traceSerializer,
    bool temporaryServer)
{
    return std::make_unique<TVolumeProxyActor>(
        std::move(config),
        std::move(traceSerializer),
        temporaryServer);
}

}   // namespace NCloud::NBlockStore::NStorage
