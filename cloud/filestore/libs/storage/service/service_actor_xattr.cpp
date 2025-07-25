#include "service_actor.h"

#include <cloud/filestore/libs/service/error.h>
#include <cloud/filestore/libs/diagnostics/profile_log_events.h>
#include <cloud/filestore/libs/storage/api/tablet_proxy.h>
#include <cloud/filestore/libs/storage/api/tablet.h>

#include <cloud/storage/core/libs/diagnostics/trace_serializer.h>


namespace NCloud::NFileStore::NStorage {

using namespace NActors;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TSetHasXAttrsActor final
    : public TActorBootstrapped<TSetHasXAttrsActor>
{
private:
    const bool HasXAttrsValue;
    const TString& FileSystemId;
    // Original request
    const TRequestInfoPtr RequestInfo;

public:
    TSetHasXAttrsActor(
        bool value,
        const TString& fileSystemId,
        TRequestInfoPtr originalRequestInfo);

    void Bootstrap(const TActorContext& ctx);

private:
    STFUNC(StateWork);

    void HandleSetHasXAttrsResponse(
        const TEvIndexTablet::TEvSetHasXAttrsResponse::TPtr& ev,
        const TActorContext& ctx);
};

////////////////////////////////////////////////////////////////////////////////

TSetHasXAttrsActor::TSetHasXAttrsActor(
        bool hasXAttrsValue,
        const TString& fileSystemId,
        TRequestInfoPtr RequestInfo)
    : HasXAttrsValue(hasXAttrsValue)
    , FileSystemId(fileSystemId)
    , RequestInfo(RequestInfo)
{}

void TSetHasXAttrsActor::Bootstrap(const TActorContext& ctx)
{
    NProtoPrivate::TSetHasXAttrsRequest request;

    LOG_DEBUG(
        ctx,
        TFileStoreComponents::SERVICE,
        "Send TSetHasXAttrsRequest to index tablet");

    auto requestToTablet =
        std::make_unique<TEvIndexTablet::TEvSetHasXAttrsRequest>();
    auto& record = requestToTablet->Record;
    record.SetFileSystemId(FileSystemId);
    record.SetValue(HasXAttrsValue);

    NCloud::Send(
        ctx,
        MakeIndexTabletProxyServiceId(),
        std::move(requestToTablet));

    Become(&TSetHasXAttrsActor::StateWork);
}

void TSetHasXAttrsActor::HandleSetHasXAttrsResponse(
    const TEvIndexTablet::TEvSetHasXAttrsResponse::TPtr&,
    const TActorContext& ctx)
{
    // We always reply E_REJECTED to the original request.
    // When the index tablet restarts and session is recreated, we stop sending
    // this message
    auto response =
        std::make_unique<TEvService::TSetNodeXAttrMethod::TResponse>(
            MakeError(E_REJECTED));
    NCloud::Reply(ctx, *RequestInfo, std::move(response));

    Die(ctx);
}

STFUNC(TSetHasXAttrsActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(
            TEvIndexTablet::TEvSetHasXAttrsResponse,
            HandleSetHasXAttrsResponse);

        default:
            HandleUnexpectedEvent(
                ev,
                TFileStoreComponents::SERVICE,
                __PRETTY_FUNCTION__);
            break;
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

template <typename TMethod>
TSessionInfo* TStorageServiceActor::GetAndValidateSession(
    const NActors::TActorContext& ctx,
    const typename TMethod::TRequest::TPtr& ev)
{
    auto* msg = ev->Get();

    const auto& clientId = GetClientId(msg->Record);
    const auto& sessionId = GetSessionId(msg->Record);
    const ui64 seqNo = GetSessionSeqNo(msg->Record);

    TSessionInfo* session = State->FindSession(sessionId, seqNo);
    if (!session || session->ClientId != clientId || !session->SessionActor) {
        auto response = std::make_unique<typename TMethod::TResponse>(
            ErrorInvalidSession(clientId, sessionId, seqNo));
        NCloud::Reply(ctx, *ev, std::move(response));
        return nullptr;
    }

    return session;
}

////////////////////////////////////////////////////////////////////////////////

template <typename TMethod>
void TStorageServiceActor::ForwardXAttrRequest(
    const TActorContext& ctx,
    const typename TMethod::TRequest::TPtr& ev,
    const TSessionInfo* session)
{
    auto* msg = ev->Get();

    const ui64 seqNo = GetSessionSeqNo(msg->Record);
    const auto shardNo = ExtractShardNo(ev->Get()->Record.GetNodeId());

    LOG_DEBUG(ctx, TFileStoreComponents::SERVICE,
        "[%s][%lu] forward %s #%lu",
        session->SessionId.Quote().c_str(),
        seqNo,
        TMethod::Name,
        msg->CallContext->RequestId);

    const NProto::TFileStore& filestore = session->FileStore;

    auto [fsId, error] = SelectShard(
        ctx,
        session->SessionId,
        seqNo,
        msg->Record.GetHeaders().GetDisableMultiTabletForwarding(),
        TMethod::Name,
        msg->CallContext->RequestId,
        filestore,
        shardNo);

    if (HasError(error)) {
        auto response =
            std::make_unique<typename TMethod::TResponse>(std::move(error));
        return NCloud::Reply(ctx, *ev, std::move(response));
    }

    if (fsId) {
        msg->Record.SetFileSystemId(fsId);
    }

    auto [cookie, inflight] = CreateInFlightRequest(
        TRequestInfo(ev->Sender, ev->Cookie, msg->CallContext),
        session->MediaKind,
        session->RequestStats,
        ctx.Now());

    InitProfileLogRequestInfo(inflight->ProfileLogRequest, msg->Record);
    TraceSerializer->BuildTraceRequest(
        *msg->Record.MutableHeaders()->MutableInternal()->MutableTrace(),
        msg->CallContext->LWOrbit);

    auto event = std::make_unique<IEventHandle>(
        MakeIndexTabletProxyServiceId(),
        SelfId(),
        ev->ReleaseBase().Release(),
        0,        // flags
        cookie,   // cookie
        nullptr   // forwardOnNondelivery
    );

    ctx.Send(event.release());
}

///////////////////////////////////////////////////////////////////////////////

template <typename TMethod>
void TStorageServiceActor::ReplyToXAttrRequest(
    const NActors::TActorContext& ctx,
    const typename TMethod::TRequest::TPtr& ev,
    std::unique_ptr<typename TMethod::TResponse> response,
    const TSessionInfo* session)
{
    auto* msg = ev->Get();

    LOG_DEBUG(
        ctx,
        TFileStoreComponents::SERVICE,
        "[%s][%lu] reply immediately to %s #%lu because there are no XAttrs",
        session->SessionId.Quote().c_str(),
        GetSessionSeqNo(msg->Record),
        TEvService::TGetNodeXAttrMethod::Name,
        msg->CallContext->RequestId);

    // This request is created and completed immediately in order to increment
    // a corresponding counter
    TInFlightRequest inflight(
        TRequestInfo(ev->Sender, ev->Cookie, ev->Get()->CallContext),
        ProfileLog,
        session->MediaKind,
        session->RequestStats);

    InitProfileLogRequestInfo(inflight.ProfileLogRequest, msg->Record);
    inflight.Start(ctx.Now());

    FinalizeProfileLogRequestInfo(
        inflight.ProfileLogRequest,
        response->Record);
    inflight.Complete(ctx.Now(), response->GetError());

    TraceSerializer->BuildTraceRequest(
        *(msg->Record.MutableHeaders()->MutableInternal()->MutableTrace()),
        msg->CallContext->LWOrbit);

    NCloud::Reply(ctx, *ev, std::move(response));
}

///////////////////////////////////////////////////////////////////////////////

void TStorageServiceActor::HandleGetNodeXAttr(
    const TEvService::TEvGetNodeXAttrRequest::TPtr& ev,
    const TActorContext& ctx)
{
    const TSessionInfo* session =
        GetAndValidateSession<TEvService::TGetNodeXAttrMethod>(ctx, ev);
    if (!session) {
        return;
    }

    // If there are no extended attributes in the filesystem we don't
    // forward corresponding requests to the tablet and reply from the service
    // actor
    if (!session->FileStore.GetFeatures().GetHasXAttrs()) {
        auto response =
            std::make_unique<TEvService::TGetNodeXAttrMethod::TResponse>(
                ErrorAttributeDoesNotExist(
                    TEvService::TGetNodeXAttrMethod::Name));

        ReplyToXAttrRequest<TEvService::TGetNodeXAttrMethod>(
            ctx,
            ev,
            std::move(response),
            session);
        return;
    }

    ForwardXAttrRequest<TEvService::TGetNodeXAttrMethod>(ctx, ev, session);
}

///////////////////////////////////////////////////////////////////////////////

void TStorageServiceActor::HandleListNodeXAttr(
    const TEvService::TEvListNodeXAttrRequest::TPtr& ev,
    const TActorContext& ctx)
{
    const TSessionInfo* session =
        GetAndValidateSession<TEvService::TListNodeXAttrMethod>(ctx, ev);
    if (!session) {
        return;
    }

    // if there no extended attributes in the file system we return an empty
    // list
    if (!session->FileStore.GetFeatures().GetHasXAttrs()) {
        auto response =
            std::make_unique<TEvService::TListNodeXAttrMethod::TResponse>();

        ReplyToXAttrRequest<TEvService::TListNodeXAttrMethod>(
            ctx,
            ev,
            std::move(response),
            session);

        return;
    }

    ForwardXAttrRequest<TEvService::TListNodeXAttrMethod>(ctx, ev, session);
}

///////////////////////////////////////////////////////////////////////////////

void TStorageServiceActor::HandleSetNodeXAttr(
    const TEvService::TEvSetNodeXAttrRequest::TPtr& ev,
    const TActorContext& ctx)
{
    const TSessionInfo* session =
        GetAndValidateSession<TEvService::TSetNodeXAttrMethod>(ctx, ev);
    if (!session) {
        return;
    }

    if (!session->FileStore.GetFeatures().GetHasXAttrs()) {
        // Send TSetHasXAttrsRequest to the index tablet
        auto* msg = ev->Get();
        LOG_INFO(
            ctx,
            TFileStoreComponents::SERVICE,
            "[%s][%lu] The first XAttr in the filesystem is set. Sending "
            "SetHasXAttrs to the index tablet.",
            session->SessionId.Quote().c_str(),
            GetSessionSeqNo(msg->Record));

        auto requestInfo =
            CreateRequestInfo(ev->Sender, ev->Cookie, msg->CallContext);
        auto setHasXAttrsActor = std::make_unique<TSetHasXAttrsActor>(
            true,
            session->FileStore.GetFileSystemId(),
            requestInfo);
        NCloud::Register(ctx, std::move(setHasXAttrsActor));
    } else {
        // Forward SetNodeXAttr to a shard only if HasXAttrs flag is set in the
        // filesystem
        ForwardXAttrRequest<TEvService::TSetNodeXAttrMethod>(ctx, ev, session);
    }
}

}   // namespace NCloud::NFileStore::NStorage
