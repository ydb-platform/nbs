#include "service_actor.h"

#include <cloud/filestore/libs/service/error.h>
#include <cloud/filestore/libs/diagnostics/profile_log_events.h>
#include <cloud/storage/core/libs/diagnostics/trace_serializer.h>
#include <cloud/filestore/libs/storage/api/tablet_proxy.h>

namespace NCloud::NFileStore::NStorage {

using namespace NActors;

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

    LOG_DEBUG(ctx, TFileStoreComponents::SERVICE,
        "[%s][%lu] forward %s #%lu",
        sessionId.Quote().c_str(),
        seqNo,
        TMethod::Name,
        msg->CallContext->RequestId);

    TSessionInfo* session = State->FindSession(sessionId, seqNo);
    if (!session || session->ClientId != clientId || !session->SessionActor) {
        auto response = std::make_unique<typename TMethod::TResponse>(
            ErrorInvalidSession(clientId, sessionId, seqNo));
        NCloud::Reply(ctx, *ev, std::move(response));
        return nullptr;
    }

    return session;
}

template <typename TMethod>
void TStorageServiceActor::ForwardXAttrRequest(
    const TActorContext& ctx,
    const typename TMethod::TRequest::TPtr& ev,
    const TSessionInfo* session)
{
    auto* msg = ev->Get();

    const ui64 seqNo = GetSessionSeqNo(msg->Record);
    const auto shardNo = ExtractShardNo(ev->Get()->Record.GetNodeId());

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
        0,          // flags
        cookie,     // cookie
        // forwardOnNondelivery
        nullptr);

    ctx.Send(event.release());
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

    // if there no extended attributes in the file system we don't create a
    // requerst for them
    if (!session->FileStore.GetFeatures().GetHasXAttrs()) {
        auto response =
            std::make_unique<TEvService::TGetNodeXAttrMethod::TResponse>(
                ErrorAttributeDoesNotExist(
                    TEvService::TGetNodeXAttrMethod::Name));
        NCloud::Reply(ctx, *ev, std::move(response));
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
            std::make_unique<TEvService::TEvListNodeXAttrResponse>();
        NCloud::Reply(ctx, *ev, std::move(response));
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

    // https://github.com/ydb-platform/nbs/issues/3777
    // message to TIndexTabletState notifying that XAttrs appeared should be
    // placed here

    ForwardXAttrRequest<TEvService::TSetNodeXAttrMethod>(ctx, ev, session);
}

}   // namespace NCloud::NFileStore::NStorage
