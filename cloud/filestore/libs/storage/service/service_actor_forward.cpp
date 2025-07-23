#include "service_actor.h"

#include <cloud/filestore/libs/diagnostics/profile_log_events.h>
#include <cloud/filestore/libs/storage/api/tablet.h>
#include <cloud/filestore/libs/storage/api/tablet_proxy.h>
#include <cloud/filestore/libs/storage/model/utils.h>

#include <cloud/storage/core/libs/diagnostics/trace_serializer.h>

namespace NCloud::NFileStore::NStorage {

using namespace NActors;

using namespace NKikimr;

////////////////////////////////////////////////////////////////////////////////

TResultOrError<TString> TStorageServiceActor::SelectShard(
    const NActors::TActorContext& ctx,
    const TString& sessionId,
    const ui64 seqNo,
    const bool disableMultiTabletForwarding,
    const TString& methodName,
    const ui64 requestId,
    const NProto::TFileStore& filestore,
    const ui32 shardNo) const
{
    const bool multiTabletForwardingEnabled =
        StorageConfig->GetMultiTabletForwardingEnabled()
        && !disableMultiTabletForwarding;
    if (multiTabletForwardingEnabled && shardNo) {
        const auto& shardIds = filestore.GetShardFileSystemIds();
        if (shardIds.size() < static_cast<int>(shardNo)) {
            LOG_DEBUG(ctx, TFileStoreComponents::SERVICE,
                "[%s][%lu] forward %s #%lu - invalid shardNo: %u/%d"
                " (legacy handle?)",
                sessionId.Quote().c_str(),
                seqNo,
                methodName.c_str(),
                requestId,
                shardNo,
                shardIds.size());

            // TODO(#1350): uncomment when there are no legacy handles anymore
            //return MakeError(E_INVALID_STATE, TStringBuilder() << "shardNo="
            //        << shardNo << ", shardIds.size=" << shardIds.size());
            return TString();
        }

        LOG_DEBUG(ctx, TFileStoreComponents::SERVICE,
            "[%s][%lu] forward %s #%lu to shard %s",
            sessionId.Quote().c_str(),
            seqNo,
            methodName.c_str(),
            requestId,
            shardIds[shardNo - 1].c_str());

        return shardIds[shardNo - 1];
    }

    return TString();
}

////////////////////////////////////////////////////////////////////////////////

template <typename TMethod>
void TStorageServiceActor::ForwardRequest(
    const TActorContext& ctx,
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

    auto* session = State->FindSession(sessionId, seqNo);
    if (!session || session->ClientId != clientId || !session->SessionActor) {
        auto response = std::make_unique<typename TMethod::TResponse>(
            ErrorInvalidSession(clientId, sessionId, seqNo));
        return NCloud::Reply(ctx, *ev, std::move(response));
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

////////////////////////////////////////////////////////////////////////////////

template <typename TMethod>
void TStorageServiceActor::ForwardRequestToShard(
    const TActorContext& ctx,
    const typename TMethod::TRequest::TPtr& ev,
    ui32 shardNo)
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

    auto* session = State->FindSession(sessionId, seqNo);
    if (!session || session->ClientId != clientId || !session->SessionActor) {
        auto response = std::make_unique<typename TMethod::TResponse>(
            ErrorInvalidSession(clientId, sessionId, seqNo));
        return NCloud::Reply(ctx, *ev, std::move(response));
    }
    const NProto::TFileStore& filestore = session->FileStore;

    auto [fsId, error] = SelectShard(
        ctx,
        sessionId,
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

////////////////////////////////////////////////////////////////////////////////

#define FILESTORE_FORWARD_REQUEST(name, ns)                                    \
    void TStorageServiceActor::Handle##name(                                   \
        const ns::TEv##name##Request::TPtr& ev,                                \
        const TActorContext& ctx)                                              \
    {                                                                          \
        ForwardRequest<ns::T##name##Method>(ctx, ev);                          \
    }                                                                          \

    FILESTORE_SERVICE_REQUESTS_FWD(FILESTORE_FORWARD_REQUEST, TEvService)

#undef FILESTORE_FORWARD_REQUEST

#define FILESTORE_FORWARD_REQUEST_TO_SHARD_BY_NODE_ID(name, ns)                \
    void TStorageServiceActor::Handle##name(                                   \
        const ns::TEv##name##Request::TPtr& ev,                                \
        const TActorContext& ctx)                                              \
    {                                                                          \
        ForwardRequestToShard<ns::T##name##Method>(                            \
            ctx,                                                               \
            ev,                                                                \
            ExtractShardNo(ev->Get()->Record.GetNodeId()));                    \
    }                                                                          \

    FILESTORE_SERVICE_REQUESTS_FWD_TO_SHARD_BY_NODE_ID(
        FILESTORE_FORWARD_REQUEST_TO_SHARD_BY_NODE_ID,
        TEvService)

#undef FILESTORE_FORWARD_REQUEST_TO_SHARD_BY_NODE_ID

#define FILESTORE_FORWARD_REQUEST_TO_SHARD_BY_HANDLE(name, ns)                 \
    void TStorageServiceActor::Handle##name(                                   \
        const ns::TEv##name##Request::TPtr& ev,                                \
        const TActorContext& ctx)                                              \
    {                                                                          \
        ForwardRequestToShard<ns::T##name##Method>(                            \
            ctx,                                                               \
            ev,                                                                \
            ExtractShardNo(ev->Get()->Record.GetHandle()));                    \
    }                                                                          \

    FILESTORE_SERVICE_REQUESTS_FWD_TO_SHARD_BY_HANDLE(
        FILESTORE_FORWARD_REQUEST_TO_SHARD_BY_HANDLE,
        TEvService)

#undef FILESTORE_FORWARD_REQUEST_TO_SHARD_BY_NODE_ID

#define FILESTORE_DEFINE_HANDLE_FORWARD(name, ns)                              \
template void TStorageServiceActor::ForwardRequest<ns::T##name##Method>(       \
    const TActorContext&, const ns::TEv##name##Request::TPtr&);                \

    FILESTORE_SERVICE_REQUESTS_HANDLE(FILESTORE_DEFINE_HANDLE_FORWARD, TEvService)

#undef FILESTORE_DEFINE_HANDLE_FORWARD

template void
TStorageServiceActor::ForwardRequestToShard<TEvService::TCreateHandleMethod>(
    const TActorContext& ctx,
    const TEvService::TCreateHandleMethod::TRequest::TPtr& ev,
    ui32 shardNo);

template void
TStorageServiceActor::ForwardRequestToShard<TEvService::TGetNodeAttrMethod>(
    const TActorContext& ctx,
    const TEvService::TGetNodeAttrMethod::TRequest::TPtr& ev,
    ui32 shardNo);

template void
TStorageServiceActor::ForwardRequestToShard<TEvService::TGetNodeXAttrMethod>(
    const TActorContext& ctx,
    const TEvService::TGetNodeXAttrMethod::TRequest::TPtr& ev,
    ui32 shardNo);

template void
TStorageServiceActor::ForwardRequestToShard<TEvService::TSetNodeXAttrMethod>(
    const TActorContext& ctx,
    const TEvService::TSetNodeXAttrMethod::TRequest::TPtr& ev,
    ui32 shardNo);

template void
TStorageServiceActor::ForwardRequestToShard<TEvService::TListNodeXAttrMethod>(
    const TActorContext& ctx,
    const TEvService::TListNodeXAttrMethod::TRequest::TPtr& ev,
    ui32 shardNo);

}   // namespace NCloud::NFileStore::NStorage
