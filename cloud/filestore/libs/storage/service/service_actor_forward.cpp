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
void TStorageServiceActor::ForwardRequestToFollower(
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

    if (StorageConfig->GetMultiTabletForwardingEnabled() && shardNo) {
        const auto& followerIds = filestore.GetFollowerFileSystemIds();
        if (followerIds.size() < static_cast<int>(shardNo)) {
            LOG_ERROR(ctx, TFileStoreComponents::SERVICE,
                "[%s][%lu] forward %s #%lu - invalid shardNo: %lu/%d",
                sessionId.Quote().c_str(),
                seqNo,
                TMethod::Name,
                msg->CallContext->RequestId,
                shardNo,
                followerIds.size());

            auto response = std::make_unique<typename TMethod::TResponse>(
                MakeError(E_INVALID_STATE, TStringBuilder() << "shardNo="
                    << shardNo << ", followerIds.size=" << followerIds.size()));
            return NCloud::Reply(ctx, *ev, std::move(response));
        }

        LOG_DEBUG(ctx, TFileStoreComponents::SERVICE,
            "[%s][%lu] forward %s #%lu to follower %s",
            sessionId.Quote().c_str(),
            seqNo,
            TMethod::Name,
            msg->CallContext->RequestId,
            followerIds[shardNo - 1].c_str());

        msg->Record.SetFileSystemId(followerIds[shardNo - 1]);
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

#define FILESTORE_FORWARD_REQUEST_TO_FOLLOWER_BY_NODE_ID(name, ns)             \
    void TStorageServiceActor::Handle##name(                                   \
        const ns::TEv##name##Request::TPtr& ev,                                \
        const TActorContext& ctx)                                              \
    {                                                                          \
        ForwardRequestToFollower<ns::T##name##Method>(                         \
            ctx,                                                               \
            ev,                                                                \
            ExtractShardNo(ev->Get()->Record.GetNodeId()));                    \
    }                                                                          \

    FILESTORE_SERVICE_REQUESTS_FWD_TO_FOLLOWER_BY_NODE_ID(
        FILESTORE_FORWARD_REQUEST_TO_FOLLOWER_BY_NODE_ID,
        TEvService)

#undef FILESTORE_FORWARD_REQUEST_TO_FOLLOWER_BY_NODE_ID

#define FILESTORE_FORWARD_REQUEST_TO_FOLLOWER_BY_HANDLE(name, ns)              \
    void TStorageServiceActor::Handle##name(                                   \
        const ns::TEv##name##Request::TPtr& ev,                                \
        const TActorContext& ctx)                                              \
    {                                                                          \
        ForwardRequestToFollower<ns::T##name##Method>(                         \
            ctx,                                                               \
            ev,                                                                \
            ExtractShardNo(ev->Get()->Record.GetHandle()));                    \
    }                                                                          \

    FILESTORE_SERVICE_REQUESTS_FWD_TO_FOLLOWER_BY_HANDLE(
        FILESTORE_FORWARD_REQUEST_TO_FOLLOWER_BY_HANDLE,
        TEvService)

#undef FILESTORE_FORWARD_REQUEST_TO_FOLLOWER_BY_NODE_ID

#define FILESTORE_DEFINE_HANDLE_FORWARD(name, ns)                              \
template void TStorageServiceActor::ForwardRequest<ns::T##name##Method>(       \
    const TActorContext&, const ns::TEv##name##Request::TPtr&);                \

    FILESTORE_SERVICE_REQUESTS_HANDLE(FILESTORE_DEFINE_HANDLE_FORWARD, TEvService)

#undef FILESTORE_DEFINE_HANDLE_FORWARD

}   // namespace NCloud::NFileStore::NStorage
