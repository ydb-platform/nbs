#include "service_actor.h"

#include <cloud/filestore/libs/diagnostics/profile_log_events.h>
#include <cloud/filestore/libs/storage/api/tablet.h>
#include <cloud/filestore/libs/storage/api/tablet_proxy.h>

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

#define FILESTORE_FORWARD_REQUEST(name, ns)                                    \
    void TStorageServiceActor::Handle##name(                                   \
        const ns::TEv##name##Request::TPtr& ev,                                \
        const TActorContext& ctx)                                              \
    {                                                                          \
        ForwardRequest<ns::T##name##Method>(ctx, ev);                          \
    }                                                                          \

    FILESTORE_SERVICE_REQUESTS_HANDLE(FILESTORE_FORWARD_REQUEST, TEvService)

#undef FILESTORE_FORWARD_REQUEST

#define FILESTORE_DEFINE_NO_HANDLE_FORWARD(name, ns)                           \
template void TStorageServiceActor::ForwardRequest<ns::T##name##Method>(       \
    const TActorContext&, const ns::TEv##name##Request::TPtr&);                \

    FILESTORE_SERVICE_REQUESTS_NO_HANDLE(FILESTORE_DEFINE_NO_HANDLE_FORWARD, TEvService)

#undef FILESTORE_DEFINE_NO_HANDLE_FORWARD

}   // namespace NCloud::NFileStore::NStorage
