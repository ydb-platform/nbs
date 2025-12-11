#include "service_actor.h"

namespace NCloud::NFileStore::NStorage {

using namespace NActors;

using namespace NKikimr;

////////////////////////////////////////////////////////////////////////////////

void TStorageServiceActor::HandleGetSessionEvents(
    const TEvService::TEvGetSessionEventsRequest::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    const auto& clientId = GetClientId(msg->Record);
    const auto seqNo = GetSessionSeqNo(msg->Record);
    const auto sessionId = GetSessionId(msg->Record);

    auto* session = State->FindSession(sessionId, seqNo);
    if (!session || session->ClientId != clientId ||
        sessionId != session->SessionId)
    {
        auto response =
            std::make_unique<TEvService::TEvGetSessionEventsResponse>(
                ErrorInvalidSession(clientId, sessionId, seqNo));
        return NCloud::Reply(ctx, *ev, std::move(response));
    }

    // forward request to the session actor
    ctx.Send(ev->Forward(session->SessionActor));
}

}   // namespace NCloud::NFileStore::NStorage
