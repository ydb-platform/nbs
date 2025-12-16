#include "service_actor.h"

namespace NCloud::NFileStore::NStorage {

using namespace NActors;

using namespace NKikimr;

////////////////////////////////////////////////////////////////////////////////

void TStorageServiceActor::HandlePingSession(
    const TEvService::TEvPingSessionRequest::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    const auto& clientId = GetClientId(msg->Record);
    const auto seqNo = GetSessionSeqNo(msg->Record);
    const auto sessionId = GetSessionId(msg->Record);

    LOG_DEBUG(
        ctx,
        TFileStoreComponents::SERVICE,
        "HandlePingSession %s, %s",
        clientId.Quote().c_str(),
        sessionId.Quote().c_str());

    auto* session = State->FindSession(sessionId, seqNo);
    if (!session ||
        session->ClientId != clientId ||
        sessionId != session->SessionId ||
        !session->SessionActor)
    {
        auto response = std::make_unique<TEvService::TEvPingSessionResponse>(
            ErrorInvalidSession(clientId, sessionId, seqNo));

        NCloud::Reply(ctx, *ev, std::move(response));
        return;
    }

    session->UpdateSubSession(seqNo, ctx.Now());

    NCloud::Send(
        ctx,
        session->SessionActor,
        std::make_unique<TEvServicePrivate::TEvPingSession>());

    auto response = std::make_unique<TEvService::TEvPingSessionResponse>();
    NCloud::Reply(ctx, *ev, std::move(response));
}

}   // namespace NCloud::NFileStore::NStorage
