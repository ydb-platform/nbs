#include "tablet_actor.h"

namespace NCloud::NFileStore::NStorage {

using namespace NActors;

using namespace NKikimr;

////////////////////////////////////////////////////////////////////////////////

void TIndexTabletActor::HandleSubscribeSession(
    const TEvService::TEvSubscribeSessionRequest::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    const auto& clientId = GetClientId(msg->Record);
    const auto& sessionId = GetSessionId(msg->Record);
    const auto& sessionSeqNo = GetSessionSeqNo(msg->Record);

    auto* session = FindSession(clientId, sessionId, sessionSeqNo);
    if (!session) {
        auto response =
            std::make_unique<TEvService::TEvSubscribeSessionResponse>(
                ErrorInvalidSession(clientId, sessionId, sessionSeqNo));
        return NCloud::Reply(ctx, *ev, std::move(response));
    }

    // TODO
    session->NotifyEvents = true;

    auto response = std::make_unique<TEvService::TEvSubscribeSessionResponse>();
    NCloud::Reply(ctx, *ev, std::move(response));
}

void TIndexTabletActor::NotifySessionEvent(
    const TActorContext& ctx,
    const NProto::TSessionEvent& event)
{
    const auto sessionsToNotify = GetSessionsToNotify(event);
    if (sessionsToNotify) {
        for (auto* session: sessionsToNotify) {
            const auto seqNo = GenerateEventId(session);

            LOG_TRACE(
                ctx,
                TFileStoreComponents::TABLET,
                "%s[%s] notify session event (seqNo: %lu)",
                LogTag.c_str(),
                session->GetSessionId().c_str(),
                seqNo);

            for (const auto& s: session->GetSubSessions()) {
                auto response =
                    std::make_unique<TEvService::TEvGetSessionEventsResponse>();

                auto* ev = response->Record.AddEvents();
                ev->CopyFrom(event);
                ev->SetSeqNo(seqNo);

                NCloud::Send(ctx, s, std::move(response));
            }
        }
    }
}

}   // namespace NCloud::NFileStore::NStorage
