#include "service_actor.h"

namespace NCloud::NFileStore::NStorage {

using namespace NActors;

using namespace NKikimr;

////////////////////////////////////////////////////////////////////////////////

void TStorageServiceActor::HandleCreateNode(
    const TEvService::TEvCreateNodeRequest::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    const auto& clientId = GetClientId(msg->Record);
    const auto& sessionId = GetSessionId(msg->Record);
    const ui64 seqNo = GetSessionSeqNo(msg->Record);

    auto* session = State->FindSession(sessionId, seqNo);
    if (!session || session->ClientId != clientId || !session->SessionActor) {
        auto response = std::make_unique<TEvService::TEvCreateNodeResponse>(
            ErrorInvalidSession(clientId, sessionId, seqNo));
        return NCloud::Reply(ctx, *ev, std::move(response));
    }
    const auto& followerId = session->SelectFollower();

    if (StorageConfig->GetMultiTabletForwardingEnabled() && followerId) {
        msg->Record.SetFollowerFileSystemId(followerId);
    }

    ForwardRequest<TEvService::TCreateNodeMethod>(ctx, ev);
}

}   // namespace NCloud::NFileStore::NStorage
