#include "tablet_actor.h"

namespace NCloud::NFileStore::NStorage {

using namespace NActors;

////////////////////////////////////////////////////////////////////////////////

void TIndexTabletActor::HandleMarkNodeRefExhaustive(
    const TEvIndexTablet::TEvMarkNodeRefExhaustiveRequest::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    auto* msg = ev->Get();
    const auto& record = msg->Record;
    const ui64 nodeId = record.GetNodeId();

    auto requestInfo =
        CreateRequestInfo(ev->Sender, ev->Cookie, msg->CallContext);

    LOG_DEBUG(
        ctx,
        TFileStoreComponents::TABLET,
        "%s MarkNodeRefExhaustive (NodeId: %lu)",
        LogTag.c_str(),
        nodeId);

    // Mark the node refs as exhaustive in the in-memory cache
    if (Config->GetInMemoryIndexCacheEnabled()) {
        MarkNodeRefExhaustive(nodeId);

        LOG_INFO(
            ctx,
            TFileStoreComponents::TABLET,
            "%s MarkNodeRefExhaustive completed for NodeId: %lu",
            LogTag.c_str(),
            nodeId);
    } else {
        LOG_WARN(
            ctx,
            TFileStoreComponents::TABLET,
            "%s MarkNodeRefExhaustive: in-memory cache is not enabled",
            LogTag.c_str());
    }

    auto response = std::make_unique<TEvIndexTablet::TEvMarkNodeRefExhaustiveResponse>();
    NCloud::Reply(ctx, *requestInfo, std::move(response));
}

}   // namespace NCloud::NFileStore::NStorage
