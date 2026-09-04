#include "service_actor.h"

namespace NCloud::NFileStore::NStorage {

using namespace NActors;

////////////////////////////////////////////////////////////////////////////////
// Stubs - no control namespace logic yet, every hook is a no-op.

bool TStorageServiceActor::TryHandleControlNamespaceGetNodeAttr(
    const TActorContext& ctx,
    const TEvService::TEvGetNodeAttrRequest::TPtr& ev)
{
    Y_UNUSED(ctx, ev);
    return false;
}

bool TStorageServiceActor::TryHandleControlNamespaceCreateHandle(
    const TActorContext& ctx,
    const TEvService::TEvCreateHandleRequest::TPtr& ev)
{
    Y_UNUSED(ctx, ev);
    return false;
}

bool TStorageServiceActor::TryHandleControlNamespaceCreateNode(
    const TActorContext& ctx,
    const TEvService::TEvCreateNodeRequest::TPtr& ev)
{
    Y_UNUSED(ctx, ev);
    return false;
}

bool TStorageServiceActor::TryHandleControlNamespaceReadData(
    const TActorContext& ctx,
    const TEvService::TEvReadDataRequest::TPtr& ev)
{
    Y_UNUSED(ctx, ev);
    return false;
}

bool TStorageServiceActor::TryHandleControlNamespaceWriteData(
    const TActorContext& ctx,
    const TEvService::TEvWriteDataRequest::TPtr& ev)
{
    Y_UNUSED(ctx, ev);
    return false;
}

bool TStorageServiceActor::TryHandleControlNamespaceListNodes(
    const TActorContext& ctx,
    const TEvService::TEvListNodesRequest::TPtr& ev)
{
    Y_UNUSED(ctx, ev);
    return false;
}

bool TStorageServiceActor::TryHandleControlNamespaceRenameNode(
    const TActorContext& ctx,
    const TEvService::TEvRenameNodeRequest::TPtr& ev)
{
    Y_UNUSED(ctx, ev);
    return false;
}

bool TStorageServiceActor::TryHandleControlNamespaceGetNodeXAttr(
    const TActorContext& ctx,
    const TEvService::TEvGetNodeXAttrRequest::TPtr& ev,
    const TSessionInfo* session)
{
    Y_UNUSED(ctx, ev, session);
    return false;
}

bool TStorageServiceActor::TryHandleControlNamespaceListNodeXAttr(
    const TActorContext& ctx,
    const TEvService::TEvListNodeXAttrRequest::TPtr& ev,
    const TSessionInfo* session)
{
    Y_UNUSED(ctx, ev, session);
    return false;
}

bool TStorageServiceActor::TryHandleControlNamespaceSetNodeXAttr(
    const TActorContext& ctx,
    const TEvService::TEvSetNodeXAttrRequest::TPtr& ev,
    const TSessionInfo* session)
{
    Y_UNUSED(ctx, ev, session);
    return false;
}

}   // namespace NCloud::NFileStore::NStorage
