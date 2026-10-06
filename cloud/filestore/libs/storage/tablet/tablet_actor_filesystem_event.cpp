#include "tablet_actor.h"

#include <cloud/filestore/libs/storage/api/tablet_proxy.h>

#include <util/generic/hash_set.h>

namespace NCloud::NFileStore::NStorage {

using namespace NActors;

////////////////////////////////////////////////////////////////////////////////

void TIndexTabletActor::RegisterFileSystemEventClient(
    const TActorId& recipient,
    const TActorId& clientId)
{
    //
    // Called by the requests whose responses the client may cache: GetNodeAttr,
    // GetNodeAttrBatch (attrs of shard-resident nodes upon ListNodes),
    // ListNodes, ListNodesInternal, CreateNode, CreateHandle. ReadNodeRefs
    // doesn't register its sender - it's used by the private API only.
    //
    // Requests received via a pipe have the pipe server as their recipient.
    // Requests sent directly to the tablet have no pipe and therefore no
    // disconnect notification - such senders are not registered.
    //

    if (recipient == SelfId()) {
        return;
    }

    FileSystemEventClients[recipient] = clientId;
}

ui32 TIndexTabletActor::SendFileSystemEvents(
    const TActorContext& ctx,
    const TVector<NProto::TFileSystemEvent>& events)
{
    THashSet<TActorId> clients;
    for (const auto& [_, clientId]: FileSystemEventClients) {
        clients.insert(clientId);
    }

    const auto& fileSystemId = GetMainFileSystemId();
    for (const auto& event: events) {
        LOG_DEBUG(
            ctx,
            TFileStoreComponents::TABLET,
            "%s Sending FileSystemEvent to %lu clients: %s",
            LogTag.c_str(),
            clients.size(),
            event.ShortUtf8DebugString().Quote().c_str());

        for (const auto& clientId: clients) {
            auto ev =
                std::make_unique<TEvIndexTabletProxy::TEvFileSystemEvent>();
            ev->Record = event;
            ev->Record.SetFileSystemId(fileSystemId);
            NCloud::Send(ctx, clientId, std::move(ev));
        }
    }

    return clients.size();
}

////////////////////////////////////////////////////////////////////////////////

void TIndexTabletActor::HandleGenerateFileSystemEvent(
    const TEvIndexTablet::TEvGenerateFileSystemEventRequest::TPtr& ev,
    const TActorContext& ctx)
{
    using TResponse = TEvIndexTablet::TEvGenerateFileSystemEventResponse;

    const auto& record = ev->Get()->Record;

    LOG_INFO(
        ctx,
        TFileStoreComponents::TABLET,
        "%s GenerateFileSystemEvent: %s",
        LogTag.c_str(),
        record.ShortUtf8DebugString().Quote().c_str());

    const auto& event = record.GetEvent();
    if (event.HasInvalidateNode() == event.HasInvalidateNodeRef()) {
        NCloud::Reply(
            ctx,
            *ev,
            std::make_unique<TResponse>(MakeError(
                E_ARGUMENT,
                "exactly one invalidation should be set")));
        return;
    }

    auto response = std::make_unique<TResponse>();
    response->Record.SetClientCount(SendFileSystemEvents(ctx, {event}));
    NCloud::Reply(ctx, *ev, std::move(response));
}

}   // namespace NCloud::NFileStore::NStorage
