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
    // Requests received via a pipe have the pipe server as their recipient.
    // Requests sent directly to the tablet have no pipe and therefore no
    // disconnect notification - such senders are not registered.
    //

    if (recipient == SelfId()) {
        return;
    }

    FileSystemEventClients[recipient] = clientId;
}

ui32 TIndexTabletActor::SendFileSystemEvent(
    const TActorContext& ctx,
    NProto::TFileSystemEvent event)
{
    event.SetFileSystemId(GetMainFileSystemId());

    THashSet<TActorId> clients;
    for (const auto& [_, clientId]: FileSystemEventClients) {
        clients.insert(clientId);
    }

    LOG_DEBUG(
        ctx,
        TFileStoreComponents::TABLET,
        "%s Sending FileSystemEvent to %lu clients: %s",
        LogTag.c_str(),
        clients.size(),
        event.ShortUtf8DebugString().Quote().c_str());

    for (const auto& clientId: clients) {
        auto ev = std::make_unique<TEvIndexTabletProxy::TEvFileSystemEvent>();
        ev->Record = event;
        NCloud::Send(ctx, clientId, std::move(ev));
    }

    return clients.size();
}

void TIndexTabletActor::FlushFileSystemEvents(const TActorContext& ctx)
{
    //
    // Called upon each transaction completion. Read-only transactions may
    // complete before the preceding read-write transaction is committed,
    // so an event may reach the clients slightly before the change becomes
    // durable. That is acceptable for invalidations: the clients re-read the
    // state which is already updated in memory.
    //

    if (!HasPendingFileSystemEvent()) {
        return;
    }

    SendFileSystemEvent(ctx, TakePendingFileSystemEvent());
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
    if (!event.InvalidateNodeSize() && !event.InvalidateNodeRefSize()) {
        NCloud::Reply(
            ctx,
            *ev,
            std::make_unique<TResponse>(
                MakeError(E_ARGUMENT, "empty FileSystemEvent")));
        return;
    }

    auto response = std::make_unique<TResponse>();
    response->Record.SetClientCount(SendFileSystemEvent(ctx, event));
    NCloud::Reply(ctx, *ev, std::move(response));
}

}   // namespace NCloud::NFileStore::NStorage
