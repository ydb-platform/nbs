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
    // Called only by the requests whose responses the client is allowed to
    // cache (see CacheTTL in their responses): GetNodeAttr, GetNodeAttrBatch
    // (attrs of shard-resident nodes upon ListNodes), ListNodes and
    // ListNodesInternal. Upon completion these requests subscribe the client
    // to the returned Nodes and NodeRefs (see FileSystemEventSubscriptions);
    // GetNodeAttrBatch, ListNodes and ListNodesInternal do this only if
    // FileSystemListingEventsEnabled is set.
    // The other responses (e.g. CreateNode, CreateHandle) must not be cached.
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

void TIndexTabletActor::AddFileSystemEventNodeSubscriber(
    const TActorContext& ctx,
    const TActorId& pipeServerId,
    ui64 nodeId)
{
    //
    // The pipe may have been disconnected while the request was being
    // processed - its subscriptions would never be removed then.
    //

    if (!IsFileSystemEventsEnabled() ||
        !FileSystemEventClients.contains(pipeServerId))
    {
        return;
    }

    FileSystemEventSubscriptions.AddNodeSubscriber(
        nodeId,
        pipeServerId,
        ctx.Now());
}

void TIndexTabletActor::AddFileSystemEventNodeRefSubscriber(
    const TActorContext& ctx,
    const TActorId& pipeServerId,
    ui64 parentNodeId,
    const TString& name)
{
    if (!IsFileSystemEventsEnabled() ||
        IsShardInternalNodeRef(parentNodeId) ||
        !FileSystemEventClients.contains(pipeServerId))
    {
        return;
    }

    FileSystemEventSubscriptions.AddNodeRefSubscriber(
        parentNodeId,
        name,
        pipeServerId,
        ctx.Now());
}

void TIndexTabletActor::RemoveFileSystemEventClient(
    const TActorId& pipeServerId)
{
    FileSystemEventClients.erase(pipeServerId);
}

TDuration TIndexTabletActor::GetFileSystemEventsCacheTTL(
    const TActorId& pipeServerId) const
{
    //
    // The client may cache the returned Nodes and NodeRefs only if it gets
    // the invalidations for them: the events have to be generated and sent
    // and the client has to be reachable via the pipe it has sent the request
    // through. A subscription lives at least one seal period, the margin
    // covers the delivery and processing delays.
    //

    if (!IsFileSystemEventsEnabled() ||
        IsFileSystemEventsDryRunModeEnabled() ||
        !FileSystemEventClients.contains(pipeServerId))
    {
        return TDuration::Zero();
    }

    const auto sealPeriod = FileSystemEventSubscriptions.GetSealPeriod();
    const auto margin = Config->GetFileSystemEventSubscriptionsCacheTTLMargin();
    return sealPeriod > margin ? sealPeriod - margin : TDuration::Zero();
}

TDuration TIndexTabletActor::GetFileSystemListingEventsCacheTTL(
    const TActorId& pipeServerId) const
{
    //
    // GetNodeAttrBatch, ListNodes and ListNodesInternal subscribe the client
    // only if FileSystemListingEventsEnabled is set.
    //

    if (!IsFileSystemListingEventsEnabled()) {
        return TDuration::Zero();
    }

    return GetFileSystemEventsCacheTTL(pipeServerId);
}

ui32 TIndexTabletActor::SendFileSystemEvents(
    const TActorContext& ctx,
    const TVector<NProto::TFileSystemEvent>& events)
{
    const bool dryRun = IsFileSystemEventsDryRunModeEnabled();
    const auto& fileSystemId = GetMainFileSystemId();

    ui32 sendCount = 0;
    for (const auto& event: events) {
        //
        // Only the clients that have read the Node or the NodeRef get the
        // event (or all the clients of a generation if the Node or the
        // NodeRef is marked for broadcast there). The registrations are
        // removed - the clients register again upon the next read. This
        // applies to the dry run mode as well, so that the counters reflect
        // the real number of sends.
        //

        TVector<TActorId> pipeServerIds;
        if (event.HasInvalidateNode()) {
            pipeServerIds = FileSystemEventSubscriptions.ExtractNodeSubscribers(
                event.GetInvalidateNode().GetNodeId(),
                ctx.Now());
        } else if (event.HasInvalidateNodeRef()) {
            const auto& invalidate = event.GetInvalidateNodeRef();
            pipeServerIds =
                FileSystemEventSubscriptions.ExtractNodeRefSubscribers(
                    invalidate.GetNodeId(),
                    invalidate.GetName(),
                    ctx.Now());
        }

        THashSet<TActorId> clients;
        for (const auto& pipeServerId: pipeServerIds) {
            if (const auto* clientId = FileSystemEventClients.FindPtr(
                    pipeServerId))
            {
                clients.insert(*clientId);
            }
        }

        if (clients.empty()) {
            continue;
        }

        sendCount += clients.size();
        auto& counter = event.HasInvalidateNode()
            ? Metrics->InvalidateNodeEventSendCount
            : Metrics->InvalidateNodeRefEventSendCount;
        counter.fetch_add(clients.size(), std::memory_order_relaxed);

        if (dryRun) {
            continue;
        }

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

    return sendCount;
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
