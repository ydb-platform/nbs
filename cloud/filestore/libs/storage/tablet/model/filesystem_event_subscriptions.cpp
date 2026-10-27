#include "filesystem_event_subscriptions.h"

#include <util/generic/bitops.h>

#include <utility>

namespace NCloud::NFileStore::NStorage {

using namespace NActors;

////////////////////////////////////////////////////////////////////////////////

void TFileSystemEventSubscriptions::TClientMask::Set(ui32 clientIndex)
{
    const ui32 pageNo = clientIndex / PageSize;
    const ui64 bit = 1ULL << (clientIndex % PageSize);

    if (pageNo == 0) {
        FirstPage |= bit;
        return;
    }

    if (!OtherPages) {
        OtherPages = std::make_unique<TVector<TPage>>();
    }

    for (auto& page: *OtherPages) {
        if (page.PageNo == pageNo) {
            page.Bits |= bit;
            return;
        }
    }

    OtherPages->push_back({.PageNo = pageNo, .Bits = bit});
}

template <typename TFunc>
void TFileSystemEventSubscriptions::TClientMask::ForEachInPage(
    ui64 bits,
    ui32 base,
    TFunc& func)
{
    while (bits) {
        func(base + CountTrailingZeroBits(bits));
        bits &= bits - 1;
    }
}

template <typename TFunc>
void TFileSystemEventSubscriptions::TClientMask::ForEach(TFunc&& func) const
{
    ForEachInPage(FirstPage, 0, func);

    if (OtherPages) {
        for (const auto& page: *OtherPages) {
            ForEachInPage(page.Bits, page.PageNo * PageSize, func);
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

ui32 TFileSystemEventSubscriptions::TGeneration::GetClientIndex(
    const TActorId& pipeServerId)
{
    auto [it, inserted] =
        ClientIndices.try_emplace(pipeServerId, Clients.size());
    if (inserted) {
        Clients.push_back(pipeServerId);
    }

    return it->second;
}

void TFileSystemEventSubscriptions::TGeneration::AddNode(
    ui64 nodeId,
    const TActorId& pipeServerId)
{
    NodeMasks[nodeId].Set(GetClientIndex(pipeServerId));
}

void TFileSystemEventSubscriptions::TGeneration::AddNodeRef(
    const TNodeRefKey& key,
    const TActorId& pipeServerId)
{
    NodeRefMasks[key].Set(GetClientIndex(pipeServerId));
}

void TFileSystemEventSubscriptions::TGeneration::CollectClients(
    const TClientMask& mask,
    THashSet<TActorId>& out) const
{
    mask.ForEach([&] (ui32 clientIndex) {
        out.insert(Clients[clientIndex]);
    });
}

void TFileSystemEventSubscriptions::TGeneration::ExtractNode(
    ui64 nodeId,
    THashSet<TActorId>& out)
{
    auto it = NodeMasks.find(nodeId);
    if (it == NodeMasks.end()) {
        return;
    }

    CollectClients(it->second, out);
    NodeMasks.erase(it);
}

void TFileSystemEventSubscriptions::TGeneration::ExtractNodeRef(
    const TNodeRefKey& key,
    THashSet<TActorId>& out)
{
    auto it = NodeRefMasks.find(key);
    if (it == NodeRefMasks.end()) {
        return;
    }

    CollectClients(it->second, out);
    NodeRefMasks.erase(it);
}

ui64 TFileSystemEventSubscriptions::TGeneration::GetClientCount() const
{
    return Clients.size();
}

ui64 TFileSystemEventSubscriptions::TGeneration::GetNodeCount() const
{
    return NodeMasks.size();
}

ui64 TFileSystemEventSubscriptions::TGeneration::GetNodeRefCount() const
{
    return NodeRefMasks.size();
}

////////////////////////////////////////////////////////////////////////////////

TFileSystemEventSubscriptions::TFileSystemEventSubscriptions(
        TDuration sealPeriod)
    : SealPeriod(sealPeriod)
{}

void TFileSystemEventSubscriptions::SealIfNeeded(TInstant now)
{
    if (!LastSealTs) {
        LastSealTs = now;
        return;
    }

    if (now - LastSealTs < SealPeriod) {
        return;
    }

    //
    // Drop the sealed generation, seal the active one, start a new active
    // one.
    //

    Sealed = std::exchange(Active, TGeneration());
    LastSealTs = now;
}

void TFileSystemEventSubscriptions::AddNodeSubscriber(
    ui64 nodeId,
    const TActorId& pipeServerId,
    TInstant now)
{
    SealIfNeeded(now);
    Active.AddNode(nodeId, pipeServerId);
}

void TFileSystemEventSubscriptions::AddNodeRefSubscriber(
    ui64 parentNodeId,
    const TString& name,
    const TActorId& pipeServerId,
    TInstant now)
{
    SealIfNeeded(now);
    Active.AddNodeRef(TNodeRefKey(parentNodeId, name), pipeServerId);
}

TVector<TActorId> TFileSystemEventSubscriptions::ExtractNodeSubscribers(
    ui64 nodeId,
    TInstant now)
{
    SealIfNeeded(now);

    THashSet<TActorId> clients;
    Sealed.ExtractNode(nodeId, clients);
    Active.ExtractNode(nodeId, clients);
    return {clients.begin(), clients.end()};
}

TVector<TActorId> TFileSystemEventSubscriptions::ExtractNodeRefSubscribers(
    ui64 parentNodeId,
    const TString& name,
    TInstant now)
{
    SealIfNeeded(now);

    const TNodeRefKey key(parentNodeId, name);
    THashSet<TActorId> clients;
    Sealed.ExtractNodeRef(key, clients);
    Active.ExtractNodeRef(key, clients);
    return {clients.begin(), clients.end()};
}

ui64 TFileSystemEventSubscriptions::GetNodeCount() const
{
    return Active.GetNodeCount() + Sealed.GetNodeCount();
}

ui64 TFileSystemEventSubscriptions::GetNodeRefCount() const
{
    return Active.GetNodeRefCount() + Sealed.GetNodeRefCount();
}

ui64 TFileSystemEventSubscriptions::GetClientCount() const
{
    return Active.GetClientCount() + Sealed.GetClientCount();
}

}   // namespace NCloud::NFileStore::NStorage
