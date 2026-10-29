#include "filesystem_event_subscriptions.h"

#include <util/generic/bitops.h>

#include <utility>

namespace NCloud::NFileStore::NStorage {

using namespace NActors;

////////////////////////////////////////////////////////////////////////////////

ui32 TFileSystemEventSubscriptions::TClientMask::Set(ui32 clientIndex)
{
    const ui32 pageNo = clientIndex / PageSize;
    const ui64 bit = 1ULL << (clientIndex % PageSize);

    if (pageNo == 0) {
        FirstPage |= bit;
        return 0;
    }

    if (!OtherPages) {
        OtherPages = std::make_unique<TVector<TPage>>();
    }

    for (auto& page: *OtherPages) {
        if (page.PageNo == pageNo) {
            page.Bits |= bit;
            return 0;
        }
    }

    OtherPages->push_back({.PageNo = pageNo, .Bits = bit});
    return 1;
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

ui32 TFileSystemEventSubscriptions::TClientMask::GetExtraPageCount() const
{
    return OtherPages ? OtherPages->size() : 0;
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
    ExtraPageCount += NodeMasks[nodeId].Set(GetClientIndex(pipeServerId));
}

void TFileSystemEventSubscriptions::TGeneration::AddNodeRef(
    const TNodeRefKey& key,
    const TActorId& pipeServerId)
{
    ExtraPageCount += NodeRefMasks[key].Set(GetClientIndex(pipeServerId));
}

void TFileSystemEventSubscriptions::TGeneration::AddNodeXAttr(
    const TNodeXAttrKey& key,
    const TActorId& pipeServerId)
{
    ExtraPageCount += NodeXAttrMasks[key].Set(GetClientIndex(pipeServerId));
}

void TFileSystemEventSubscriptions::TGeneration::CollectClients(
    const TClientMask& mask,
    THashSet<TActorId>& out) const
{
    mask.ForEach([&] (ui32 clientIndex) {
        out.insert(Clients[clientIndex]);
    });
}

template <typename TMasks>
void TFileSystemEventSubscriptions::TGeneration::Extract(
    TMasks& masks,
    const typename TMasks::key_type& key,
    THashSet<TActorId>& out)
{
    auto it = masks.find(key);
    if (it == masks.end()) {
        return;
    }

    CollectClients(it->second, out);
    ExtraPageCount -= it->second.GetExtraPageCount();
    masks.erase(it);
}

void TFileSystemEventSubscriptions::TGeneration::ExtractNode(
    ui64 nodeId,
    THashSet<TActorId>& out)
{
    Extract(NodeMasks, nodeId, out);
}

void TFileSystemEventSubscriptions::TGeneration::ExtractNodeRef(
    const TNodeRefKey& key,
    THashSet<TActorId>& out)
{
    Extract(NodeRefMasks, key, out);
}

void TFileSystemEventSubscriptions::TGeneration::ExtractNodeXAttr(
    const TNodeXAttrKey& key,
    THashSet<TActorId>& out)
{
    Extract(NodeXAttrMasks, key, out);
}

auto TFileSystemEventSubscriptions::TGeneration::GetStats() const -> TStats
{
    return {
        .NodeCount = NodeMasks.size(),
        .NodeRefCount = NodeRefMasks.size(),
        .ClientCount = Clients.size(),
        .ExtraPageCount = ExtraPageCount,
        .NodeXAttrCount = NodeXAttrMasks.size(),
    };
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

void TFileSystemEventSubscriptions::AddNodeXAttrSubscriber(
    ui64 nodeId,
    const TString& name,
    const TActorId& pipeServerId,
    TInstant now)
{
    SealIfNeeded(now);
    Active.AddNodeXAttr(TNodeXAttrKey(nodeId, name), pipeServerId);
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

TVector<TActorId> TFileSystemEventSubscriptions::ExtractNodeXAttrSubscribers(
    ui64 nodeId,
    const TString& name,
    TInstant now)
{
    SealIfNeeded(now);

    const TNodeXAttrKey key(nodeId, name);
    THashSet<TActorId> clients;
    Sealed.ExtractNodeXAttr(key, clients);
    Active.ExtractNodeXAttr(key, clients);
    return {clients.begin(), clients.end()};
}

auto TFileSystemEventSubscriptions::GetStats() const -> TStats
{
    const auto aStats = Active.GetStats();
    const auto sStats = Sealed.GetStats();
    return {
        .NodeCount = aStats.NodeCount + sStats.NodeCount,
        .NodeRefCount = aStats.NodeRefCount + sStats.NodeRefCount,
        .ClientCount = aStats.ClientCount + sStats.ClientCount,
        .ExtraPageCount = aStats.ExtraPageCount + sStats.ExtraPageCount,
        .NodeXAttrCount = aStats.NodeXAttrCount + sStats.NodeXAttrCount,
    };
}

TDuration TFileSystemEventSubscriptions::GetSealPeriod() const
{
    return SealPeriod;
}

}   // namespace NCloud::NFileStore::NStorage
