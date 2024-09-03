#include "large_blocks.h"

#include "sparse_segment.h"

#include <util/generic/set.h>

namespace NCloud::NFileStore::NStorage {

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TMarkerInfo
{
    TDeletionMarker Marker;
    TSparseSegment UnprocessedPart;

    explicit TMarkerInfo(TDeletionMarker marker, IAllocator* alloc)
        : Marker(marker)
        , UnprocessedPart(
            alloc,
            marker.BlockIndex,
            marker.BlockIndex + marker.BlockCount)
    {
    }
};

struct TMarkerInfoLess
{
    using is_transparent = void;

    bool operator()(const auto& lhs, const auto& rhs) const
    {
        return GetEnd(lhs) < GetEnd(rhs);
    }

    static ui64 GetEnd(const TMarkerInfo& markerInfo)
    {
        return markerInfo.Marker.BlockIndex + markerInfo.Marker.BlockCount;
    }

    static ui64 GetEnd(ui64 end)
    {
        return end;
    }
};

using TRangeMap = TSet<TMarkerInfo, TMarkerInfoLess, TStlAllocator>;

}   // namespace

////////////////////////////////////////////////////////////////////////////////

struct TLargeBlocks::TImpl
{
    IAllocator* Alloc;
    using TByNodeId =
        THashMap<ui64, TRangeMap, THash<ui64>, TEqualTo<ui64>, TStlAllocator>;
    TByNodeId NodeId2Markers;
    TVector<TDeletionMarker> ProcessedMarkers;
    // Right now this implementation never decrements MaxMarkerBlocks for
    // simplicity. It's ok since most of the markers are going to be similar
    // in size and there is an upper limit for marker length.
    ui64 MaxMarkerBlocks = 0;

    explicit TImpl(IAllocator* alloc)
        : Alloc(alloc)
        , NodeId2Markers(alloc)
    {
    }

    void Apply(TVector<TBlock>& blocks, bool update)
    {
        for (auto& block: blocks) {
            auto it = NodeId2Markers.find(block.NodeId);
            if (it == NodeId2Markers.end()) {
                continue;
            }

            auto rangeIt = it->second.upper_bound(block.BlockIndex);
            while (rangeIt != it->second.end()) {
                const ui64 end =
                    rangeIt->Marker.BlockIndex + rangeIt->Marker.BlockCount;
                if (block.BlockIndex + MaxMarkerBlocks < end) {
                    break;
                }

                const bool inside = rangeIt->Marker.BlockIndex <= block.BlockIndex
                    && end > block.BlockIndex;
                if (!inside) {
                    ++rangeIt;
                    continue;
                }

                block.MaxCommitId =
                    Min(block.MaxCommitId, rangeIt->Marker.CommitId);

                if (update) {
                    const_cast<TSparseSegment&>(rangeIt->UnprocessedPart)
                        .PunchHole(block.BlockIndex, block.BlockIndex + 1);

                    if (rangeIt->UnprocessedPart.Empty()) {
                        ProcessedMarkers.push_back(rangeIt->Marker);
                        rangeIt = it->second.erase(rangeIt);
                        continue;
                    }
                }

                ++rangeIt;
            }

            if (update && it->second.empty()) {
                NodeId2Markers.erase(it);
            }
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

TLargeBlocks::TLargeBlocks(IAllocator* alloc)
    : Impl(new TImpl{alloc})
{}

TLargeBlocks::~TLargeBlocks() = default;

void TLargeBlocks::AddDeletionMarker(TDeletionMarker deletionMarker)
{
    TImpl::TByNodeId::insert_ctx ctx;
    auto it = Impl->NodeId2Markers.find(deletionMarker.NodeId, ctx);
    if (it == Impl->NodeId2Markers.end()) {
        it = Impl->NodeId2Markers.emplace_direct(
            ctx,
            deletionMarker.NodeId,
            Impl->Alloc);
    }
    it->second.emplace(deletionMarker, Impl->Alloc);
    Impl->MaxMarkerBlocks =
        Max<ui64>(Impl->MaxMarkerBlocks, deletionMarker.BlockCount);
}

void TLargeBlocks::ApplyDeletionMarkers(TVector<TBlock>& blocks) const
{
    Impl->Apply(blocks, false);
}

void TLargeBlocks::ApplyAndUpdateDeletionMarkers(TVector<TBlock>& blocks)
{
    Impl->Apply(blocks, true);
}

TDeletionMarker TLargeBlocks::GetOne() const
{
    const TDeletionMarker invalid(0, InvalidCommitId, 0, 0);
    if (Impl->NodeId2Markers.empty()) {
        return invalid;
    }

    const auto& markers = Impl->NodeId2Markers.begin()->second;
    Y_DEBUG_ABORT_UNLESS(!markers.empty());
    if (markers.empty()) {
        return invalid;
    }

    return markers.begin()->Marker;
}

TVector<TDeletionMarker> TLargeBlocks::ExtractProcessedDeletionMarkers()
{
    auto res = std::move(Impl->ProcessedMarkers);
    Impl->ProcessedMarkers.clear();
    return res;
}

}   // namespace NCloud::NFileStore::NStorage
