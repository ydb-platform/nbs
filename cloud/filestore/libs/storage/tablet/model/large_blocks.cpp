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

    bool operator()(const TMarkerInfo& lhs, const TMarkerInfo& rhs) const
    {
        return std::make_tuple(
            GetEnd(lhs),
            lhs.Marker.BlockIndex,
            lhs.Marker.CommitId
        ) < std::make_tuple(
            GetEnd(rhs),
            rhs.Marker.BlockIndex,
            rhs.Marker.CommitId);
    }

    bool operator()(const ui64 lhs, const TMarkerInfo& rhs) const
    {
        return lhs < GetEnd(rhs);
    }

    static ui64 GetEnd(const TMarkerInfo& markerInfo)
    {
        return markerInfo.Marker.BlockIndex + markerInfo.Marker.BlockCount;
    }
};

using TRangeMap = TSet<TMarkerInfo, TMarkerInfoLess, TStlAllocator>;

////////////////////////////////////////////////////////////////////////////////

struct TBlockVisitor final: public ILargeBlockVisitor
{
    TBlock& Block;
    bool& Affected;

    explicit TBlockVisitor(TBlock& block, bool& affected)
        : Block(block)
        , Affected(affected)
    {}

    void Accept(const TBlockDeletion& deletion) override
    {
        if (deletion.CommitId < Block.MaxCommitId
                && deletion.CommitId > Block.MinCommitId)
        {
            Block.MaxCommitId = deletion.CommitId;
            Affected = true;
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TNoOpVisitor final: public ILargeBlockVisitor
{
    void Accept(const TBlockDeletion& deletion) override
    {
        Y_UNUSED(deletion);
    }
};

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

    void Apply(
        ILargeBlockVisitor& visitor,
        ui64 nodeId,
        ui32 blockIndex,
        ui32 blockCount,
        ui64 commitId,
        bool update)
    {
        auto it = NodeId2Markers.find(nodeId);
        if (it == NodeId2Markers.end()) {
            return;
        }

        auto rangeIt = it->second.upper_bound(blockIndex);
        const auto lastBlock = blockIndex + blockCount - 1;
        while (rangeIt != it->second.end()) {
            const ui64 end =
                rangeIt->Marker.BlockIndex + rangeIt->Marker.BlockCount;
            if (lastBlock + MaxMarkerBlocks < end) {
                break;
            }

            if (commitId < rangeIt->Marker.CommitId) {
                ++rangeIt;
                continue;
            }

            const auto intersectionStart = Max(
                rangeIt->Marker.BlockIndex,
                blockIndex);
            const auto intersectionEnd = Min(
                rangeIt->Marker.BlockIndex + rangeIt->Marker.BlockCount,
                blockIndex + blockCount);

            for (auto b = intersectionStart; b < intersectionEnd; ++b) {
                visitor.Accept({
                    rangeIt->Marker.NodeId,
                    b,
                    rangeIt->Marker.CommitId});
            }

            if (update) {
                const_cast<TSparseSegment&>(rangeIt->UnprocessedPart)
                    .PunchHole(intersectionStart, intersectionEnd);

                if (rangeIt->UnprocessedPart.empty()) {
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

    bool Apply(TVector<TBlock>& blocks, bool update)
    {
        bool affected = false;
        for (auto& block: blocks) {
            TBlockVisitor visitor(block, affected);
            Apply(
                visitor,
                block.NodeId,
                block.BlockIndex,
                1,
                Max<ui64>(),
                update);
        }
        return affected;
    }

    void MarkProcessed(
        ui64 nodeId,
        ui64 commitId,
        ui32 blockIndex,
        ui32 blocksCount)
    {
        TNoOpVisitor visitor;
        Apply(
            visitor,
            nodeId,
            blockIndex,
            blocksCount,
            commitId,
            true);
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

bool TLargeBlocks::ApplyDeletionMarkers(TVector<TBlock>& blocks) const
{
    return Impl->Apply(blocks, false);
}

void TLargeBlocks::MarkProcessed(
    ui64 nodeId,
    ui64 commitId,
    ui32 blockIndex,
    ui32 blocksCount)
{
    Impl->MarkProcessed(nodeId, commitId, blockIndex, blocksCount);
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

void TLargeBlocks::FindBlocks(
    ILargeBlockVisitor& visitor,
    ui64 nodeId,
    ui64 commitId,
    ui32 blockIndex,
    ui32 blocksCount) const
{
    Impl->Apply(visitor, nodeId, blockIndex, blocksCount, commitId, false);
}

}   // namespace NCloud::NFileStore::NStorage
