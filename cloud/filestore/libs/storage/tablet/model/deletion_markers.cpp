#include "deletion_markers.h"

#include "alloc.h"

#include <util/generic/hash.h>
#include <util/generic/set.h>

namespace NCloud::NFileStore::NStorage {

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TDeletedRange
{
    ui32 Start;
    ui32 End;
    ui64 CommitId;

    TDeletedRange(ui32 start, ui32 end, ui64 commitId)
        : Start(start)
        , End(end)
        , CommitId(commitId)
    {}
};

struct TDeletedRangeLess
{
    using is_transparent = void;

    bool operator()(const auto& lhs, const auto& rhs) const
    {
        return GetEnd(lhs) < GetEnd(rhs);
    }

    static ui32 GetEnd(const TDeletedRange& deletedRange)
    {
        return deletedRange.End;
    }

    static ui32 GetEnd(ui32 end)
    {
        return end;
    }
};

struct TDisjointRangeMap: TSet<TDeletedRange, TDeletedRangeLess, TStlAllocator>
{
    using TBase = TSet<TDeletedRange, TDeletedRangeLess, TStlAllocator>;

    TDisjointRangeMap(IAllocator* alloc)
        : TBase(alloc)
    {}
};

using TDisjointRangeMapByNodeId = THashMap<
    ui64,
    TDisjointRangeMap,
    THash<ui64>,
    TEqualTo<ui64>,
    TStlAllocator
>;

}   // namespace

////////////////////////////////////////////////////////////////////////////////

class TDeletionMarkers::TImpl
{
private:
    TVector<TDeletionMarker> DeletionMarkers;
    TDisjointRangeMapByNodeId DisjointRangeMapByNodeId;

    IAllocator* Alloc;

public:
    TImpl(IAllocator* alloc)
        : DisjointRangeMapByNodeId{alloc}
        , Alloc(alloc)
    {}

    void Add(TDeletionMarker deletionMarker);

    ui32 Apply(TArrayRef<TBlock> blocks) const;

    TVector<TDeletionMarker> Extract();
};

////////////////////////////////////////////////////////////////////////////////

void TDeletionMarkers::TImpl::Add(TDeletionMarker deletionMarker)
{
    TDisjointRangeMapByNodeId::insert_ctx ctx;
    auto it = DisjointRangeMapByNodeId.find(deletionMarker.NodeId, ctx);
    if (it == DisjointRangeMapByNodeId.end()) {
        it = DisjointRangeMapByNodeId.emplace_direct(ctx, deletionMarker.NodeId, Alloc);
    }

    auto& map = it->second;

    const ui32 start = deletionMarker.BlockIndex;
    const ui32 end = deletionMarker.BlockIndex + deletionMarker.BlockCount;

    auto lo = map.upper_bound(start);
    auto hi = map.upper_bound(end);

    if (lo != map.end() && lo->Start < start) {
        // cutting lo from the right side
        map.emplace(lo->Start, start, lo->CommitId);
    }

    if (hi != map.end() && hi->Start < end) {
        // cutting hi from the left side
        const_cast<TDeletedRange&>(*hi).Start = end;
    }

    while (lo != hi) {
        map.erase(lo++);
    }

    map.emplace(start, end, deletionMarker.CommitId);

    Y_IF_DEBUG({
        // check that ranges are disjoint
        ui32 prevEnd = 0;

        for (const auto deletedRange: map) {
            Y_ABORT_UNLESS(deletedRange.Start >= prevEnd);
            prevEnd = deletedRange.End;
        }
    });

    DeletionMarkers.push_back(deletionMarker);
}

ui32 TDeletionMarkers::TImpl::Apply(TArrayRef<TBlock> blocks) const
{
    ui32 updateCount = 0;

    for (auto& block: blocks) {
        auto it = DisjointRangeMapByNodeId.find(block.NodeId);
        if (it == DisjointRangeMapByNodeId.end()) {
            continue;
        }

        auto& map = it->second;

        auto jt = map.upper_bound(block.BlockIndex);
        if (jt != map.end() &&
            jt->Start <= block.BlockIndex &&
            jt->CommitId > block.MinCommitId &&
            jt->CommitId < block.MaxCommitId)
        {
            block.MaxCommitId = jt->CommitId;
            ++updateCount;
        }
    }

    return updateCount;
}

TVector<TDeletionMarker> TDeletionMarkers::TImpl::Extract()
{
    DisjointRangeMapByNodeId.clear();

    const ui32 prevCapacity = DeletionMarkers.capacity();

    TVector<TDeletionMarker> extracted{std::move(DeletionMarkers)};

    DeletionMarkers = {};
    DeletionMarkers.reserve(prevCapacity);

    return extracted;
}

////////////////////////////////////////////////////////////////////////////////

TDeletionMarkers::TDeletionMarkers(IAllocator* alloc)
    : Impl(new TImpl(alloc))
{}

TDeletionMarkers::~TDeletionMarkers() = default;

void TDeletionMarkers::Add(TDeletionMarker deletionMarker)
{
    return Impl->Add(deletionMarker);
}

ui32 TDeletionMarkers::Apply(TBlock& block) const
{
    return Apply(MakeArrayRef(&block, 1));
}

ui32 TDeletionMarkers::Apply(TArrayRef<TBlock> blocks) const
{
    return Impl->Apply(blocks);
}

TVector<TDeletionMarker> TDeletionMarkers::Extract()
{
    return Impl->Extract();
}

void TDeletionMarkers::Swap(TDeletionMarkers& other)
{
    Impl.swap(other.Impl);
}

}   // namespace NCloud::NFileStore::NStorage
