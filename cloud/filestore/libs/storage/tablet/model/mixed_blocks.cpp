#include "mixed_blocks.h"

#include "alloc.h"
#include "deletion_markers.h"

#include <cloud/storage/core/libs/common/lru_cache.h>

#include <util/generic/hash_set.h>
#include <util/generic/intrlist.h>

#include <array>

namespace NCloud::NFileStore::NStorage {

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr size_t NumLevels = 3;

////////////////////////////////////////////////////////////////////////////////

struct TBlobMeta: TIntrusiveListItem<TBlobMeta>
{
    const TPartialBlobId BlobId;
    const TBlockList BlockList;
    const TMixedBlobStats Stats;
    const size_t Level;

    TBlobMeta(
            const TPartialBlobId& blobId,
            TBlockList blockList,
            const TMixedBlobStats& stats,
            size_t level)
        : BlobId(blobId)
        , BlockList(std::move(blockList))
        , Stats(stats)
        , Level(level)
    {}
};

using TBlobMetaList = TIntrusiveList<TBlobMeta>;

////////////////////////////////////////////////////////////////////////////////

struct TBlobMetaOps
{
    struct TEqual
    {
        template <typename T1, typename T2>
        bool operator ()(const T1& l, const T2& r) const
        {
            return GetBlobId(l) == GetBlobId(r);
        }
    };

    struct THash
    {
        template <typename T>
        size_t operator ()(const T& value) const
        {
            return GetBlobId(value).GetHash();
        }
    };

    static const TPartialBlobId& GetBlobId(const TBlobMeta& blob)
    {
        return blob.BlobId;
    }

    static const TPartialBlobId& GetBlobId(const TPartialBlobId& blobId)
    {
        return blobId;
    }
};

using TBlobMetaMap = THashSet<
    TBlobMeta,
    TBlobMetaOps::THash,
    TBlobMetaOps::TEqual,
    TStlAllocator
>;

////////////////////////////////////////////////////////////////////////////////

struct TLevel
{
    TBlobMetaList Blobs;
    size_t BlobsCount = 0;
};

////////////////////////////////////////////////////////////////////////////////

struct TRange
{
    TBlobMetaMap Blobs;
    TDeletionMarkers DeletionMarkers;
    std::array<TLevel, NumLevels> Levels;

    ui64 RefCount = 1;

    void Swap(TRange& other)
    {
        Blobs.swap(other.Blobs);
        DeletionMarkers.Swap(other.DeletionMarkers);
        Levels.swap(other.Levels);
        std::swap(RefCount, other.RefCount);
    }

    explicit TRange(IAllocator* alloc)
        : Blobs{alloc}
        , DeletionMarkers(alloc)
    {}
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

struct TMixedBlocks::TImpl
{
    // Holds all ranges that are actively used, i.e. have ref count > 0
    using TRangeMap = THashMap<ui32, TRange>;
    // Is used to store ranges that are not actively used, i.e. have
    // ref count == 0. May be useful for caching
    using TOffloadedRangeMap = NCloud::TLRUCache<ui32, TRange>;

    void SetOffloadedRangesCapacity(ui64 offloadedRangesCapacity)
    {
        OffloadedRanges.SetMaxSize(offloadedRangesCapacity);
    }

    explicit TImpl(IAllocator* alloc)
        : Alloc(alloc)
        , OffloadedRanges(alloc)
    {}

    TRange* FindRange(ui32 rangeId)
    {
        auto* it = Ranges.FindPtr(rangeId);
        if (it) {
            return it;
        }
        return OffloadedRanges.FindPtr(rangeId);
    }

    IAllocator* Alloc;
    TRangeMap Ranges;
    TOffloadedRangeMap OffloadedRanges;
};

////////////////////////////////////////////////////////////////////////////////

TMixedBlocks::TMixedBlocks(IAllocator* alloc)
    : Impl(new TImpl(alloc))
{}

TMixedBlocks::~TMixedBlocks()
{}

void TMixedBlocks::Reset(ui64 offloadedRangesCapacity)
{
    Impl->SetOffloadedRangesCapacity(offloadedRangesCapacity);
}

bool TMixedBlocks::IsLoaded(ui32 rangeId) const
{
    auto* range = Impl->FindRange(rangeId);
    return range;
}

void TMixedBlocks::RefRange(ui32 rangeId)
{
    TImpl::TRangeMap::insert_ctx ctx = nullptr;
    if (auto it = Impl->Ranges.find(rangeId, ctx); it == Impl->Ranges.end()) {
        // If the range is offloaded, move it to active ranges. Otherwise,
        // create a new range
        if (auto offloadedIt = Impl->OffloadedRanges.find(rangeId)) {
            Impl->Ranges.emplace_direct(ctx, rangeId, Impl->Alloc);
            Impl->Ranges.at(rangeId).Swap(offloadedIt->second);
            Impl->Ranges.at(rangeId).RefCount = 1;
            Impl->OffloadedRanges.erase(offloadedIt);
        } else {
            Impl->Ranges.emplace_direct(ctx, rangeId, Impl->Alloc);
        }
    } else {
        it->second.RefCount++;
    }
}

void TMixedBlocks::UnRefRange(ui32 rangeId)
{
    auto it = Impl->Ranges.find(rangeId);
    Y_ABORT_UNLESS(it != Impl->Ranges.end(), "already removed range: %u", rangeId);
    Y_ABORT_UNLESS(it->second.RefCount, "invalid ref count for range: %u", rangeId);

    it->second.RefCount--;

    // If ref count drops to 0, move the range to offloaded ranges. No need to
    // offload the range if the capacity is 0
    if (it->second.RefCount == 0) {
        if (Impl->OffloadedRanges.GetMaxSize() != 0) {
            auto [_, inserted, evicted] =
                Impl->OffloadedRanges.emplace(rangeId, Impl->Alloc);
            Y_UNUSED(evicted);
            Y_DEBUG_ABORT_UNLESS(inserted);
            Impl->OffloadedRanges.at(rangeId).Swap(it->second);
        }
        Impl->Ranges.erase(it);
    }
}

bool TMixedBlocks::AddBlocks(
    ui32 rangeId,
    const TPartialBlobId& blobId,
    TBlockList blockList,
    const TMixedBlobStats& stats)
{
    auto* range = Impl->FindRange(rangeId);
    Y_ABORT_UNLESS(range);

    // TODO: pick level
    auto [it, inserted] = range->Blobs.emplace(
        blobId,
        std::move(blockList),
        stats,
        0);

    if (!inserted) {
        return false;
    }

    auto& blob = const_cast<TBlobMeta&>(*it);

    auto& level = range->Levels[blob.Level];
    level.Blobs.PushBack(&blob);
    ++level.BlobsCount;

    return true;
}

bool TMixedBlocks::RemoveBlocks(
    ui32 rangeId,
    const TPartialBlobId& blobId,
    TMixedBlobStats* stats)
{
    auto* range = Impl->FindRange(rangeId);
    Y_ABORT_UNLESS(range);

    auto it = range->Blobs.find(blobId);
    if (it == range->Blobs.end()) {
        return false;
    }

    auto& blob = const_cast<TBlobMeta&>(*it);

    auto& level = range->Levels[blob.Level];
    blob.Unlink();
    --level.BlobsCount;

    if (stats) {
        *stats = blob.Stats;
    }

    range->Blobs.erase(it);
    return true;
}

void TMixedBlocks::FindBlocks(
    IMixedBlockVisitor& visitor,
    ui32 rangeId,
    ui64 nodeId,
    ui64 commitId,
    ui32 blockIndex,
    ui32 blocksCount) const
{
    auto* range = Impl->FindRange(rangeId);
    Y_ABORT_UNLESS(range);

    // TODO: limit range scan
    for (const auto& blob: range->Blobs) {
        auto iter = blob.BlockList.FindBlocks(
            nodeId,
            commitId,
            blockIndex,
            blocksCount);

        while (iter.Next()) {
            auto& block = iter.Block;

            Y_ABORT_UNLESS(block.NodeId == nodeId);
            Y_ABORT_UNLESS(block.MinCommitId <= commitId);

            const auto deletionMarkersEmpty = range->DeletionMarkers.Empty();
            if (iter.BlocksCount > 1 && !deletionMarkersEmpty) {
                auto deletionMarkerIter = range->DeletionMarkers.FindBlocks(
                    block,
                    iter.BlocksCount);

                auto b = block;

                ui32 blobOffset = iter.BlobOffset;
                while (deletionMarkerIter.Next()) {
                    b.MaxCommitId = deletionMarkerIter.MaxCommitId;

                    if (commitId < b.MaxCommitId) {
                        visitor.Accept(
                            b,
                            blob.BlobId,
                            blobOffset,
                            deletionMarkerIter.BlocksCount);
                    }

                    b.BlockIndex += deletionMarkerIter.BlocksCount;
                    blobOffset += deletionMarkerIter.BlocksCount;
                }
            } else {
                if (!deletionMarkersEmpty) {
                    range->DeletionMarkers.Apply(block);
                }

                if (commitId < block.MaxCommitId) {
                    visitor.Accept(
                        block,
                        blob.BlobId,
                        iter.BlobOffset,
                        iter.BlocksCount);
                }
            }
        }
    }
}

void TMixedBlocks::AddDeletionMarker(
    ui32 rangeId,
    TDeletionMarker deletionMarker)
{
    auto* range = Impl->FindRange(rangeId);
    Y_ABORT_UNLESS(range);

    range->DeletionMarkers.Add(deletionMarker);
}

TVector<TDeletionMarker> TMixedBlocks::ExtractDeletionMarkers(ui32 rangeId)
{
    auto* range = Impl->FindRange(rangeId);
    Y_ABORT_UNLESS(range);

    return range->DeletionMarkers.Extract();
}

void TMixedBlocks::ApplyDeletionMarkers(
    const IBlockLocation2RangeIndex& hasher,
    TVector<TBlock>& blocks) const
{
    const auto rangeId = GetMixedRangeIndex(hasher, blocks);

    auto* range = Impl->FindRange(rangeId);
    Y_ABORT_UNLESS(range);

    range->DeletionMarkers.Apply(MakeArrayRef(blocks));
}

TVector<TMixedBlobMeta> TMixedBlocks::ApplyDeletionMarkers(ui32 rangeId) const
{
    auto* range = Impl->FindRange(rangeId);
    Y_ABORT_UNLESS(range);

    TVector<TMixedBlobMeta> result;

    for (const auto& blob: range->Blobs) {
        auto blocks = blob.BlockList.DecodeBlocks();

        if (range->DeletionMarkers.Apply(MakeArrayRef(blocks)) > 0) {
            result.emplace_back(blob.BlobId, std::move(blocks));
        }
    }

    return result;
}

auto TMixedBlocks::ApplyDeletionMarkersAndGetMetas(ui32 rangeId) const
    -> TVector<TDeletionMarkerApplicationResult>
{
    auto* range = Impl->FindRange(rangeId);
    Y_ABORT_UNLESS(range);

    TVector<TDeletionMarkerApplicationResult> result;

    for (const auto& blob: range->Blobs) {
        auto blocks = blob.BlockList.DecodeBlocks();
        const bool affected =
            range->DeletionMarkers.Apply(MakeArrayRef(blocks)) > 0;

        result.emplace_back(
            TMixedBlobMeta{blob.BlobId, std::move(blocks)},
            affected);
    }

    return result;
}

TVector<TMixedBlobMeta> TMixedBlocks::GetBlobsForCompaction(ui32 rangeId) const
{
    auto* range = Impl->FindRange(rangeId);
    Y_ABORT_UNLESS(range);

    TVector<TMixedBlobMeta> result;

    // TODO: pick level
    for (const auto& blob: range->Blobs) {
        auto blocks = blob.BlockList.DecodeBlocks();

        range->DeletionMarkers.Apply(MakeArrayRef(blocks));
        result.emplace_back(blob.BlobId, std::move(blocks));
    }

    return result;
}

TMixedBlobMeta TMixedBlocks::FindBlob(ui32 rangeId, TPartialBlobId blobId) const
{
    auto* range = Impl->FindRange(rangeId);
    Y_ABORT_UNLESS(range);

    TVector<TMixedBlobMeta> result;

    auto it = range->Blobs.find(blobId);
    Y_ABORT_UNLESS(it != range->Blobs.end());

    auto blocks = it->BlockList.DecodeBlocks();
    range->DeletionMarkers.Apply(MakeArrayRef(blocks));

    return {it->BlobId, std::move(blocks)};
}

ui32 TMixedBlocks::CalculateGarbageBlockCount(ui32 rangeId) const
{
    auto* range = Impl->FindRange(rangeId);
    Y_DEBUG_ABORT_UNLESS(range);
    if (!range) {
        return 0;
    }

    ui32 result = 0;
    for (const auto& blob: range->Blobs) {
        auto blocks = blob.BlockList.DecodeBlocks();

        range->DeletionMarkers.Apply(MakeArrayRef(blocks));
        for (const auto& block: blocks) {
            // TODO(#1923): take checkpoints into account
            if (block.MaxCommitId != InvalidCommitId) {
                ++result;
            }
        }
    }

    return result;
}

TBlobMetaMapStats TMixedBlocks::GetBlobMetaMapStats() const
{
    return {
        .LoadedRanges = Impl->Ranges.size(),
        .OffloadedRanges = Impl->OffloadedRanges.size(),
    };
}

}   // namespace NCloud::NFileStore::NStorage
