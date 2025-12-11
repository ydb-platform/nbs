#include "mixed_index_cache.h"

#include <util/digest/multi.h>
#include <util/generic/hash_set.h>
#include <util/generic/intrlist.h>

namespace NCloud::NBlockStore::NStorage::NPartition {

////////////////////////////////////////////////////////////////////////////////

static constexpr ui32 ExpectedBlocksCapacity = 256;

////////////////////////////////////////////////////////////////////////////////

using ERangeTemperature = TMixedIndexCache::ERangeTemperature;
using TBlockKey = TMixedIndexCache::TBlockKey;
using IInserter = TMixedIndexCache::IInserter;
using TInserterPtr = TMixedIndexCache::TInserterPtr;

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TMixedBlockOps
{
    struct THash
    {
        ui64 operator()(const auto& value) const
        {
            return ComputeHash(GetBlockKey(value));
        }
    };

    struct TEqualTo
    {
        bool operator()(const auto& lhs, const auto& rhs) const
        {
            return GetBlockKey(lhs) == GetBlockKey(rhs);
        }
    };

    static TBlockKey GetBlockKey(const TMixedBlock& block)
    {
        return {block.BlockIndex, block.CommitId};
    }

    static TBlockKey GetBlockKey(const TBlockKey& blockKey)
    {
        return blockKey;
    }
};

////////////////////////////////////////////////////////////////////////////////

using TBlocks = THashSet<
    TMixedBlock,
    TMixedBlockOps::THash,
    TMixedBlockOps::TEqualTo,
    TStlAlloc<TMixedBlock> >;

////////////////////////////////////////////////////////////////////////////////

struct TCacheItem: public TIntrusiveListItem<TCacheItem>
{
    ui32 RangeIdx;
    ERangeTemperature Temperature = ERangeTemperature::Warm;
    TBlocks Blocks;

    TCacheItem(ui32 rangeIdx, IAllocator* allocator)
        : RangeIdx(rangeIdx)
        , Blocks(allocator)
    {}
};

////////////////////////////////////////////////////////////////////////////////

class TWarmRangeInserter final: public IInserter
{
private:
    TBlocks& Blocks;

public:
    TWarmRangeInserter(TBlocks& blocks)
        : Blocks(blocks)
    {}

    void Insert(TMixedBlock block) override
    {
        Blocks.insert(block);
    }
};

////////////////////////////////////////////////////////////////////////////////

class TColdRangeInserter final: public IInserter
{
public:
    void Insert(TMixedBlock block) override
    {
        Y_UNUSED(block);
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

class TMixedIndexCache::TImpl
{
private:
    using TIndex = THashMap<
        ui32,
        TCacheItem,
        THash<ui32>,
        TEqualTo<ui32>,
        TStlAlloc<int> >;
    using TList = TIntrusiveList<TCacheItem>;

    TIndex Index;
    TList List;

    ui32 Size = 0;
    const ui32 MaxSize;

    IAllocator* Allocator;

public:
    TImpl(ui32 maxSize, IAllocator* allocator);

    ERangeTemperature GetRangeTemperature(ui32 rangeIdx) const;
    void RaiseRangeTemperature(ui32 rangeIdx);

    TInserterPtr GetInserterForRange(ui32 rangeIdx);
    void InsertBlockIfHot(ui32 rangeIdx, TMixedBlock block);
    void EraseBlockIfHot(ui32 rangeIdx, TBlockKey blockKey);

    bool VisitBlocksIfHot(ui32 rangeIdx, IBlocksIndexVisitor& visitor);
};

////////////////////////////////////////////////////////////////////////////////

TMixedIndexCache::TImpl::TImpl(ui32 maxSize, IAllocator* allocator)
    : Index(allocator)
    , MaxSize(maxSize)
    , Allocator(allocator)
{}

ERangeTemperature TMixedIndexCache::TImpl::GetRangeTemperature(
    ui32 rangeIdx) const
{
    if (auto it = Index.find(rangeIdx); it != Index.end()) {
        return it->second.Temperature;
    }
    return ERangeTemperature::Cold;
}

void TMixedIndexCache::TImpl::RaiseRangeTemperature(ui32 rangeIdx)
{
    if (auto it = Index.find(rangeIdx); it != Index.end()) {
        auto& item = it->second;
        item.Temperature = ERangeTemperature::Hot;
        List.Remove(&item);
        List.PushBack(&item);
        return;
    }

    auto [it, emplaced] = Index.try_emplace(rangeIdx, rangeIdx, Allocator);

    Y_ABORT_UNLESS(emplaced);
    List.PushBack(&it->second);

    if (Size != MaxSize) {
        // no need to prune
        ++Size;
        return;
    }

    auto* removed = List.PopFront();
    const size_t removedCount = Index.erase(removed->RangeIdx);
    Y_ABORT_UNLESS(removedCount == 1);
}

TInserterPtr TMixedIndexCache::TImpl::GetInserterForRange(ui32 rangeIdx)
{
    auto it = Index.find(rangeIdx);
    if (it == Index.end()) {
        return std::make_unique<TColdRangeInserter>();
    }

    Y_ABORT_UNLESS(it->second.Temperature == ERangeTemperature::Warm);

    auto& blocks = it->second.Blocks;
    blocks.clear();
    blocks.reserve(ExpectedBlocksCapacity);

    return std::make_unique<TWarmRangeInserter>(blocks);
}

void TMixedIndexCache::TImpl::InsertBlockIfHot(ui32 rangeIdx, TMixedBlock block)
{
    auto it = Index.find(rangeIdx);
    if (it != Index.end() && it->second.Temperature == ERangeTemperature::Hot) {
        const bool inserted = it->second.Blocks.insert(block).second;
        Y_ABORT_UNLESS(
            inserted,
            "duplicate block (blockIndex: %u, commitId: %lu",
            block.BlockIndex,
            block.CommitId);
    }
}

void TMixedIndexCache::TImpl::EraseBlockIfHot(ui32 rangeIdx, TBlockKey blockKey)
{
    auto it = Index.find(rangeIdx);
    if (it != Index.end() && it->second.Temperature == ERangeTemperature::Hot) {
        auto& blocks = it->second.Blocks;
        if (auto jt = blocks.find(blockKey); jt != blocks.end()) {
            blocks.erase(jt);
        }
    }
}

bool TMixedIndexCache::TImpl::VisitBlocksIfHot(
    ui32 rangeIdx,
    IBlocksIndexVisitor& visitor)
{
    auto it = Index.find(rangeIdx);
    if (it == Index.end() || it->second.Temperature != ERangeTemperature::Hot) {
        return false;
    }

    for (const auto& b: it->second.Blocks) {
        if (!visitor.Visit(b.BlockIndex, b.CommitId, b.BlobId, b.BlobOffset)) {
            // interrupted
            return true;
        }
    }

    return true;
}

////////////////////////////////////////////////////////////////////////////////

TMixedIndexCache::TMixedIndexCache(ui32 maxSize, IAllocator* allocator)
    : Impl(new TImpl(maxSize, allocator))
{}

TMixedIndexCache::~TMixedIndexCache() = default;

ERangeTemperature TMixedIndexCache::GetRangeTemperature(ui32 rangeIdx) const
{
    return Impl->GetRangeTemperature(rangeIdx);
}

void TMixedIndexCache::RaiseRangeTemperature(ui32 rangeIdx)
{
    Impl->RaiseRangeTemperature(rangeIdx);
}

TInserterPtr TMixedIndexCache::GetInserterForRange(ui32 rangeIdx)
{
    return Impl->GetInserterForRange(rangeIdx);
}

void TMixedIndexCache::InsertBlockIfHot(ui32 rangeIdx, TMixedBlock block)
{
    Impl->InsertBlockIfHot(rangeIdx, block);
}

void TMixedIndexCache::EraseBlockIfHot(ui32 rangeIdx, TBlockKey blockKey)
{
    Impl->EraseBlockIfHot(rangeIdx, blockKey);
}

bool TMixedIndexCache::VisitBlocksIfHot(
    ui32 rangeIdx,
    IBlocksIndexVisitor& visitor)
{
    return Impl->VisitBlocksIfHot(rangeIdx, visitor);
}

}   // namespace NCloud::NBlockStore::NStorage::NPartition
