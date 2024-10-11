#include "block_index.h"

#include <cloud/storage/core/libs/common/alloc.h>

#include <util/generic/set.h>

#include <cstring>

namespace NCloud::NBlockStore::NStorage::NPartition2 {

////////////////////////////////////////////////////////////////////////////////

struct TFreshBlockCompare
{
    using is_transparent = void;

    bool operator ()(const TFreshBlock& l, const TFreshBlock& r) const
    {
        return TBlockCompare()(l.Meta, r.Meta);
    }

    bool operator ()(const TBlockKey& l, const TFreshBlock& r) const
    {
        return TBlockCompare()(l, r.Meta);
    }

    bool operator ()(const TFreshBlock& l, const TBlockKey& r) const
    {
        return TBlockCompare()(l.Meta, r);
    }
};

struct TBlockIndex::TImpl
{
    using TBlockMap = TSet<TFreshBlock, TFreshBlockCompare, TStlAllocator>;

    IAllocator* Allocator;
    TBlockMap Blocks;

    TImpl(IAllocator* allocator)
        : Allocator(allocator)
        , Blocks(allocator)
    {}
};

////////////////////////////////////////////////////////////////////////////////

TBlockIndex::TBlockIndex(IAllocator* allocator)
    : Impl(new TImpl(allocator))
{}

TBlockIndex::~TBlockIndex()
{
    for (const auto& block: Impl->Blocks) {
        if (block.Content) {
            IAllocator::TBlock alloc{
                const_cast<char*>(block.Content.data()),
                block.Content.size()
            };
            Impl->Allocator->Release(alloc);
        }
    }
}

bool TBlockIndex::AddBlock(
    ui32 blockIndex,
    TStringBuf blockContent,
    ui64 minCommitId,
    ui64 maxCommitId)
{
    TImpl::TBlockMap::iterator it;
    bool inserted;

    TStringBuf content;
    if (blockContent) {
        auto alloc = Impl->Allocator->Allocate(blockContent.size());
        std::memcpy(alloc.Data, blockContent.data(), blockContent.size());
        content = {static_cast<const char*>(alloc.Data), blockContent.size()};
    }

    std::tie(it, inserted) = Impl->Blocks.emplace(
        TBlock{
            blockIndex,
            minCommitId,
            maxCommitId,
            false,
        },
        content
    );

    return inserted;
}

bool TBlockIndex::RemoveBlock(ui32 blockIndex, ui64 minCommitId)
{
    auto it = Impl->Blocks.find(TBlockKey(blockIndex, minCommitId));
    if (it != Impl->Blocks.end()) {
        if (it->Content) {
            IAllocator::TBlock alloc{
                const_cast<char*>(it->Content.data()),
                it->Content.size()
            };
            Impl->Allocator->Release(alloc);
        }
        Impl->Blocks.erase(it);
        return true;
    }

    return false;
}

ui64 TBlockIndex::AddDeletedBlock(ui32 blockIndex, ui64 maxCommitId)
{
    auto it = Impl->Blocks.upper_bound(TBlockKey(blockIndex, maxCommitId));
    if (it != Impl->Blocks.end() && it->Meta.BlockIndex == blockIndex) {
        Y_ABORT_UNLESS(it->Meta.MinCommitId < maxCommitId);
        if (it->Meta.MaxCommitId > maxCommitId) {
            const_cast<TFreshBlock&>(*it).Meta.MaxCommitId = maxCommitId;
            return it->Meta.MinCommitId;
        }
    }

    return 0;
}

bool TBlockIndex::HasBlockAt(ui32 blockIndex) const
{
    return !!FindBlock(blockIndex);
}

const TFreshBlock* TBlockIndex::FindBlock(ui32 blockIndex) const
{
    auto it = Impl->Blocks.lower_bound(TBlockKey(blockIndex, InvalidCommitId));
    if (it != Impl->Blocks.end() && it->Meta.BlockIndex == blockIndex) {
        return &*it;
    }

    return nullptr;
}

const TFreshBlock* TBlockIndex::FindBlock(ui32 blockIndex, ui64 checkpointId) const
{
    auto it = Impl->Blocks.lower_bound(TBlockKey(blockIndex, checkpointId));
    if (it != Impl->Blocks.end() && it->Meta.BlockIndex == blockIndex) {
        Y_ABORT_UNLESS(it->Meta.MinCommitId <= checkpointId);
        if (it->Meta.MaxCommitId > checkpointId) {
            return &*it;
        }
    }

    return nullptr;
}

TVector<TFreshBlock> TBlockIndex::FindBlocks() const
{
    TVector<TFreshBlock> result(Reserve(Impl->Blocks.size()));

    for (const auto& block: Impl->Blocks) {
        result.push_back(block);
    }

    return result;
}

TVector<TFreshBlock> TBlockIndex::FindBlocks(ui64 checkpointId) const
{
    TVector<TFreshBlock> result(Reserve(Impl->Blocks.size()));

    for (const auto& block: Impl->Blocks) {
        if (block.Meta.MinCommitId <= checkpointId &&
            block.Meta.MaxCommitId > checkpointId)
        {
            result.push_back(block);
            // this MaxCommitId should not be seen at checkpointId
            // at least for consistency with TBlobIndex
            result.back().Meta.MaxCommitId = InvalidCommitId;
        }
    }

    return result;
}

TVector<TFreshBlock> TBlockIndex::FindBlocks(const TBlockRange32& blockRange) const
{
    TVector<TFreshBlock> result(Reserve(blockRange.Size()));

    auto start = Impl->Blocks.lower_bound(TBlockKey(blockRange.Start, InvalidCommitId));
    auto end = Impl->Blocks.upper_bound(TBlockKey(blockRange.End, 0));

    for (auto it = start; it != end; ++it) {
        result.push_back(*it);
    }

    return result;
}

TVector<TFreshBlock> TBlockIndex::FindBlocks(
    const TBlockRange32& blockRange,
    ui64 checkpointId) const
{
    TVector<TFreshBlock> result(Reserve(blockRange.Size()));

    auto start = Impl->Blocks.lower_bound(TBlockKey(blockRange.Start, InvalidCommitId));
    auto end = Impl->Blocks.upper_bound(TBlockKey(blockRange.End, 0));

    for (auto it = start; it != end; ++it) {
        if (it->Meta.MinCommitId <= checkpointId &&
            it->Meta.MaxCommitId > checkpointId)
        {
            result.push_back(*it);
            result.back().Meta.MaxCommitId = InvalidCommitId;
        }
    }

    return result;
}

size_t TBlockIndex::GetBlockCount() const
{
    return Impl->Blocks.size();
}

}   // namespace NCloud::NBlockStore::NStorage::NPartition2
