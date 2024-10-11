#include "block_index.h"

#include <cloud/storage/core/libs/common/alloc.h>

#include <util/generic/set.h>

#include <cstring>

namespace NCloud::NBlockStore::NStorage::NPartition {

namespace {

////////////////////////////////////////////////////////////////////////////////

TBlock MakeKey(ui32 blockIndex, ui64 commitId)
{
    return TBlock(blockIndex, commitId, false);
}

}  // namespace

////////////////////////////////////////////////////////////////////////////////

struct TFreshBlockCompare
{
    using is_transparent = void;

    bool operator ()(const TFreshBlock& l, const TFreshBlock& r) const
    {
        return CompareBlocks(l.Meta, r.Meta);
    }

    bool operator ()(const TBlock& l, const TFreshBlock& r) const
    {
        return CompareBlocks(l, r.Meta);
    }

    bool operator ()(const TFreshBlock& l, const TBlock& r) const
    {
        return CompareBlocks(l.Meta, r);
    }

    bool CompareBlocks(const TBlock l, const TBlock r) const
    {
        // order by (BlockIndex ASC, MinCommitId DESC)
        if (l.BlockIndex == r.BlockIndex) {
            return l.CommitId > r.CommitId;
        }

        return l.BlockIndex < r.BlockIndex;
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
    ui64 commitId,
    bool isStoredInDb,
    TStringBuf blockContent)
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
        TBlock{blockIndex, commitId, isStoredInDb},
        content
    );

    if (!inserted && content) {
        IAllocator::TBlock alloc{
            const_cast<char*>(content.data()),
            content.size()
        };
        Impl->Allocator->Release(alloc);
    }

    return inserted;
}

bool TBlockIndex::RemoveBlock(ui32 blockIndex, ui64 commitId, bool isStoredInDb)
{
    auto it = Impl->Blocks.find(MakeKey(blockIndex, commitId));
    if (it != Impl->Blocks.end()) {
        if (it->Meta.IsStoredInDb != isStoredInDb) {
            return false;
        }

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

void TBlockIndex::FindBlocks(
    IFreshBlocksIndexVisitor& visitor,
    const TBlockRange32& blockRange,
    ui64 checkpointId) const
{
    auto start = Impl->Blocks.lower_bound(MakeKey(blockRange.Start, checkpointId));
    auto end = Impl->Blocks.upper_bound(MakeKey(blockRange.End, 0));

    for (auto it = start; it != end; ++it) {
        if (it->Meta.CommitId <= checkpointId) {
            if (!visitor.Visit(*it)) {
                break;
            }
        }
    }
}

void TBlockIndex::GetCommitIds(ui32 blockIndex, TVector<ui64>& commitIds)
{
    auto start = Impl->Blocks.lower_bound(MakeKey(blockIndex, Max()));
    auto end = Impl->Blocks.upper_bound(MakeKey(blockIndex, 0));

    for (auto it = start; it != end; ++it) {
        commitIds.push_back(it->Meta.CommitId);
    }
}

}   // namespace NCloud::NBlockStore::NStorage::NPartition
