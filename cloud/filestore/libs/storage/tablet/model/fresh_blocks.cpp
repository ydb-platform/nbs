#include "fresh_blocks.h"

#include <cloud/storage/core/libs/tablet/model/commit.h>

namespace NCloud::NFileStore::NStorage {

namespace {

////////////////////////////////////////////////////////////////////////////////

TBlock BlockKey(ui64 nodeId, ui32 blockIndex, ui64 commitId)
{
    return { nodeId, blockIndex, commitId, 0 };
}

TBlock BlockKey(ui64 nodeId, ui32 blockIndex)
{
    return { nodeId, blockIndex, InvalidCommitId, 0 };
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

bool TFreshBlocks::AddBlock(
    ui64 nodeId,
    ui32 blockIndex,
    TBlockDataHolder blockData,
    ui64 minCommitId,
    ui64 maxCommitId)
{
    auto [it, inserted] = Blocks.emplace(
        TBlock(nodeId, blockIndex, minCommitId, maxCommitId),
        std::move(blockData));

    return inserted;
}

bool TFreshBlocks::AddBlock(
    ui64 nodeId,
    ui32 blockIndex,
    TStringBuf blockData,
    ui32 blockSize,
    ui64 minCommitId,
    ui64 maxCommitId)
{
    Y_ABORT_UNLESS(blockSize >= blockData.size());
    TString buffer;
    buffer.reserve(blockSize);
    buffer.append(blockData);
    buffer.resize(blockSize, 0);
    auto blockBuffer = CreateBlockBuffer(
        {blockIndex * blockSize, blockSize, blockSize},
        std::move(buffer));
    return AddBlock(
        nodeId,
        blockIndex,
        {0, std::move(blockBuffer)},
        minCommitId,
        maxCommitId);
}

ui64 TFreshBlocks::MarkBlockDeleted(ui64 nodeId, ui32 blockIndex, ui64 commitId)
{
    auto it = Blocks.lower_bound(BlockKey(nodeId, blockIndex, commitId));
    if (it != Blocks.end()) {
        auto& block = const_cast<TBlock&>(it->first);

        if (block.NodeId == nodeId && block.BlockIndex == blockIndex) {
            Y_ABORT_UNLESS(block.MinCommitId <= commitId);
            if (block.MaxCommitId == InvalidCommitId) {
                block.MaxCommitId = commitId;
                return block.MinCommitId;
            }
        }
    }

    return InvalidCommitId;
}

TVector<TFreshBlocks::TVersionedBlock> TFreshBlocks::MarkBlocksDeleted(
    ui64 nodeId,
    ui32 blockIndex,
    ui32 blocksCount,
    ui64 commitId)
{
    TVector<TFreshBlocks::TVersionedBlock> result;

    auto start = Blocks.lower_bound(BlockKey(nodeId, blockIndex));
    auto end = Blocks.lower_bound(BlockKey(nodeId, blockIndex + blocksCount));

    for (auto it = start; it != end; ++it) {
        auto& block = const_cast<TBlock&>(it->first);

        if (block.MinCommitId <= commitId &&
            block.MaxCommitId == InvalidCommitId)
        {
            block.MaxCommitId = commitId;
            result.emplace_back(block.BlockIndex, block.MinCommitId);
        }
    }

    return result;
}

bool TFreshBlocks::RemoveBlock(ui64 nodeId, ui32 blockIndex, ui64 commitId)
{
    auto it = Blocks.find(BlockKey(nodeId, blockIndex, commitId));
    if (it != Blocks.end()) {
        Blocks.erase(it);
        return true;
    }

    return false;
}

bool TFreshBlocks::FindBlock(ui64 nodeId, ui32 blockIndex) const
{
    auto it = Blocks.lower_bound(BlockKey(nodeId, blockIndex, InvalidCommitId));
    if (it != Blocks.end()) {
        const auto& block = it->first;

        if (block.NodeId == nodeId && block.BlockIndex == blockIndex) {
            return true;
        }
    }

    return false;
}

TMaybe<TFreshBlock> TFreshBlocks::FindBlock(
    ui64 nodeId,
    ui32 blockIndex,
    ui64 commitId) const
{
    auto it = Blocks.lower_bound(BlockKey(nodeId, blockIndex, commitId));
    if (it != Blocks.end()) {
        const auto& block = it->first;

        if (block.NodeId == nodeId && block.BlockIndex == blockIndex) {
            Y_ABORT_UNLESS(block.MinCommitId <= commitId);
            if (block.MaxCommitId > commitId) {
                return TFreshBlock{block, it->second.GetBlockData()};
            }
        }
    }

    return {};
}

void TFreshBlocks::FindBlocks(IFreshBlockVisitor& visitor) const
{
    for (const auto& [block, freshBlock]: Blocks) {
        visitor.Accept(block, freshBlock);
    }
}

void TFreshBlocks::FindBlocks(IFreshBlockVisitor& visitor, ui64 commitId) const
{
    for (const auto& [block, freshBlock]: Blocks) {
        if (VisibleCommitId(commitId, block.MinCommitId, block.MaxCommitId)) {
            visitor.Accept(block, freshBlock);
        }
    }
}

void TFreshBlocks::FindBlocks(
    IFreshBlockVisitor& visitor,
    ui64 nodeId,
    ui32 blockIndex,
    ui32 blocksCount) const
{
    auto start = Blocks.lower_bound(BlockKey(nodeId, blockIndex));
    auto end = Blocks.lower_bound(BlockKey(nodeId, blockIndex + blocksCount));

    for (auto it = start; it != end; ++it) {
        visitor.Accept(it->first, it->second);
    }
}

void TFreshBlocks::FindBlocks(
    IFreshBlockVisitor& visitor,
    ui64 nodeId,
    ui32 blockIndex,
    ui32 blocksCount,
    ui64 commitId) const
{
    auto start = Blocks.lower_bound(BlockKey(nodeId, blockIndex));
    auto end = Blocks.lower_bound(BlockKey(nodeId, blockIndex + blocksCount));

    for (auto it = start; it != end; ++it) {
        const auto& block = it->first;

        if (VisibleCommitId(commitId, block.MinCommitId, block.MaxCommitId)) {
            visitor.Accept(block, it->second);
        }
    }
}

}   // namespace NCloud::NFileStore::NStorage
