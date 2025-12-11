#include "block.h"

#include <util/stream/output.h>
#include <util/system/yassert.h>

namespace NCloud::NBlockStore::NStorage::NPartition2 {

////////////////////////////////////////////////////////////////////////////////

IOutputStream& operator<<(IOutputStream& out, const TBlock& block)
{
    return out << "{" << "BlockIndex: " << block.BlockIndex << ", "
               << "MinCommitId: " << block.MinCommitId << ", "
               << "MaxCommitId: " << block.MaxCommitId << ", "
               << "Zeroed: " << block.Zeroed << "}";
}

////////////////////////////////////////////////////////////////////////////////

bool IsContinuousBlockRange(const TVector<TBlock>& blocks)
{
    Y_ABORT_UNLESS(blocks);
    ui32 prev = blocks[0].BlockIndex;
    for (size_t i = 1; i < blocks.size(); ++i) {
        Y_DEBUG_ABORT_UNLESS(prev <= blocks[i].BlockIndex);
        if (blocks[i].BlockIndex - prev > 1) {
            return false;
        }
        prev = blocks[i].BlockIndex;
    }
    return true;
}

TVector<ui32> BuildBlocks(const TVector<TBlock>& blocks)
{
    TVector<ui32> result(Reserve(blocks.size()));
    for (const auto& block: blocks) {
        result.push_back(block.BlockIndex);
    }
    return result;
}

}   // namespace NCloud::NBlockStore::NStorage::NPartition2
