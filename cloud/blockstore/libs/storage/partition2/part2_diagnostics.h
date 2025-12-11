#pragma once

#include "public.h"

#include "part2_actor.h"

#include <cloud/blockstore/libs/diagnostics/profile_log.h>
#include <cloud/blockstore/libs/storage/partition2/model/block.h>

namespace NCloud::NBlockStore::NStorage::NPartition2 {

////////////////////////////////////////////////////////////////////////////////

class TDumpBlockCommitIds
{
private:
    const TVector<TBlock>& Blocks;
    TVector<IProfileLog::TBlockCommitId>& BlockCommitIds;

    const bool Enabled;

public:
    TDumpBlockCommitIds(
        const TVector<TBlock>& blocks,
        TVector<IProfileLog::TBlockCommitId>& blockCommitIds,
        bool enabled)
        : Blocks(blocks)
        , BlockCommitIds(blockCommitIds)
        , Enabled(enabled)
    {
        if (!Enabled) {
            return;
        }

        BlockCommitIds.reserve(BlockCommitIds.size() + Blocks.size());

        for (const auto& block: Blocks) {
            BlockCommitIds.push_back(
                {block.BlockIndex,
                 block.MinCommitId,
                 block.MaxCommitId,
                 0,   // minCommitIdNew (fill later)
                 0}   // maxCommitIdNew (fill later)
            );
        }
    }

    ~TDumpBlockCommitIds()
    {
        if (!Enabled) {
            return;
        }

        auto it = BlockCommitIds.rbegin();

        for (auto jt = Blocks.rbegin(); jt != Blocks.rend(); ++it, ++jt) {
            Y_ABORT_UNLESS(it != BlockCommitIds.rend());
            Y_ABORT_UNLESS(it->BlockIndex == jt->BlockIndex);

            it->MinCommitIdNew = jt->MinCommitId;
            it->MaxCommitIdNew = jt->MaxCommitId;
        }
    }
};

}   // namespace NCloud::NBlockStore::NStorage::NPartition2
