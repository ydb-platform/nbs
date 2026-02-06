#pragma once

#include <cloud/blockstore/libs/storage/partition/model/commit_queue.h>
#include <cloud/blockstore/libs/storage/partition/model/checkpoint.h>

#include <cloud/storage/core/libs/tablet/model/commit.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

class TCommitIdsState
{
private:
    const ui64 Generation = 0;
    NPartition::TCommitQueue CommitQueue;
    ui32 LastCommitId = 0;

    NPartition::TCheckpointStore Checkpoints;

public:
    TCommitIdsState(ui64 generation, ui64 lastCommitId);

    [[nodiscard]] const NPartition::TCommitQueue& GetCommitQueue() const
    {
        return CommitQueue;
    }

    [[nodiscard]] NPartition::TCommitQueue& AccessCommitQueue()
    {
        return CommitQueue;
    }


    [[nodiscard]] ui64 GetLastCommitId() const
    {
        return MakeCommitId(Generation, LastCommitId);
    }

    ui64 GenerateCommitId()
    {
        if (LastCommitId == Max<ui32>()) {
            return InvalidCommitId;
        }
        return MakeCommitId(Generation, ++LastCommitId);
    }

    [[nodiscard]] auto& AccessCheckpoints()
    {
        return Checkpoints;
    }

    [[nodiscard]] const auto& GetCheckpoints() const
    {
        return Checkpoints;
    }

    void GetCheckpointCommitIds(TVector<ui64>& result) const
    {
        Checkpoints.GetCommitIds(result);
    }
};

}   // namespace NCloud::NBlockStore::NStorage
