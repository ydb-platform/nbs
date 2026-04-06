#pragma once

#include <cloud/blockstore/libs/storage/partition/model/checkpoint.h>
#include <cloud/blockstore/libs/storage/partition/model/commit_queue.h>
#include <cloud/blockstore/libs/storage/partition_common/model/commit_id_generator.h>

#include <cloud/storage/core/libs/tablet/model/commit.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

class TCommitIdsState
{
private:
    NPartition::TCommitQueue CommitQueue;
    TCommitIdGeneratorPtr CommitIdGenerator;

    NPartition::TCheckpointStore Checkpoints;
    NPartition::TCheckpointsInFlight CheckpointsInFlight;

public:
    explicit TCommitIdsState(TCommitIdGeneratorPtr generator);

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
        return CommitIdGenerator->GetLastCommitId();
    }

    ui64 GenerateCommitId()
    {
        return CommitIdGenerator->GenerateCommitId();
    }

    [[nodiscard]] auto& AccessCheckpoints()
    {
        return Checkpoints;
    }

    [[nodiscard]] const auto& GetCheckpoints() const
    {
        return Checkpoints;
    }

    [[nodiscard]] auto& AccessCheckpointsInFlight()
    {
        return CheckpointsInFlight;
    }

    [[nodiscard]] const auto& GetCheckpointsInFlight() const
    {
        return CheckpointsInFlight;
    }

    void GetCheckpointCommitIds(TVector<ui64>& result) const
    {
        Checkpoints.GetCommitIds(result);
    }

    [[nodiscard]] auto GetCommitIdGenerator() const
    {
        return CommitIdGenerator;
    }
};

}   // namespace NCloud::NBlockStore::NStorage
