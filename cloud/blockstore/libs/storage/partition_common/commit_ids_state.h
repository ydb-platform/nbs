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
    NPartition::TCheckpointStore Checkpoints;
    NPartition::TCheckpointsInFlight CheckpointsInFlight;

public:
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
};

}   // namespace NCloud::NBlockStore::NStorage
