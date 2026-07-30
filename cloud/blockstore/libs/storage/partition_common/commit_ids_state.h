#pragma once

#include <cloud/blockstore/libs/storage/partition_common/model/checkpoint.h>
#include <cloud/blockstore/libs/storage/partition_common/model/commit_queue.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

class TCommitIdsState
{
private:
    TCheckpointStore Checkpoints;
    TCheckpointsInFlight CheckpointsInFlight;

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
