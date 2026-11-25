#include "part_mirror_resync_state.h"

#include "config.h"

#include <cloud/blockstore/libs/storage/core/config.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

////////////////////////////////////////////////////////////////////////////////

TMirrorPartitionResyncState::TMirrorPartitionResyncState(
        TStorageConfigPtr config,
        TString rwClientId,
        TNonreplicatedPartitionConfigPtr partConfig,
        TVector<TDevices> replicaDevices,
        ui64 initialResyncIndex)
    : Config(std::move(config))
    , RWClientId(std::move(rwClientId))
    , ProcessingBlocks(
        partConfig->GetBlockCount(),
        partConfig->GetBlockSize(),
        initialResyncIndex)
{
    ReplicaInfos.push_back(
        TReplicaInfo{.Config = partConfig->Fork(partConfig->GetDevices())});
    for (auto& devices: replicaDevices) {
        ReplicaInfos.push_back(
            TReplicaInfo{.Config = partConfig->Fork(std::move(devices))});
    }
}

////////////////////////////////////////////////////////////////////////////////

bool TMirrorPartitionResyncState::IsResynced(TBlockRange64 range) const
{
    return ProcessingBlocks.IsProcessing() &&
        ProcessingBlocks.IsProcessed(range);
}

void TMirrorPartitionResyncState::MarkResynced(TBlockRange64 range)
{
    ProcessingBlocks.MarkProcessed(range);
}

bool TMirrorPartitionResyncState::SkipResyncedRanges()
{
    return ProcessingBlocks.SkipProcessedRanges();
}

TBlockRange64 TMirrorPartitionResyncState::BuildResyncRange() const
{
    return ProcessingBlocks.BuildProcessingRange();
}

bool TMirrorPartitionResyncState::DevicesReadyForReading(
    ui32 replicaIndex,
    const TBlockRange64 blockRange) const
{
    const auto& replicaInfo = ReplicaInfos[replicaIndex];
    return replicaInfo.Config->DevicesReadyForReading(blockRange);
}

bool TMirrorPartitionResyncState::AddPendingResyncRange(ui32 rangeId)
{
    if (!ActiveResyncRangeSet.contains(rangeId)) {
        return PendingResyncRangeSet.insert(rangeId).second;
    }

    return false;
}

bool TMirrorPartitionResyncState::StartNextResyncRange(ui32* rangeId)
{
    if (PendingResyncRangeSet.empty()) {
        return false;
    }

    auto it = PendingResyncRangeSet.begin();
    *rangeId = *it;
    PendingResyncRangeSet.erase(it);
    ActiveResyncRangeSet.insert(*rangeId);
    return true;
}

void TMirrorPartitionResyncState::FinishResyncRange(ui32 rangeId)
{
    ActiveResyncRangeSet.erase(rangeId);
}

const TSet<ui32>& TMirrorPartitionResyncState::GetActiveResyncRangeSet() const
{
    return ActiveResyncRangeSet;
}

}   // namespace NCloud::NBlockStore::NStorage
