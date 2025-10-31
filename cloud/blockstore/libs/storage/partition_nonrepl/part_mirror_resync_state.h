#pragma once

#include "public.h"

#include "config.h"
#include "replica_info.h"

#include <cloud/blockstore/libs/common/block_range.h>
#include <cloud/blockstore/libs/storage/core/public.h>
#include <cloud/blockstore/libs/storage/partition_nonrepl/model/processing_blocks.h>

#include <ydb/library/actors/core/actorid.h>

#include <util/generic/deque.h>
#include <util/generic/set.h>
#include <util/generic/vector.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

class TMirrorPartitionResyncState
{
private:
    const TStorageConfigPtr Config;
    TString RWClientId;
    TVector<TReplicaInfo> ReplicaInfos;

    TProcessingBlocks ProcessingBlocks;
    TSet<ui32> PendingResyncRangeSet;
    TSet<ui32> ActiveResyncRangeSet;

public:
    TMirrorPartitionResyncState(
        TStorageConfigPtr config,
        TString rwClientId,
        TNonreplicatedPartitionConfigPtr partConfig,
        TVector<TDevices> replicaDevices,
        ui64 initialResyncIndex);

public:
    bool IsResynced(TBlockRange64 range) const;
    void MarkResynced(TBlockRange64 range);
    bool SkipResyncedRanges();
    TBlockRange64 BuildResyncRange() const;

    void SetRWClientId(TString rwClientId)
    {
        RWClientId = std::move(rwClientId);
    }
    const TString& GetRWClientId() const
    {
        return RWClientId;
    }

    std::optional<ui64> GetLastReportedResyncIndex() const
    {
        return ProcessingBlocks.GetLastReportedProcessingIndex();
    }

    void SetLastReportedResyncIndex(ui64 i)
    {
        ProcessingBlocks.SetLastReportedProcessingIndex(i);
    }

    const TVector<TReplicaInfo>& GetReplicaInfos() const
    {
        return ReplicaInfos;
    }

    [[nodiscard]] bool DevicesReadyForReading(
        ui32 replicaIndex,
        const TBlockRange64 blockRange) const;

    bool AddPendingResyncRange(ui32 rangeId);
    bool StartNextResyncRange(ui32* rangeId);
    void FinishResyncRange(ui32 rangeId);
    const TSet<ui32>& GetActiveResyncRangeSet() const;
};

}   // namespace NCloud::NBlockStore::NStorage
