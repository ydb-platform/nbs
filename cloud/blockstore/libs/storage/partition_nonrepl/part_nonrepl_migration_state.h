#pragma once

#include "public.h"

#include <cloud/blockstore/libs/common/block_range.h>
#include <cloud/blockstore/libs/storage/core/public.h>
#include <cloud/blockstore/libs/storage/partition_nonrepl/model/processing_blocks.h>

#include <library/cpp/actors/core/actorid.h>

#include <util/generic/vector.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

class TNonreplicatedPartitionMigrationState
{
private:
    const TStorageConfigPtr Config;
    TString RWClientId;

    TNonreplicatedPartitionConfigPtr SrcPartitionActorConfig;
    TNonreplicatedPartitionConfigPtr DstPartitionActorConfig;

    TProcessingBlocks ProcessingBlocks;

public:
    TNonreplicatedPartitionMigrationState(
        TStorageConfigPtr config,
        ui64 initialMigrationIndex,
        TString rwClientId,
        TNonreplicatedPartitionConfigPtr srcPartitionActorConfig);

public:
    void SetupDstPartition(TNonreplicatedPartitionConfigPtr config);
    void AbortMigration();
    bool IsMigrationStarted() const;
    void MarkMigrated(TBlockRange64 range);
    bool SkipMigratedRanges();
    bool AdvanceMigrationIndex();
    TBlockRange64 BuildMigrationRange() const;
    ui64 GetMigratedBlockCount() const;
    TDuration CalculateMigrationTimeout(
        ui32 maxMigrationBandwidthMiBs,
        ui32 expectedDiskAgentSize) const;
    void SetRWClientId(TString rwClientId)
    {
        RWClientId = std::move(rwClientId);
    }
    const TString& GetRWClientId() const
    {
        return RWClientId;
    }

    ui64 GetLastReportedMigrationIndex() const
    {
        return ProcessingBlocks.GetLastReportedProcessingIndex();
    }

    void SetLastReportedMigrationIndex(ui64 i)
    {
        ProcessingBlocks.SetLastReportedProcessingIndex(i);
    }
};

}   // namespace NCloud::NBlockStore::NStorage
