#pragma once

#include <cloud/blockstore/libs/storage/core/write_buffer_request.h>
#include <cloud/blockstore/libs/storage/partition/model/barrier.h>
#include <cloud/blockstore/libs/storage/partition/model/block_index.h>
#include <cloud/blockstore/libs/storage/partition/model/checkpoint.h>
#include <cloud/blockstore/libs/storage/partition/model/operation_status.h>
#include <cloud/blockstore/libs/storage/partition_common/commit_ids_state.h>

#include <cloud/storage/core/libs/common/backoff_delay_provider.h>
#include <cloud/storage/core/libs/tablet/gc_logic.h>

#include <util/generic/set.h>
#include <util/system/types.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

class TPartitionFlushState
{
private:
    NPartition::TOperationState FlushState;
    TRequestBuffer<TWriteBufferRequestData> WriteBuffer;
    ui32 FreshBlocksInFlight = 0;
    ui32 UnflushedFreshBlobCount = 0;
    ui64 UnflushedFreshBlobByteCount = 0;

    THashSet<ui64> FlushedCommitIdsInProgress;

public:
    NPartition::TOperationState& GetFlushState()
    {
        return FlushState;
    }

    [[nodiscard]] const NPartition::TOperationState& GetFlushState() const
    {
        return FlushState;
    }

    TRequestBuffer<TWriteBufferRequestData>& GetWriteBuffer()
    {
        return WriteBuffer;
    }

    ui32 GetFreshBlocksQueued() const
    {
        return WriteBuffer.GetWeight();
    }

    ui32 GetFreshBlocksInFlight() const
    {
        return FreshBlocksInFlight;
    }

    ui32 GetUnflushedFreshBlobCount() const
    {
        return UnflushedFreshBlobCount;
    }

    ui32 GetUnflushedFreshBlobByteCount() const
    {
        return UnflushedFreshBlobByteCount;
    }

    void IncrementUnflushedFreshBlobCount(ui32 value);
    void DecrementUnflushedFreshBlobCount(ui32 value);
    void IncrementUnflushedFreshBlobByteCount(ui64 value);
    void DecrementUnflushedFreshBlobByteCount(ui64 value);

    ui32 IncrementFreshBlocksInFlight(size_t value);
    ui32 DecrementFreshBlocksInFlight(size_t value);

    THashSet<ui64>& GetFlushedCommitIdsInProgress()
    {
        return FlushedCommitIdsInProgress;
    }

    [[nodiscard]] const THashSet<ui64>& GetFlushedCommitIdsInProgress() const
    {
        return FlushedCommitIdsInProgress;
    }
};

class TPartitionFreshBlobState
{
public:
    struct TFreshBlobMeta
    {
        const ui64 CommitId;
        const ui64 BlobSize;

        bool operator<(const TFreshBlobMeta& other) const
        {
            return CommitId < other.CommitId;
        }
    };

private:
    ui64 UntrimmedFreshBlobByteCount = 0;
    TSet<TFreshBlobMeta> UntrimmedFreshBlobs;

public:
    ui64 GetUntrimmedFreshBlobByteCount() const
    {
        return UntrimmedFreshBlobByteCount;
    }

    void AddFreshBlob(TFreshBlobMeta freshBlobMeta);
    void TrimFreshBlobs(ui64 commitId);
};

class TPartitionTrimFreshLogState
{
private:
    const TCommitIdsState& CommitIdsState;
    NPartition::TBarriers TrimFreshLogBarriers;
    NPartition::TOperationState TrimFreshLogState;
    ui64 LastTrimFreshLogToCommitId = 0;
    TBackoffDelayProvider TrimFreshLogBackoffDelayProvider{
        TDuration::Zero(),
        TDuration::MilliSeconds(100),
        TDuration::Seconds(5)};

public:
    explicit TPartitionTrimFreshLogState(const TCommitIdsState& commitIdsState)
        : CommitIdsState(commitIdsState)
    {}

    NPartition::TBarriers& GetTrimFreshLogBarriers()
    {
        return TrimFreshLogBarriers;
    }

    ui64 GetTrimFreshLogToCommitId() const
    {
        return Min(
            // if there are no fresh blocks, we should trim up to current
            // commitId
            CommitIdsState.GetLastCommitId(),
            // if there are some fresh blocks, we should trim till the lowest
            // fresh commitId minus 1
            TrimFreshLogBarriers.GetMinCommitId() - 1);
    }

    NPartition::TOperationState& GetTrimFreshLogState()
    {
        return TrimFreshLogState;
    }

    TDuration GetTrimFreshLogBackoffDelay()
    {
        return TrimFreshLogBackoffDelayProvider.GetDelay();
    }

    void RegisterTrimFreshLogError()
    {
        TrimFreshLogBackoffDelayProvider.IncreaseDelay();
    }

    void RegisterTrimFreshLogSuccess()
    {
        TrimFreshLogBackoffDelayProvider.Reset();
    }

    ui64 GetLastTrimFreshLogToCommitId() const
    {
        return LastTrimFreshLogToCommitId;
    }

    void SetLastTrimFreshLogToCommitId(ui64 commitId)
    {
        LastTrimFreshLogToCommitId = commitId;
    }
};

class TPartitionFreshBlocksState
{
private:
    const TCommitIdsState& CommitIdsState;
    const TPartitionFlushState& FlushState;
    TPartitionTrimFreshLogState& TrimFreshLogState;
    ui32 UnflushedFreshBlocksFromChannelCount = 0;

protected:
    NPartition::TBlockIndex Blocks;

public:
    TPartitionFreshBlocksState(
        const TCommitIdsState& commitIdsState,
        const TPartitionFlushState& flushState,
        TPartitionTrimFreshLogState& trimFreshLogState);

    void InitFreshBlocks(
        const TVector<NPartition::TOwningFreshBlock>& freshBlocks);

    void FindFreshBlocks(
        NPartition::IFreshBlocksIndexVisitor& visitor,
        const TBlockRange32& readRange,
        ui64 maxCommitId = Max());

    void WriteFreshBlocks(
        const TBlockRange32& writeRange,
        ui64 commitId,
        TSgList sglist);

    void ZeroFreshBlocks(const TBlockRange32& zeroRange, ui64 commitId);

    void DeleteFreshBlock(ui32 blockIndex, ui64 commitId);

    ui32 IncrementUnflushedFreshBlocksFromChannelCount(size_t value);
    ui32 DecrementUnflushedFreshBlocksFromChannelCount(size_t value);

    ui32 GetUnflushedFreshBlocksCountFromChannel() const
    {
        return UnflushedFreshBlocksFromChannelCount;
    }

private:
    void WriteFreshBlocksImpl(
        const TBlockRange32& writeRange,
        ui64 commitId,
        auto getBlockContent);
};

}   // namespace NCloud::NBlockStore::NStorage
