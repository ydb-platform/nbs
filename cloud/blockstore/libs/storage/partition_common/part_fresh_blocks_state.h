#pragma once

#include "part_thread_safe_state.h"

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
    NPartition::TOperationState& AccessFlushState()
    {
        return FlushState;
    }

    [[nodiscard]] const NPartition::TOperationState& GetFlushState() const
    {
        return FlushState;
    }

    [[nodiscard]] TRequestBuffer<TWriteBufferRequestData>& AccessWriteBuffer()
    {
        return WriteBuffer;
    }

    [[nodiscard]] const TRequestBuffer<TWriteBufferRequestData>&
    GetWriteBuffer() const
    {
        return WriteBuffer;
    }

    [[nodiscard]] ui32 GetFreshBlocksQueued() const
    {
        return WriteBuffer.GetWeight();
    }

    [[nodiscard]] ui32 GetFreshBlocksInFlight() const
    {
        return FreshBlocksInFlight;
    }

    [[nodiscard]] ui32 GetUnflushedFreshBlobCount() const
    {
        return UnflushedFreshBlobCount;
    }

    [[nodiscard]] ui32 GetUnflushedFreshBlobByteCount() const
    {
        return UnflushedFreshBlobByteCount;
    }

    void IncrementUnflushedFreshBlobCount(ui32 value);
    void DecrementUnflushedFreshBlobCount(ui32 value);
    void IncrementUnflushedFreshBlobByteCount(ui64 value);
    void DecrementUnflushedFreshBlobByteCount(ui64 value);

    ui32 IncrementFreshBlocksInFlight(size_t value);
    ui32 DecrementFreshBlocksInFlight(size_t value);

    [[nodiscard]] THashSet<ui64>& AccessFlushedCommitIdsInProgress()
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
    [[nodiscard]] ui64 GetUntrimmedFreshBlobByteCount() const
    {
        return UntrimmedFreshBlobByteCount;
    }

    void AddFreshBlob(TFreshBlobMeta freshBlobMeta);
    void TrimFreshBlobs(ui64 commitId);
};

class TPartitionTrimFreshLogState
{
private:
    NPartition::TOperationState TrimFreshLogState;
    ui64 LastTrimFreshLogToCommitId = 0;
    TBackoffDelayProvider TrimFreshLogBackoffDelayProvider{
        TDuration::Zero(),
        TDuration::MilliSeconds(100),
        TDuration::Seconds(5)};

public:
    [[nodiscard]] NPartition::TOperationState& AccessTrimFreshLogState()
    {
        return TrimFreshLogState;
    }

    [[nodiscard]] const NPartition::TOperationState& GetTrimFreshLogState() const
    {
        return TrimFreshLogState;
    }

    [[nodiscard]] TDuration GetTrimFreshLogBackoffDelay() const
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

    [[nodiscard]] ui64 GetLastTrimFreshLogToCommitId() const
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
    TPartitionThreadSafeState& ThreadSafeState;

    ui32 UnflushedFreshBlocksFromChannelCount = 0;

protected:
    NPartition::TBlockIndex Blocks;

public:
    TPartitionFreshBlocksState(
        const TCommitIdsState& commitIdsState,
        const TPartitionFlushState& flushState,
        TPartitionThreadSafeState& threadSafeState);

    void InitFreshBlocks(
        const TVector<NPartition::TOwningFreshBlock>& freshBlocks);

    void FindFreshBlocks(
        NPartition::IFreshBlocksIndexVisitor& visitor,
        const TBlockRange32& readRange,
        ui64 maxCommitId);

    void WriteFreshBlocks(
        const TBlockRange32& writeRange,
        ui64 commitId,
        TSgList sglist);

    void ZeroFreshBlocks(const TBlockRange32& zeroRange, ui64 commitId);

    void DeleteFreshBlock(ui32 blockIndex, ui64 commitId);

    ui32 IncrementUnflushedFreshBlocksFromChannelCount(size_t value);
    ui32 DecrementUnflushedFreshBlocksFromChannelCount(size_t value);

    [[nodiscard]] ui32 GetUnflushedFreshBlocksCountFromChannel() const
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
