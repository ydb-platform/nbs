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

class TFlushOperationState
{
private:
    NPartition::TOperationState OperationState;
    ui64 FlushCommitId = 0;
    TRequestInfoPtr RequestInfo;

public:
    ui64 GetFlushCommitId() const
    {
        return FlushCommitId;
    }

    TRequestInfoPtr GetRequestInfo() const
    {
        return RequestInfo;
    }

    [[nodiscard]] bool SetStarted(
        ui64 flushCommitId,
        TRequestInfoPtr requestInfo,
        TInstant timestamp)
    {
        if (OperationState.Status != NPartition::EOperationStatus::Enqueued &&
            OperationState.Status != NPartition::EOperationStatus::Idle)
        {
            return false;
        }
        FlushCommitId = flushCommitId;
        RequestInfo = std::move(requestInfo);
        OperationState.SetStatus(
            NPartition::EOperationStatus::Started,
            timestamp);

        return true;
    }

    [[nodiscard]] bool SetEnqueued(TInstant timestamp)
    {
        if (OperationState.Status != NPartition::EOperationStatus::Idle) {
            return false;
        }

        OperationState.SetStatus(
            NPartition::EOperationStatus::Enqueued,
            timestamp);

        return true;
    }

    void SetIdle(TInstant timestamp)
    {
        FlushCommitId = 0;
        RequestInfo = nullptr;
        OperationState.SetStatus(NPartition::EOperationStatus::Idle, timestamp);
    }

    [[nodiscard]] const NPartition::TOperationState& GetOperationState() const
    {
        return OperationState;
    }
};

class TPartitionFlushState
{
private:
    TFlushOperationState FlushState;
    TRequestBuffer<TWriteBufferRequestData> WriteBuffer;

    THashSet<ui64> FlushedCommitIdsInProgress;

public:
    TFlushOperationState& AccessFlushState()
    {
        return FlushState;
    }

    [[nodiscard]] const TFlushOperationState& GetFlushState() const
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
private:
    ui64 TabletID = 0;

    ui64 UntrimmedFreshBlobByteCount = 0;
    TMap<ui64, ui64> UntrimmedFreshBlobByteCountByCommitId;

    ui32 UnflushedFreshBlobCount = 0;
    ui64 UnflushedFreshBlobByteCount = 0;
    TMap<ui64, ui64> UnflushedFreshBlobByteCountByCommitId;

public:
    explicit TPartitionFreshBlobState(ui64 tabletID)
        : TabletID(tabletID)
    {}

    [[nodiscard]] ui64 GetUntrimmedFreshBlobByteCount() const
    {
        return UntrimmedFreshBlobByteCount;
    }

    [[nodiscard]] ui64 GetUnflushedFreshBlobCount() const
    {
        return UnflushedFreshBlobCount;
    }

    [[nodiscard]] ui64 GetUnflushedFreshBlobByteCount() const
    {
        return UnflushedFreshBlobByteCount;
    }

    [[nodiscard]] TVector<ui64> GetUnflushedFreshBlobCommitIds(
        ui64 commitId) const;

    void AddFreshBlob(ui64 commitId, ui64 blobSize);
    void TrimFreshBlobs(ui64 commitId);
    ui64 FlushFreshBlob(ui64 commitId);
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

    [[nodiscard]] const NPartition::TOperationState&
    GetTrimFreshLogState() const
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
    TPartitionThreadSafeStatePtr ThreadSafeState;

    ui32 UnflushedFreshBlocksFromChannelCount = 0;

protected:
    NPartition::TBlockIndex Blocks;

public:
    TPartitionFreshBlocksState(
        const TCommitIdsState& commitIdsState,
        const TPartitionFlushState& flushState,
        TPartitionThreadSafeStatePtr threadSafeState);

    void InitFreshBlocks(
        const TVector<NPartition::TOwningFreshBlock>& freshBlocks);

    void FindFreshBlocks(
        NPartition::IFreshBlocksIndexVisitor& visitor,
        const TBlockRange32& readRange,
        ui64 maxCommitId);

    void WriteFreshBlocks(
        const TBlockRange32& writeRange,
        ui64 commitId,
        TSgList sglist,
        TPartialBlobId blobId);

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
        auto getBlockContent,
        TPartialBlobId blobId);
};

}   // namespace NCloud::NBlockStore::NStorage
