#pragma once

#include "part_thread_safe_state.h"

#include <cloud/blockstore/libs/storage/core/write_buffer_request.h>
#include <cloud/blockstore/libs/storage/partition_common/commit_ids_state.h>
#include <cloud/blockstore/libs/storage/partition_common/model/barrier.h>
#include <cloud/blockstore/libs/storage/partition_common/model/block_index.h>
#include <cloud/blockstore/libs/storage/partition_common/model/checkpoint.h>
#include <cloud/blockstore/libs/storage/partition_common/model/operation_status.h>

#include <cloud/storage/core/libs/common/backoff_delay_provider.h>
#include <cloud/storage/core/libs/tablet/gc_logic.h>

#include <util/datetime/base.h>
#include <util/generic/set.h>
#include <util/system/types.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

class TFlushOperationState
{
private:
    TOperationState OperationState;
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
        if (OperationState.Status != EOperationStatus::Enqueued &&
            OperationState.Status != EOperationStatus::Idle)
        {
            return false;
        }
        FlushCommitId = flushCommitId;
        RequestInfo = std::move(requestInfo);
        OperationState.SetStatus(EOperationStatus::Started, timestamp);

        return true;
    }

    [[nodiscard]] bool SetEnqueued(TInstant timestamp)
    {
        if (OperationState.Status != EOperationStatus::Idle) {
            return false;
        }

        OperationState.SetStatus(EOperationStatus::Enqueued, timestamp);

        return true;
    }

    void SetIdle(TInstant timestamp)
    {
        FlushCommitId = 0;
        RequestInfo = nullptr;
        OperationState.SetStatus(EOperationStatus::Idle, timestamp);
    }

    [[nodiscard]] const TOperationState& GetOperationState() const
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
    struct TUntrimmedFreshBlob
    {
        ui64 ByteCount = 0;
        // The last trim barrier reference to the blob was released.
        bool Dead = false;
    };

    struct TUnflushedFreshBlob
    {
        ui64 ByteCount = 0;
        TInstant Timestamp;
    };

    ui64 TabletID = 0;
    ui32 BlockSize = 0;

    ui64 UntrimmedFreshBlobByteCount = 0;
    TMap<ui64, TUntrimmedFreshBlob> UntrimmedFreshBlobsByCommitId;

    // The dead byte counts below weigh a blob at least one block, so that
    // many small dead blobs count as much as they cost.

    // Dead untrimmed blobs.
    ui64 DeadFreshBlobByteCount = 0;
    // Untrimmed blobs at or below OrdinaryTrimToCommitId, the last value passed
    // to GetOrdinaryTrimDeadFreshBlobByteCount. They are all dead, because that
    // value is below every referenced commit.
    ui64 OrdinaryTrimToCommitId = 0;
    ui64 OrdinaryTrimDeadFreshBlobByteCount = 0;

    ui32 UnflushedFreshBlobCount = 0;
    ui64 UnflushedFreshBlobByteCount = 0;
    TMap<ui64, TUnflushedFreshBlob> UnflushedFreshBlobsByCommitId;

public:
    TPartitionFreshBlobState(ui64 tabletID, ui32 blockSize)
        : TabletID(tabletID)
        , BlockSize(blockSize)
    {}

    [[nodiscard]] ui64 GetUntrimmedFreshBlobByteCount() const
    {
        return UntrimmedFreshBlobByteCount;
    }

    [[nodiscard]] ui64 GetDeadFreshBlobByteCount() const
    {
        return DeadFreshBlobByteCount;
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

    // Returns the timestamp of the first unflushed blob after the trim
    // boundary.
    [[nodiscard]] TInstant GetFirstUnflushedFreshBlobTimestamp(
        ui64 trimFreshLogToCommitId) const;

    void AddFreshBlob(ui64 commitId, ui64 blobSize, TInstant timestamp);

    // Called when the last trim barrier reference to an untrimmed blob is
    // released.
    void MarkFreshBlobDead(ui64 commitId);

    // Returns the dead bytes of the untrimmed blobs at or below
    // ordinaryTrimToCommitId, which must not decrease between calls.
    [[nodiscard]] ui64 GetOrdinaryTrimDeadFreshBlobByteCount(
        ui64 ordinaryTrimToCommitId);

    // For debug checks: true when the dead byte counts match the referenced
    // commits. liveCommitIds: every referenced commit, ascending.
    [[nodiscard]] bool CheckDeadFreshBlobByteCount(
        const TVector<ui64>& liveCommitIds) const;

    // Trims every blob at or below commitId. Returns the unflushed bytes
    // removed.
    [[nodiscard]] ui64 TrimFreshBlobs(ui64 commitId);

    // A missing blob must be at or below trimmedToCommitId: a trim that
    // completed during the flush removed it. Returns 0 then.
    ui64 FlushFreshBlob(ui64 commitId, ui64 trimmedToCommitId);

private:
    [[nodiscard]] ui64 GetDeadByteCount(const TUntrimmedFreshBlob& blob) const;

    void EraseUntrimmedFreshBlob(
        TMap<ui64, TUntrimmedFreshBlob>::iterator it);

    ui64 EraseUnflushedFreshBlob(
        TMap<ui64, TUnflushedFreshBlob>::iterator it);
};

class TPartitionTrimFreshLogState
{
private:
    TOperationState TrimFreshLogState;
    ui64 LastTrimFreshLogToCommitId = 0;
    TBackoffDelayProvider TrimFreshLogBackoffDelayProvider{
        TDuration::Zero(),
        TDuration::MilliSeconds(100),
        TDuration::Seconds(5)};

    // A flush requested an ordinary trim, or the last one failed. Cleared when
    // an ordinary trim starts.
    bool OrdinaryTrimRequested = false;

public:
    [[nodiscard]] TOperationState& AccessTrimFreshLogState()
    {
        return TrimFreshLogState;
    }

    [[nodiscard]] const TOperationState& GetTrimFreshLogState() const
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

    [[nodiscard]] bool IsOrdinaryTrimRequested() const
    {
        return OrdinaryTrimRequested;
    }

    void SetOrdinaryTrimRequested(bool requested)
    {
        OrdinaryTrimRequested = requested;
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
    TBlockIndex Blocks;

public:
    TPartitionFreshBlocksState(
        const TCommitIdsState& commitIdsState,
        const TPartitionFlushState& flushState,
        TPartitionThreadSafeStatePtr threadSafeState);

    void InitFreshBlocks(const TVector<TOwningFreshBlock>& freshBlocks);

    void FindFreshBlocks(
        IFreshBlocksIndexVisitor& visitor,
        const TBlockRange32& readRange,
        ui64 maxCommitId);

    // deadCommitIds receives the fresh blobs whose last trim barrier
    // reference was released by the write.
    void WriteFreshBlocks(
        const TBlockRange32& writeRange,
        ui64 commitId,
        TSgList sglist,
        TPartialBlobId blobId,
        ui64& removedBlocksCount,
        TVector<ui64>& deadCommitIds);

    void ZeroFreshBlocks(
        const TBlockRange32& zeroRange,
        ui64 commitId,
        ui64& removedBlocksCount,
        TVector<ui64>& deadCommitIds);

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
        TPartialBlobId blobId,
        ui64& removedBlocksCount,
        TVector<ui64>& deadCommitIds);
};

}   // namespace NCloud::NBlockStore::NStorage
