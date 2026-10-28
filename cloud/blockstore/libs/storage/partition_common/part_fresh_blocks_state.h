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

#include <optional>

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

struct TSelectiveTrimPlan
{
    // New soft barrier. Equal to the floor when only flags are sent.
    ui64 TrimToCommitId = 0;
    TVector<TPartialBlobId> Keep;
    TVector<TPartialBlobId> DoNotKeep;
    // Dead blobs that the soft command collects or releases. They leave the
    // untrimmed and unflushed accounting only after the command succeeds in
    // every group.
    TVector<ui64> CollectingCommitIds;
};

class TPartitionFreshBlobState
{
private:
    struct TUntrimmedFreshBlob
    {
        ui64 ByteCount = 0;
        // The blob as written. Selective trim names it in Keep and DoNotKeep.
        TPartialBlobId BlobId;
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

    using TUntrimmedFreshBlobsByCommitId = TMap<ui64, TUntrimmedFreshBlob>;
    TUntrimmedFreshBlobsByCommitId UntrimmedFreshBlobsByCommitId;

    // The dead byte counts below weigh a blob at least one block, so that
    // many small dead blobs count as much as they cost.

    // Dead untrimmed blobs.
    ui64 DeadUntrimmedFreshBlobByteCount = 0;
    // Every blob killed in this tablet lifetime. Only grows.
    ui64 TotalKilledFreshBlobByteCount = 0;
    // Untrimmed blobs at or below TrimmableByOrdinaryToCommitId, the last value
    // passed to GetOrdinaryTrimmableFreshBlobByteCount. They are all dead,
    // because that value is below every referenced commit.
    ui64 TrimmableByOrdinaryToCommitId = 0;
    ui64 OrdinaryTrimmableFreshBlobByteCount = 0;

    ui32 UnflushedFreshBlobCount = 0;
    ui64 UnflushedFreshBlobByteCount = 0;

    using TUnflushedFreshBlobsByCommitId = TMap<ui64, TUnflushedFreshBlob>;
    TUnflushedFreshBlobsByCommitId UnflushedFreshBlobsByCommitId;

public:
    TPartitionFreshBlobState(ui64 tabletID, ui32 blockSize)
        : TabletID(tabletID)
        , BlockSize(blockSize)
    {}

    [[nodiscard]] ui64 GetUntrimmedFreshBlobByteCount() const
    {
        return UntrimmedFreshBlobByteCount;
    }

    [[nodiscard]] ui64 GetDeadUntrimmedFreshBlobByteCount() const
    {
        return DeadUntrimmedFreshBlobByteCount;
    }

    [[nodiscard]] ui64 GetTotalKilledFreshBlobByteCount() const
    {
        return TotalKilledFreshBlobByteCount;
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

    void AddFreshBlob(
        ui64 commitId,
        const TPartialBlobId& blobId,
        ui64 blobSize,
        TInstant timestamp);

    // Called when the last trim barrier reference to an untrimmed blob is
    // released.
    void MarkFreshBlobDead(ui64 commitId);

    // Returns the dead bytes of the untrimmed blobs at or below
    // ordinaryTrimToCommitId, which must not decrease between calls.
    [[nodiscard]] ui64 GetOrdinaryTrimmableFreshBlobByteCount(
        ui64 ordinaryTrimToCommitId);

    // For debug checks: true when the dead byte counts match the referenced
    // commits. liveCommitIds: every referenced commit, ascending.
    [[nodiscard]] bool CheckDeadFreshBlobByteCount(
        const TVector<ui64>& liveCommitIds) const;

    // Trims every blob at or below commitId. Returns the unflushed bytes
    // removed.
    [[nodiscard]] ui64 TrimFreshBlobs(ui64 commitId);

    // Trims the blobs that a selective trim collected or released. Returns the
    // unflushed bytes removed.
    [[nodiscard]] ui64 TrimCollectedFreshBlobs(const TVector<ui64>& commitIds);

    // Places the soft barrier on the newest dead blob in
    // (ordinaryTrimToCommitId, selectiveTrimToCommitId] that the flag budget
    // allows, with Keep flags for the live blobs below it and DoNotKeep flags
    // for kept blobs that died. The barrier also collects the blobs at or
    // below ordinaryTrimToCommitId that are known to carry no Keep flag: those
    // of the current generation above lastSentSelectiveTrimToCommitId.
    // liveCommitIds: referenced commits in
    // (ordinaryTrimToCommitId, selectiveTrimToCommitId], ascending. Returns
    // std::nullopt when no soft command is needed.
    [[nodiscard]] std::optional<TSelectiveTrimPlan> BuildSelectiveTrimPlan(
        ui64 ordinaryTrimToCommitId,
        ui64 selectiveTrimToCommitId,
        const TVector<ui64>& liveCommitIds,
        ui64 keptToCommitId,
        ui64 lastSentSelectiveTrimToCommitId,
        ui32 currentGeneration,
        ui64 maxFlags) const;

    // A missing blob must be at or below trimmedToCommitId: a trim that
    // completed during the flush removed it. Returns 0 then.
    ui64 FlushFreshBlob(ui64 commitId, ui64 trimmedToCommitId);

private:
    [[nodiscard]] ui64 GetDeadByteCount(const TUntrimmedFreshBlob& blob) const;

    void EraseUntrimmedFreshBlob(TUntrimmedFreshBlobsByCommitId::iterator it);

    ui64 EraseUnflushedFreshBlob(TUnflushedFreshBlobsByCommitId::iterator it);
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

    // Selective trim. Nothing here is persisted.

    // Every live untrimmed blob at or below this commit carries a Keep flag
    // that every group acknowledged.
    ui64 KeptToCommitId = 0;

    // The highest soft barrier sent in this tablet lifetime, whether or not
    // the command succeeded. A later soft barrier must not be lower.
    ui64 LastSentSelectiveTrimToCommitId = 0;

    // After a scan, the next one waits until the total killed byte count
    // reaches this value, unless the soft command of the scan failed.
    ui64 NextSelectiveScanTotalKilledByteCount = 0;

    // The kind of the last trim started, to alternate when both are requested.
    bool LastTrimWasOrdinary = false;

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

    [[nodiscard]] ui64 GetKeptToCommitId() const
    {
        return KeptToCommitId;
    }

    [[nodiscard]] ui64 GetLastSentSelectiveTrimToCommitId() const
    {
        return LastSentSelectiveTrimToCommitId;
    }

    [[nodiscard]] bool IsSelectiveScanAllowed(ui64 totalKilledByteCount) const
    {
        return totalKilledByteCount >= NextSelectiveScanTotalKilledByteCount;
    }

    // Called on every scan, so that soft commands that collect little cannot
    // follow each other while dead blobs out of reach keep the trim due.
    void DeferSelectiveScan(ui64 totalKilledByteCount, ui64 threshold)
    {
        NextSelectiveScanTotalKilledByteCount =
            totalKilledByteCount + threshold;
    }

    [[nodiscard]] bool GetLastTrimWasOrdinary() const
    {
        return LastTrimWasOrdinary;
    }

    void SetLastTrimWasOrdinary(bool ordinary)
    {
        LastTrimWasOrdinary = ordinary;
    }

    void StartSelectiveTrim(ui64 trimToCommitId)
    {
        Y_ABORT_UNLESS(TrimFreshLogState.Status != EOperationStatus::Started);
        Y_ABORT_UNLESS(trimToCommitId >= LastSentSelectiveTrimToCommitId);
        // Set before sending: a failed or timed-out command may still apply,
        // so its value is the floor for every later soft barrier.
        LastSentSelectiveTrimToCommitId = trimToCommitId;
    }

    void CompleteSelectiveTrim()
    {
        Y_ABORT_UNLESS(TrimFreshLogState.Status == EOperationStatus::Started);
        KeptToCommitId = LastSentSelectiveTrimToCommitId;
    }

    void FailSelectiveTrim()
    {
        Y_ABORT_UNLESS(TrimFreshLogState.Status == EOperationStatus::Started);
        // Keep LastSentSelectiveTrimToCommitId. The entries stay dead and
        // untrimmed, so the next scan is due again. It resends Keep for live
        // blobs in (KeptToCommitId, LastSentSelectiveTrimToCommitId] and
        // DoNotKeep for dead ones.
        NextSelectiveScanTotalKilledByteCount = 0;
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
