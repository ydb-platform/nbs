#pragma once

#include "part_thread_safe_state.h"

#include <cloud/blockstore/libs/storage/core/write_buffer_request.h>
#include <cloud/blockstore/libs/storage/partition_common/commit_ids_state.h>
#include <cloud/blockstore/libs/storage/partition_common/model/barrier.h>
#include <cloud/blockstore/libs/storage/partition_common/model/block_index.h>
#include <cloud/blockstore/libs/storage/partition_common/model/checkpoint.h>
#include <cloud/blockstore/libs/storage/partition_common/model/operation_status.h>

#include <cloud/storage/core/libs/common/backoff_delay_provider.h>
#include <cloud/storage/core/libs/common/verify.h>
#include <cloud/storage/core/libs/tablet/gc_logic.h>

#include <util/datetime/base.h>
#include <util/generic/set.h>
#include <util/generic/utility.h>
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

struct TTrimFreshBlobsResult
{
    ui64 UnflushedBytesRemoved = 0;
};

struct TFlushFreshBlobResult
{
    ui64 UnflushedBytesRemoved = 0;
};

struct TSelectiveTrimPlanParams
{
    ui64 OrdinaryTrimToCommitId = 0;
    ui64 SelectiveTrimToCommitId = 0;
    // Every live blob at or below this commit carries an acknowledged Keep
    // flag.
    ui64 KeptToCommitId = 0;
    // The highest soft barrier sent in this tablet lifetime.
    ui64 LastSentSelectiveTrimToCommitId = 0;
    // Blobs of earlier generations may carry Keep flags of an earlier tablet
    // lifetime.
    ui32 CurrentGeneration = 0;
};

struct TSelectiveTrimPlan
{
    // New soft barrier. Never below OrdinaryTrimToCommitId or
    // LastSentSelectiveTrimToCommitId.
    ui64 TrimToCommitId = 0;
    TVector<TPartialBlobId> Keep;
    TVector<TPartialBlobId> DoNotKeep;
    // Dead blobs that the soft command collects or releases. They leave the
    // untrimmed and unflushed accounting only after the command succeeds in
    // every group.
    TVector<ui64> CollectingCommitIds;
};

struct TFreshBlobStats
{
    ui64 BlobCount = 0;
    ui64 ByteCount = 0;
};

// Untrimmed fresh blobs by the kind of trim that can reach them.
struct TUntrimmedFreshBlobsBreakdown
{
    // At or below OrdinaryTrimToCommitId, so all dead.
    TFreshBlobStats OrdinaryTrimmable;
    // Dead blobs above OrdinaryTrimToCommitId, which an older live blob or
    // in-flight write holds. Until it moves, only a selective trim reaches
    // them.
    TFreshBlobStats DeadAboveBoundary;
    TFreshBlobStats Live;
    // Live blobs at or below the last soft barrier sent in this tablet
    // lifetime: they carry Keep flags.
    ui64 KeptBlobCount = 0;
};

class TPartitionFreshBlobState
{
private:
    struct TUntrimmedFreshBlob
    {
        ui64 ByteCount = 0;
        // The blob as written. Selective trim names it in Keep and DoNotKeep.
        TPartialBlobId BlobId;
        bool Dead = false;
    };

    struct TUnflushedFreshBlob
    {
        ui64 ByteCount = 0;
        TInstant Timestamp;
    };

    ui64 TabletID = 0;

    ui64 UntrimmedFreshBlobByteCount = 0;

    using TUntrimmedFreshBlobsByCommitId = TMap<ui64, TUntrimmedFreshBlob>;
    TUntrimmedFreshBlobsByCommitId UntrimmedFreshBlobsByCommitId;

    ui64 DeadUntrimmedFreshBlobByteCount = 0;
    // Every blob marked dead in this tablet lifetime. Only grows.
    ui64 TotalDeadFreshBlobByteCount = 0;
    ui64 TrimmableByOrdinaryToCommitId = 0;
    ui64 OrdinaryTrimmableFreshBlobByteCount = 0;

    ui32 UnflushedFreshBlobCount = 0;
    ui64 UnflushedFreshBlobByteCount = 0;

    using TUnflushedFreshBlobsByCommitId = TMap<ui64, TUnflushedFreshBlob>;
    TUnflushedFreshBlobsByCommitId UnflushedFreshBlobsByCommitId;

public:
    explicit TPartitionFreshBlobState(ui64 tabletID)
        : TabletID(tabletID)
    {}

    [[nodiscard]] ui64 GetUntrimmedFreshBlobByteCount() const
    {
        return UntrimmedFreshBlobByteCount;
    }

    [[nodiscard]] ui64 GetDeadUntrimmedFreshBlobByteCount() const
    {
        return DeadUntrimmedFreshBlobByteCount;
    }

    [[nodiscard]] ui64 GetTotalDeadFreshBlobByteCount() const
    {
        return TotalDeadFreshBlobByteCount;
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

    // Called when the blob's last trim barrier reference is released.
    void MarkFreshBlobDead(ui64 commitId);

    // Moves the ordinary trim boundary up to ordinaryTrimToCommitId, which must
    // not decrease between calls. Returns the bytes of every untrimmed blob at
    // or below it without reading their Dead flags: the caller keeps the
    // boundary below every trim barrier, so all of these blobs are dead.
    [[nodiscard]] ui64 GetAndUpdateOrdinaryTrimmableFreshBlobByteCount(
        ui64 ordinaryTrimToCommitId);

    // Debug check of the Dead flags and the dead byte counts. liveCommitIds:
    // all referenced commits, ascending. Each of them at or below
    // addedToCommitId must have an untrimmed entry.
    [[nodiscard]] bool CheckDeadFreshBlobByteCount(
        const TVector<ui64>& liveCommitIds,
        ui64 addedToCommitId) const;

    // Scans every untrimmed blob, for the monitoring page.
    [[nodiscard]] TUntrimmedFreshBlobsBreakdown GetUntrimmedFreshBlobsBreakdown(
        ui64 ordinaryTrimToCommitId,
        ui64 lastSentSelectiveTrimToCommitId) const;

    [[nodiscard]] TTrimFreshBlobsResult TrimFreshBlobs(ui64 commitId);

    // Trims the blobs that a selective trim collected or released.
    [[nodiscard]] TTrimFreshBlobsResult TrimCollectedFreshBlobs(
        const TVector<ui64>& commitIds);

    // Places the soft barrier on the newest dead blob in
    // (OrdinaryTrimToCommitId, SelectiveTrimToCommitId], with Keep flags for
    // the live blobs below it and DoNotKeep flags for kept blobs that died. The
    // barrier also collects the blobs at or below OrdinaryTrimToCommitId that
    // are known to carry no Keep flag: those of the current generation above
    // LastSentSelectiveTrimToCommitId. Every blob at or below
    // SelectiveTrimToCommitId must have been added, so that its Dead flag tells
    // whether it is live. Returns std::nullopt when no soft command is needed,
    // in particular when SelectiveTrimToCommitId is not above
    // OrdinaryTrimToCommitId.
    [[nodiscard]] std::optional<TSelectiveTrimPlan> BuildSelectiveTrimPlan(
        const TSelectiveTrimPlanParams& params) const;

    [[nodiscard]] TFlushFreshBlobResult FlushFreshBlob(ui64 commitId);

private:
    void EraseUntrimmedFreshBlob(TUntrimmedFreshBlobsByCommitId::iterator it);

    ui64 EraseUnflushedFreshBlob(TUnflushedFreshBlobsByCommitId::iterator it);
};

struct TTrimFreshLogStats
{
    TOperationState State;
    ui64 SucceededCount = 0;
    ui64 FailedCount = 0;
    ui64 TrimmedByteCount = 0;
};

class TPartitionTrimFreshLogState
{
private:
    ui64 TabletID = 0;

    TOperationState TrimFreshLogState;
    ui64 LastTrimFreshLogToCommitId = 0;
    TBackoffDelayProvider TrimFreshLogBackoffDelayProvider{
        TDuration::Zero(),
        TDuration::MilliSeconds(100),
        TDuration::Seconds(5)};

    bool OrdinaryTrimRequested = false;

    // Selective trim. Nothing here is persisted.

    // Every live untrimmed blob at or below this commit carries a Keep flag
    // that every group acknowledged.
    ui64 KeptToCommitId = 0;

    // The highest soft barrier sent in this tablet lifetime, whether or not
    // the command succeeded. A later soft barrier must not be lower: a VDisk
    // rejects a decreasing one.
    ui64 LastSentSelectiveTrimToCommitId = 0;

    // After a scan, the next one waits until the total dead byte count
    // reaches this value, unless the soft command of the scan failed.
    ui64 NextSelectiveScanTotalDeadByteCount = 0;

    TTrimFreshLogStats OrdinaryTrimStats;
    TTrimFreshLogStats SelectiveTrimStats;

public:
    explicit TPartitionTrimFreshLogState(ui64 tabletID)
        : TabletID(tabletID)
    {}

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

    [[nodiscard]] bool IsSelectiveScanAllowed(ui64 totalDeadByteCount) const
    {
        return totalDeadByteCount >= NextSelectiveScanTotalDeadByteCount;
    }

    [[nodiscard]] ui64 GetNextSelectiveScanTotalDeadByteCount() const
    {
        return NextSelectiveScanTotalDeadByteCount;
    }

    // Called on every scan, so that soft commands that collect little cannot
    // follow each other while dead blobs out of reach keep the trim due.
    void DeferSelectiveScan(ui64 totalDeadByteCount, ui64 threshold)
    {
        STORAGE_VERIFY_C(
            threshold <= Max<ui64>() - totalDeadByteCount,
            TWellKnownEntityTypes::TABLET,
            TabletID,
            "Total dead byte count: " << totalDeadByteCount
                                      << ", threshold: " << threshold);
        NextSelectiveScanTotalDeadByteCount = totalDeadByteCount + threshold;
    }

    void StartSelectiveTrim(ui64 trimToCommitId)
    {
        Y_ABORT_UNLESS(TrimFreshLogState.Status != EOperationStatus::Started);
        Y_ABORT_UNLESS(trimToCommitId >= LastSentSelectiveTrimToCommitId);
        // Set before sending: a failed or timed-out command may still apply,
        // so every later soft barrier must be at or above its value.
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
        NextSelectiveScanTotalDeadByteCount = 0;
    }

    void StartTrimFreshLog(bool selective, TInstant now)
    {
        TrimFreshLogState.SetStatus(EOperationStatus::Started, now);
        auto& stats = selective ? SelectiveTrimStats : OrdinaryTrimStats;
        stats.State.SetStatus(EOperationStatus::Started, now);
    }

    void CompleteTrimFreshLog(
        bool selective,
        bool failed,
        ui64 trimmedByteCount,
        TInstant now)
    {
        TrimFreshLogState.SetStatus(EOperationStatus::Idle, now);
        auto& stats = selective ? SelectiveTrimStats : OrdinaryTrimStats;
        stats.State.SetStatus(EOperationStatus::Idle, now);
        if (failed) {
            ++stats.FailedCount;
            return;
        }
        ++stats.SucceededCount;
        stats.TrimmedByteCount += trimmedByteCount;
    }

    [[nodiscard]] const TTrimFreshLogStats& GetOrdinaryTrimStats() const
    {
        return OrdinaryTrimStats;
    }

    [[nodiscard]] const TTrimFreshLogStats& GetSelectiveTrimStats() const
    {
        return SelectiveTrimStats;
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
