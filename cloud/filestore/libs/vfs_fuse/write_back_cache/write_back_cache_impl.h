#pragma once

#include "write_back_cache.h"
#include "session_sequencer.h"

#include <cloud/storage/core/libs/common/file_ring_buffer.h>

#include <util/generic/hash_set.h>

namespace NCloud::NFileStore::NFuse {

////////////////////////////////////////////////////////////////////////////////

class TWriteBackCache::TImpl final
    : public std::enable_shared_from_this<TImpl>
{
private:
    enum class EWriteDataEntryStatus;
    class TWriteDataEntry;
    struct TWriteDataEntryPart;
    struct THandleEntry;
    class TFlushOperation;
    struct TPendingOperations;

    const TSessionSequencerPtr Session;
    const ISchedulerPtr Scheduler;
    const ITimerPtr Timer;
    const TDuration AutomaticFlushPeriod;

    // All fields below should be protected by this lock
    TMutex Lock;

    // Entries with statues Cached and CachedFlushRequested
    TIntrusiveListWithAutoDelete<TWriteDataEntry, TDelete> CachedEntries;

    // Entries with statues Pending and PendingFlushRequested
    TIntrusiveList<TWriteDataEntry> PendingEntries;

    // Serialized entries from CachedEntries with one-by-one correspondence.
    TFileRingBuffer CachedEntriesPersistentQueue;

    // Cached and pending WriteData entries grouped by handle
    THashMap<ui64, THandleEntry> EntriesByHandle;

    // Handles with new cached WriteData entries since last FlushAll
    THashSet<ui64> HandlesWithNewCachedEntries;

    // Pending and executing flush operations
    THashMap<ui64, std::unique_ptr<TFlushOperation>> FlushStateByHandle;

public:
    TImpl(
        IFileStorePtr session,
        ISchedulerPtr scheduler,
        ITimerPtr timer,
        const TString& filePath,
        ui64 capacityBytes,
        TDuration automaticFlushPeriod);

    NThreading::TFuture<NProto::TReadDataResponse> ReadData(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TReadDataRequest> request);

    NThreading::TFuture<NProto::TWriteDataResponse> WriteData(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TWriteDataRequest> request);

    NThreading::TFuture<void> FlushData(ui64 handle);

    NThreading::TFuture<void> FlushAllData();

    void ScheduleAutomaticFlushIfNeeded();

    static TVector<TWriteDataEntryPart> CalculateDataPartsToRead(
        const TDeque<TWriteDataEntry*>& entries,
        ui64 startingFromOffset,
        ui64 length);

    static TVector<TWriteDataEntryPart> InvertDataParts(
        const TVector<TWriteDataEntryPart>& parts,
        ui64 startingFromOffset,
        ui64 length);

private:
    TVector<TWriteDataEntryPart> CalculateCachedDataPartsToRead(
        ui64 handle,
        ui64 startingFromOffset,
        ui64 length);

    TVector<TWriteDataEntryPart> CalculateCachedDataPartsToRead(ui64 handle);

    void ReadDataPart(
        TWriteDataEntryPart part,
        ui64 startingFromOffset,
        TString* out);

    bool TryAddEntryToPersistentQueue(
        TWriteDataEntry* entry,
        TPendingOperations& pendingOperations);

    void RequestFlush(ui64 handle, TPendingOperations& pendingOperations);
    void RequestFlushAll(TPendingOperations& pendingOperations);

    void StartPendingOperations(TPendingOperations& pendingOperations);

    void OnEntriesFlushed(
        ui64 handle,
        size_t entriesCount,
        TPendingOperations& pendingOperations);

    void ClearFinishedEntries(TPendingOperations& pendingOperations);

    TVector<std::shared_ptr<NProto::TWriteDataRequest>>
    MakeWriteDataRequestsForFlush(
        ui64 handle,
        const TVector<TWriteDataEntryPart>& parts);

    NThreading::TFuture<void> RequestFlushData(
        ui64 handle,
        TPendingOperations& pendingOperations);

    // only for testing purposes
    friend struct TCalculateDataPartsToReadTestBootstrap;
};

////////////////////////////////////////////////////////////////////////////////

enum class TWriteBackCache::TImpl::EWriteDataEntryStatus
{
    Invalid,
    Pending,
    PendingFlushRequested,
    Cached,
    CachedFlushRequested,
    Finished
};

////////////////////////////////////////////////////////////////////////////////

class TWriteBackCache::TImpl::TWriteDataEntry
    : public TIntrusiveListItem<TWriteDataEntry>
{
private:
    // Store request metadata and request buffer separately
    // The idea is to deduplicate memory and to reference request buffer
    // directly in the cache if the request is cached.
    std::shared_ptr<NProto::TWriteDataRequest> Request;
    TString RequestBuffer;
    TStringBuf Buffer;

    NThreading::TPromise<NProto::TWriteDataResponse> Promise;
    NThreading::TPromise<void> FinishedPromise;
    EWriteDataEntryStatus Status = EWriteDataEntryStatus::Invalid;
    mutable int RefCount = 0;

public:
    EWriteDataEntryStatus GetStatus() const
    {
        return Status;
    }

    const NProto::TWriteDataRequest* GetRequest() const
    {
        return Request.get();
    }

    ui64 GetHandle() const
    {
        return Request->GetHandle();
    }

    TStringBuf GetBuffer() const
    {
        return Buffer;
    }

    ui64 Begin() const
    {
        return Request->GetOffset();
    }

    ui64 End() const
    {
        return Request->GetOffset() + Buffer.size();
    }

    bool CanBeCleared() const
    {
        return Status == EWriteDataEntryStatus::Finished && RefCount == 0;
    }

    void IncrementRefCount() const
    {
        RefCount++;
    }

    void DecrementRefCount() const
    {
        Y_ABORT_UNLESS(--RefCount >= 0);
    }

    static std::unique_ptr<TWriteDataEntry> CreatePendingRequest(
        std::shared_ptr<NProto::TWriteDataRequest> request);

    static std::unique_ptr<TWriteDataEntry> DeserializeCachedRequest(
        TStringBuf serializedRequest);

    size_t GetSerializedSize() const;
    void MoveToCache(char* data, TPendingOperations& pendingOperations);
    void Finish(TPendingOperations& pendingOperations);

    bool FlushRequested() const;
    bool RequestFlush();

    NThreading::TFuture<NProto::TWriteDataResponse> GetFuture();
    NThreading::TFuture<void> GetFinishedFuture();
};

////////////////////////////////////////////////////////////////////////////////

struct TWriteBackCache::TImpl::TWriteDataEntryPart
{
    const TWriteDataEntry* Source = nullptr;
    ui64 OffsetInSource = 0;
    ui64 Offset = 0;
    ui64 Length = 0;

    ui64 End() const
    {
        return Offset + Length;
    }

    bool operator==(const TWriteDataEntryPart& p) const
    {
        return std::tie(Source, OffsetInSource, Offset, Length) ==
               std::tie(p.Source, p.OffsetInSource, p.Offset, p.Length);
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TWriteBackCache::TImpl::THandleEntry
{
    // Entries from TWriteBackCache::TImpl::CachedEntries
    // with statues Cached and CachedFlushRequested filtered by handle
    // The order is preserved
    TDeque<TWriteDataEntry*> CachedEntries;

    // Entries from TWriteBackCache::TImpl::PendingEntries
    // with statues Pending and PendingFlushRequested filtered by handle
    // The order is preserved
    TDeque<TWriteDataEntry*> PendingEntries;

    // Count entries with statues CachedFlushRequested and PendingFlushRequested
    size_t EntriesWithFlushRequested = 0;

    bool Empty() const;
    bool ShouldFlush() const;
    TWriteDataEntry* GetLastEntry() const;
};

////////////////////////////////////////////////////////////////////////////////

class TWriteBackCache::TImpl::TFlushOperation
{
private:
    const ui64 Handle = 0;
    TVector<std::shared_ptr<NProto::TWriteDataRequest>> WriteRequests;
    TVector<std::shared_ptr<NProto::TWriteDataRequest>> FailedWriteRequests;
    size_t AffectedWriteDataEntriesCount = 0;
    size_t RemainingWriteRequestsCount = 0;

public:
    explicit TFlushOperation(ui64 handle)
        : Handle(handle)
    {}

    void Start(TImpl* impl);

private:
    bool Prepare(TImpl* impl);
    void WriteDataRequestCompleted(
        TImpl* impl,
        size_t index,
        const NProto::TWriteDataResponse& response);
    void ScheduleRetry(TImpl* impl);
    void Complete(TImpl* impl);
};

////////////////////////////////////////////////////////////////////////////////

// Accumulate operations to be executed ouside |Lock|
struct TWriteBackCache::TImpl::TPendingOperations
{
    TVector<TFlushOperation*> FlushOperations;
    TVector<NThreading::TPromise<NProto::TWriteDataResponse>> PromisesToSet;
    TVector<NThreading::TPromise<void>> FinishedPromisesToSet;
};

}   // namespace NCloud::NFileStore::NFuse
