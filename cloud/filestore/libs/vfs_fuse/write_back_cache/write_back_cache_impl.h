#pragma once

#include "write_back_cache.h"

#include "session_sequencer.h"

#include <cloud/storage/core/libs/common/file_ring_buffer.h>

#include <util/generic/hash.h>
#include <util/generic/intrlist.h>

namespace NCloud::NFileStore::NFuse {

////////////////////////////////////////////////////////////////////////////////

class TWriteBackCache::TImpl final
    : public std::enable_shared_from_this<TImpl>
{
private:
    enum class EFlushStatus;
    class TWriteDataEntry;
    struct TWriteDataEntryPart;
    struct TPendingWriteDataRequest;

    const TSessionSequencerPtr Session;
    const ISchedulerPtr Scheduler;
    const ITimerPtr Timer;
    const TDuration AutomaticFlushPeriod;

    // TODO(nasonov): remove this wrapper when TFileRingBuffer supports
    // in-place allocation
    class TFileRingBuffer: public NCloud::TFileRingBuffer
    {
        using NCloud::TFileRingBuffer::TFileRingBuffer;

    public:
        ui64 MaxAllocationSize() const;
        bool AllocateBack(size_t size, char** ptr);
        void CompleteAllocation(char* ptr);
    };

    struct TPendingWriteDataRequest
        : public TIntrusiveListItem<TPendingWriteDataRequest>
    {
        std::unique_ptr<TWriteDataEntry> Entry;
        NThreading::TPromise<NProto::TWriteDataResponse> Promise;

        TPendingWriteDataRequest(
                std::unique_ptr<TWriteDataEntry> entry,
                NThreading::TPromise<NProto::TWriteDataResponse> promise)
            : Entry(std::move(entry))
            , Promise(std::move(promise))
        {}
    };

    // all fields below should be protected by this lock
    TMutex Lock;

    // used as an index for |WriteDataRequestsQueue| to speed up lookups
    THashMap<ui64, TVector<TWriteDataEntry*>> WriteDataEntriesByHandle;
    TIntrusiveListWithAutoDelete<TWriteDataEntry, TDelete> WriteDataEntries;

    TFileRingBuffer WriteDataRequestsQueue;

    TIntrusiveListWithAutoDelete<TPendingWriteDataRequest, TDelete>
        PendingWriteDataRequests;

    struct TFlushInfo
    {
        NThreading::TFuture<void> Future;
        ui64 FlushId = 0;
    };

    THashMap<ui64, TFlushInfo> LastFlushInfoByHandle;
    TFlushInfo LastFlushAllDataInfo;
    ui64 NextFlushId = 0;

public:
    TImpl(
        IFileStorePtr session,
        ISchedulerPtr scheduler,
        ITimerPtr timer,
        const TString& filePath,
        ui32 capacityBytes,
        TDuration automaticFlushPeriod);

    void ScheduleAutomaticFlushIfNeeded();

    NThreading::TFuture<NProto::TReadDataResponse> ReadData(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TReadDataRequest> request);

    NThreading::TFuture<NProto::TWriteDataResponse> WriteData(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TWriteDataRequest> request);

    NThreading::TFuture<void> FlushData(ui64 handle);

    NThreading::TFuture<void> FlushAllData();

private:
    void AddWriteDataEntry(std::unique_ptr<TWriteDataEntry> entry);

    TVector<TWriteDataEntryPart> CalculateCachedDataPartsToRead(
        ui64 handle,
        ui64 startingFromOffset,
        ui64 length);

    TVector<TWriteDataEntryPart> CalculateCachedDataPartsToRead(ui64 handle);

    void ReadDataPart(
        TWriteDataEntryPart part,
        ui64 startingFromOffset,
        TString* out);

    void OnEntriesFlushed(const TVector<TWriteDataEntry*>& entries);

    auto MakeWriteDataRequestsForFlush(
        ui64 handle) -> TVector<std::shared_ptr<NProto::TWriteDataRequest>>;

    static TVector<TWriteDataEntryPart> CalculateDataPartsToRead(
        const TVector<TWriteDataEntry*>& entries,
        ui64 startingFromOffset,
        ui64 length);

    static TVector<TWriteDataEntryPart> InvertDataParts(
        const TVector<TWriteDataEntryPart>& parts,
        ui64 startingFromOffset,
        ui64 length);

    // only for testing purposes
    friend struct TCalculateDataPartsToReadTestBootstrap;
};

////////////////////////////////////////////////////////////////////////////////

enum class TWriteBackCache::TImpl::EFlushStatus
{
    NotStarted,
    Started,
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

    // Reference to either RequestBuffer or a memory region
    // in WriteDataRequestsQueue
    TStringBuf Buffer;

public:
    explicit TWriteDataEntry(
        std::shared_ptr<NProto::TWriteDataRequest> request);

    explicit TWriteDataEntry(TStringBuf serializedRequest);

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

    size_t GetSerializedSize() const;
    void SerializeToCache(char* cachePtr);

    EFlushStatus FlushStatus = EFlushStatus::NotStarted;
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

}   // namespace NCloud::NFileStore::NFuse
