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
    struct TWriteDataEntry;
    struct TWriteDataEntryPart;
    struct TPendingWriteDataRequest;

    const TSessionSequencerPtr Session;
    const ISchedulerPtr Scheduler;
    const ITimerPtr Timer;
    const TDuration AutomaticFlushPeriod;

    struct TPendingWriteDataRequest
        : public TIntrusiveListItem<TPendingWriteDataRequest>
    {
        std::shared_ptr<NProto::TWriteDataRequest> Request;
        NThreading::TPromise<NProto::TWriteDataResponse> Promise;

        TPendingWriteDataRequest(
                std::shared_ptr<NProto::TWriteDataRequest> request,
                NThreading::TPromise<NProto::TWriteDataResponse> promise)
            : Request(std::move(request))
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
    void AddWriteDataEntry(
        const NProto::TWriteDataRequest& request,
        TStringBuf serializedRequest);

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

struct TWriteBackCache::TImpl::TWriteDataEntry
    : public TIntrusiveListItem<TWriteDataEntry>
{
    ui64 Handle;
    ui64 Offset;
    ui64 Length;
    // serialized TWriteDataRequest
    TStringBuf SerializedRequest;
    TString FileSystemId;
    NProto::THeaders RequestHeaders;

    TWriteDataEntry(
            ui64 handle,
            ui64 offset,
            ui64 length,
            TStringBuf serializedRequest,
            TString fileSystemId,
            NProto::THeaders requestHeaders)
        : Handle(handle)
        , Offset(offset)
        , Length(length)
        , SerializedRequest(serializedRequest)
        , FileSystemId(std::move(fileSystemId))
        , RequestHeaders(std::move(requestHeaders))
    {}

    ui64 End() const
    {
        return Offset + Length;
    }

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
