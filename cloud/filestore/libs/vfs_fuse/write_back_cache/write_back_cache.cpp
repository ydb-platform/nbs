#include "write_back_cache.h"

#include "session_sequencer.h"

#include <cloud/filestore/libs/service/context.h>

#include <cloud/storage/core/libs/common/file_ring_buffer.h>

#include <library/cpp/threading/future/subscription/wait_all.h>

#include <util/generic/hash.h>
#include <util/generic/intrlist.h>
#include <util/generic/mem_copy.h>
#include <util/generic/strbuf.h>
#include <util/generic/vector.h>
#include <util/system/mutex.h>

#include <algorithm>
#include <atomic>

namespace NCloud::NFileStore::NFuse {

using namespace NCloud::NFileStore::NVFS;
using namespace NThreading;

////////////////////////////////////////////////////////////////////////////////

class TWriteBackCache::TImpl final
    : public std::enable_shared_from_this<TImpl>
{
private:
    using TWriteDataEntry = TWriteBackCache::TWriteDataEntry;
    using TWriteDataEntryPart = TWriteBackCache::TWriteDataEntryPart;

    const TSessionSequencerPtr Session;
    const ISchedulerPtr Scheduler;
    const ITimerPtr Timer;
    const TDuration AutomaticFlushPeriod;

    struct TPendingWriteDataRequest
        : public TIntrusiveListItem<TPendingWriteDataRequest>
    {
        std::shared_ptr<NProto::TWriteDataRequest> Request;
        TPromise<NProto::TWriteDataResponse> Promise;

        TPendingWriteDataRequest(
                std::shared_ptr<NProto::TWriteDataRequest> request,
                TPromise<NProto::TWriteDataResponse> promise)
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
        TFuture<void> Future;
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
            TDuration automaticFlushPeriod,
            const TString& filePath,
            ui32 capacityBytes)
        : Session(CreateSessionSequencer(std::move(session)))
        , Scheduler(std::move(scheduler))
        , Timer(std::move(timer))
        , AutomaticFlushPeriod(automaticFlushPeriod)
        , WriteDataRequestsQueue(filePath, capacityBytes)
    {
        WriteDataRequestsQueue.Visit([&] (auto, auto serializedRequest) {
            NProto::TWriteDataRequest parsedRequest;
            // TODO(svartmetal): avoid parsing with copy
            Y_ABORT_UNLESS(
                parsedRequest.ParseFromArray(
                    serializedRequest.data(),
                    serializedRequest.size()));

            AddWriteDataEntry(
                parsedRequest.GetHandle(),
                parsedRequest.GetOffset(),
                parsedRequest.GetBuffer().length(),
                serializedRequest);
        });

        if (AutomaticFlushPeriod) {
            ScheduleAutomaticFlush();
        }
    }

    void ScheduleAutomaticFlush()
    {
        Scheduler->Schedule(
            Timer->Now() + AutomaticFlushPeriod,
            [ptr = weak_from_this()] () {
                if (auto self = ptr.lock()) {
                    self->FlushAllData().Subscribe(
                        [ptr = self->weak_from_this()] (auto) {
                            if (auto self = ptr.lock()) {
                                self->ScheduleAutomaticFlush();
                            }
                        });
                }
            });
    }

    void AddWriteDataEntry(
        ui64 handle,
        ui64 offset,
        ui64 bufferLength,
        TStringBuf serializedRequest)
    {
        auto entry = std::make_unique<TWriteDataEntry>(
            handle,
            offset,
            bufferLength,
            serializedRequest);
        WriteDataEntriesByHandle[handle].emplace_back(entry.get());
        WriteDataEntries.PushBack(entry.release());
    }

    // should be protected by |Lock|
    TVector<TWriteDataEntryPart> CalculateDataPartsToRead(
        ui64 handle,
        ui64 startingFromOffset,
        ui64 length)
    {
        auto entriesIter = WriteDataEntriesByHandle.find(handle);
        if (entriesIter == WriteDataEntriesByHandle.end()) {
            return {};
        }

        return TWriteBackCache::CalculateDataPartsToRead(
            entriesIter->second,
            startingFromOffset,
            length);
    }

    // should be protected by |Lock|
    void ReadDataPart(
        TWriteDataEntryPart part,
        ui64 startingFromOffset,
        TString* out)
    {
        // TODO(svartmetal): avoid parsing with copy
        NProto::TWriteDataRequest parsedRequest;
        Y_ABORT_UNLESS(
            parsedRequest.ParseFromArray(
                part.Source->SerializedRequest.data(),
                part.Source->SerializedRequest.size()));

        const char* from = parsedRequest.GetBuffer().data();
        from += part.OffsetInSource;

        char* to = out->begin();
        to += (part.Offset - startingFromOffset);

        MemCopy(to, from, part.Length);
    }

    TFuture<NProto::TReadDataResponse> ReadData(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TReadDataRequest> request)
    {
        TString buffer(request->GetLength(), 0);
        TVector<TWriteDataEntryPart> parts;

        with_lock (Lock) {
            parts = CalculateDataPartsToRead(
                request->GetHandle(),
                request->GetOffset(),
                request->GetLength());

            for (const auto& part: parts)  {
                ReadDataPart(part, request->GetOffset(), &buffer);
            }

            if (!parts.empty() &&
                parts.front().Offset == request->GetOffset() &&
                parts.back().End() ==
                    request->GetOffset() + request->GetLength()
            ) {
                bool sufficient = true;

                for (size_t i = 1; i < parts.size(); i++) {
                    Y_DEBUG_ABORT_UNLESS(parts[i-1].End() <= parts[i].Offset);

                    if (parts[i-1].End() != parts[i].Offset) {
                        sufficient = false;
                        break;
                    }
                }

                if (sufficient) {
                    // serve request from cache

                    NProto::TReadDataResponse response;
                    response.SetBuffer(std::move(buffer));

                    auto promise = NewPromise<NProto::TReadDataResponse>();
                    promise.SetValue(std::move(response));
                    return promise.GetFuture();
                }
            }
        }

        // cache is not sufficient to serve the request - read all the data
        // and apply cache on top of it
        return Session->ReadData(std::move(callContext), request).Apply(
            [handle = request->GetHandle(),
             startingFromOffset = request->GetOffset(),
             length = request->GetLength(),
             buffer = std::move(buffer),
             parts = std::move(parts) ] (auto future)
            {
                auto response = future.ExtractValue();

                // TODO(svartmetal): handle response error

                Y_ABORT_UNLESS(
                    length == response.GetBuffer().length());
                // TODO(svartmetal): support buffer offsetting
                Y_ABORT_UNLESS(0 == response.GetBufferOffset());

                // be careful and don't touch |part.Source| here as it may be
                // already deleted
                for (const auto& part: parts) {
                    const char* from = buffer.data();
                    from += (part.Offset - startingFromOffset);

                    char* to = response.MutableBuffer()->begin();
                    to += (part.Offset - startingFromOffset);

                    MemCopy(to, from, part.Length);
                }

                return response;
            });
    }

    TFuture<NProto::TWriteDataResponse> WriteData(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TWriteDataRequest> request)
    {
        Y_UNUSED(callContext);

        auto promise = NewPromise<NProto::TWriteDataResponse>();
        auto future = promise.GetFuture();

        // TODO(svartmetal): support buffer offsetting
        Y_ABORT_UNLESS(0 == request->GetBufferOffset());

        // TODO(svartmetal): get rid of double-copy
        TString result;
        Y_ABORT_UNLESS(request->SerializeToString(&result));

        with_lock (Lock) {
            if (WriteDataRequestsQueue.Push(result)) {
                AddWriteDataEntry(
                    request->GetHandle(),
                    request->GetOffset(),
                    request->GetBuffer().length(),
                    WriteDataRequestsQueue.Back());
                promise.SetValue({});
            } else {
                auto pending = std::make_unique<TPendingWriteDataRequest>(
                    std::move(request),
                    std::move(promise));
                PendingWriteDataRequests.PushBack(pending.release());
                FlushAllData(); // flushes asynchronously
            }
        }

        return future;
    }

    void OnEntriesFlushed(const TVector<TWriteDataEntry*>& entries)
    {
        with_lock (Lock) {
            for (auto* entry: entries) {
                entry->Flushed = true;
            }

            while (!WriteDataEntries.Empty()) {
                auto* entry = WriteDataEntries.Front();
                if (!entry->Flushed) {
                    return;
                }

                WriteDataRequestsQueue.Pop();

                auto& entries = WriteDataEntriesByHandle[entry->Handle];
                Erase(entries, entry);
                if (entries.empty()) {
                    WriteDataEntriesByHandle.erase(entry->Handle);
                }

                std::unique_ptr<TWriteDataEntry> holder(entry);
                entry->Unlink();
            }
        }
    }

    TFuture<void> FlushData(ui64 handle)
    {
        struct TState
        {
            TPromise<void> Promise = NewPromise();
            std::atomic<int> WriteRequestsRemaining = 0;
        };

        auto state = std::make_shared<TState>();

        TVector<std::shared_ptr<NProto::TWriteDataRequest>> writeRequests;
        TVector<TWriteDataEntry*> entriesToFlush;
        TFuture<void> lastFlushFuture;
        ui64 currFlushId = 0;

        with_lock (Lock) {
            auto entriesIter = WriteDataEntriesByHandle.find(handle);
            if (entriesIter == WriteDataEntriesByHandle.end()) {
                state->Promise.SetValue();
                return state->Promise.GetFuture();
            }

            auto& entries = entriesIter->second;

            const auto nothingToFlush = [] (const TWriteDataEntry* e)
            {
                return e->Flushing || e->Flushed;
            };

            // TODO(svartmetal): optimise, can be done in O(1)
            if (AllOf(entries, nothingToFlush)) {
                auto flushInfoIter = LastFlushInfoByHandle.find(handle);
                if (flushInfoIter == LastFlushInfoByHandle.end()) {
                    state->Promise.SetValue();
                    return state->Promise.GetFuture();
                }

                return flushInfoIter->second.Future;
            }

            currFlushId = ++NextFlushId;

            lastFlushFuture = LastFlushInfoByHandle[handle].Future;
            LastFlushInfoByHandle[handle] = {
                state->Promise.GetFuture(),
                currFlushId
            };

            auto parts = CalculateDataPartsToRead(handle, 0, 0);
            EraseIf(
                parts,
                [=] (const auto& x) { return nothingToFlush(x.Source); });

            size_t partIndex = 0;
            while (partIndex < parts.size()) {
                auto rangeEndIndex = partIndex;

                while (++rangeEndIndex < parts.size()) {
                    const auto& prevPart = parts[rangeEndIndex - 1];
                    Y_DEBUG_ABORT_UNLESS(
                        prevPart.End() <= parts[rangeEndIndex].Offset);

                    if (prevPart.End() != parts[rangeEndIndex].Offset) {
                        break;
                    }
                }

                ui64 rangeLength = 0;
                for (size_t i = partIndex; i < rangeEndIndex; i++) {
                    rangeLength += parts[i].Length;
                }
                TString buffer(rangeLength, 0);

                const auto startingFromOffset = parts[partIndex].Offset;
                for (size_t i = partIndex; i < rangeEndIndex; i++) {
                    ReadDataPart(parts[i], startingFromOffset, &buffer);
                }

                auto request = std::make_shared<NProto::TWriteDataRequest>();
                request->SetHandle(handle);
                request->SetOffset(parts[partIndex].Offset);
                request->SetBuffer(std::move(buffer));

                writeRequests.push_back(std::move(request));

                partIndex = rangeEndIndex;
            }

            for (auto* entry: entries) {
                if (nothingToFlush(entry)) {
                    continue;
                }

                entry->Flushing = true;
                entriesToFlush.push_back(entry);
            }
        }

        auto future = state->Promise.GetFuture();

        if (lastFlushFuture.Initialized()) {
            future = NWait::WaitAll(lastFlushFuture, future);
        }

        future.Subscribe([=, ptr = weak_from_this()] (auto) {
            if (auto self = ptr.lock()) {
                with_lock (self->Lock) {
                    auto flushInfoIter =
                        self->LastFlushInfoByHandle.find(handle);
                    if (flushInfoIter == self->LastFlushInfoByHandle.end()) {
                        return;
                    }

                    const auto& flushInfo = flushInfoIter->second;
                    if (flushInfo.FlushId == currFlushId) {
                        self->LastFlushInfoByHandle.erase(handle);
                    }
                }
            }
        });

        if (writeRequests.empty()) {
            state->Promise.SetValue();
            return future;
        }

        state->Promise.GetFuture().Subscribe(
            [ptr = weak_from_this(),
             entriesToFlush = std::move(entriesToFlush)] (auto)
            {
                if (auto self = ptr.lock()) {
                    self->OnEntriesFlushed(entriesToFlush);
                }
            });

        state->WriteRequestsRemaining = writeRequests.size();

        for (auto& request: writeRequests) {
            Session->WriteData(
                MakeIntrusive<TCallContext>(),
                std::move(request)).Subscribe(
                    [state] (auto)
                    {
                        // TODO(svartmetal): handle response error
                        if (--state->WriteRequestsRemaining == 0) {
                            state->Promise.SetValue();
                        }
                    });
        }

        return future;
    }

    TFuture<void> FlushAllData()
    {
        struct TState
        {
            TPromise<void> Promise = NewPromise();
            std::atomic<int> FlushRequestsRemaining = 0;
        };

        auto state = std::make_shared<TState>();

        TVector<ui64> handlesToFlush;
        TFuture<void> lastFlushFuture;
        ui64 currFlushId = 0;

        with_lock (Lock) {
            const auto nothingToFlush = [] (const TWriteDataEntry& e)
            {
                return e.Flushing || e.Flushed;
            };

            // TODO(svartmetal): optimise, can be done in O(1)
            if (AllOf(WriteDataEntries, nothingToFlush)) {
                if (LastFlushAllDataInfo.Future.Initialized()) {
                    return LastFlushAllDataInfo.Future;
                }

                state->Promise.SetValue();
                return state->Promise.GetFuture();
            }

            currFlushId = ++NextFlushId;

            lastFlushFuture = LastFlushAllDataInfo.Future;
            LastFlushAllDataInfo = {
                state->Promise.GetFuture(),
                currFlushId,
            };

            for (const auto& [handle, entries]: WriteDataEntriesByHandle) {
                handlesToFlush.push_back(handle);
            }
        }

        auto future = state->Promise.GetFuture();

        if (lastFlushFuture.Initialized()) {
            future = NWait::WaitAll(lastFlushFuture, future);
        }

        future.Subscribe([=, ptr = weak_from_this()] (auto) {
            if (auto self = ptr.lock()) {
                with_lock (self->Lock) {
                    if (self->LastFlushAllDataInfo.Future.Initialized() &&
                        currFlushId == self->LastFlushAllDataInfo.FlushId)
                    {
                        self->LastFlushAllDataInfo.Future = {};
                    }
                }
            }
        });

        if (handlesToFlush.empty()) {
            state->Promise.SetValue();
            return state->Promise.GetFuture();
        }

        state->FlushRequestsRemaining = handlesToFlush.size();

        for (const auto& handle: handlesToFlush) {
            FlushData(handle).Subscribe([state] (auto) {
                if (--state->FlushRequestsRemaining == 0) {
                    state->Promise.SetValue();
                }
            });
        }

        future.Subscribe([ptr = weak_from_this()] (auto) {
            auto self = ptr.lock();
            if (!self) {
                return;
            }

            with_lock (self->Lock) {
                while (!self->PendingWriteDataRequests.Empty()) {
                    auto* pending = self->PendingWriteDataRequests.Front();
                    auto request = std::move(pending->Request);

                    // TODO(svartmetal): get rid of double-copy
                    TString result;
                    Y_ABORT_UNLESS(request->SerializeToString(&result));

                    if (!self->WriteDataRequestsQueue.Push(result)) {
                        self->FlushAllData(); // flushes asynchronously
                        return;
                    }

                    self->AddWriteDataEntry(
                        request->GetHandle(),
                        request->GetOffset(),
                        request->GetBuffer().length(),
                        self->WriteDataRequestsQueue.Back());

                    pending->Promise.SetValue({});

                    std::unique_ptr<TPendingWriteDataRequest> holder(pending);
                    pending->Unlink();
                }
            }
        });

        return future;
    }
};

////////////////////////////////////////////////////////////////////////////////

TWriteBackCache::TWriteBackCache(
        IFileStorePtr session,
        ISchedulerPtr scheduler,
        ITimerPtr timer,
        TDuration automaticFlushPeriod,
        const TString& filePath,
        ui32 capacityBytes)
    : Impl(
        new TImpl(
            session,
            scheduler,
            timer,
            automaticFlushPeriod,
            filePath,
            capacityBytes))
{}

TWriteBackCache::~TWriteBackCache() = default;

TFuture<NProto::TReadDataResponse> TWriteBackCache::ReadData(
    TCallContextPtr callContext,
    std::shared_ptr<NProto::TReadDataRequest> request)
{
    return Impl->ReadData(std::move(callContext), std::move(request));
}

TFuture<NProto::TWriteDataResponse> TWriteBackCache::WriteData(
    TCallContextPtr callContext,
    std::shared_ptr<NProto::TWriteDataRequest> request)
{
    return Impl->WriteData(std::move(callContext), std::move(request));
}

TFuture<void> TWriteBackCache::FlushData(ui64 handle)
{
    return Impl->FlushData(handle);
}

TFuture<void> TWriteBackCache::FlushAllData()
{
    return Impl->FlushAllData();
}

////////////////////////////////////////////////////////////////////////////////

auto TWriteBackCache::CalculateDataPartsToRead(
    const TVector<TWriteDataEntry*>& entries,
    ui64 startingFromOffset,
    ui64 length) -> TVector<TWriteDataEntryPart>
{
    struct TPoint
    {
        const TWriteDataEntry* Entry = nullptr;
        ui64 Offset = 0;
        bool IsEnd = false;
        size_t Order = 0;
    };

    TVector<TPoint> points(Reserve(entries.size()));

    for (size_t i = 0; i < entries.size(); i++) {
        const auto* entry = entries[i];

        auto pointBegin = entry->Offset;
        auto pointEnd = entry->End();
        if (length != 0) {
            // intersect with [startingFromOffset, length)
            pointBegin = Max(pointBegin, startingFromOffset);
            pointEnd = Min(pointEnd, startingFromOffset + length);
        }

        if (pointBegin < pointEnd) {
            points.emplace_back(entry, pointBegin, false, i);
            points.emplace_back(entry, pointEnd, true, i);
        }
    }

    if (points.empty()) {
        return {};
    }

    StableSort(points, [] (const auto& l, const auto& r) {
        return l.Offset < r.Offset;
    });

    TVector<TWriteDataEntryPart> res(Reserve(entries.size()));

    const auto& heapComparator = [] (const auto& l, const auto& r) {
        return l.Order < r.Order;
    };
    TVector<TPoint> heap;

    const auto cutTop = [&res, &heap] (auto lastOffset, auto currOffset)
    {
        if (currOffset <= lastOffset) {
            // ignore
            return lastOffset;
        }

        const auto partLength = currOffset - lastOffset;
        const auto& top = heap.front();

        Y_DEBUG_ABORT_UNLESS(lastOffset >= top.Entry->Offset);
        const auto offsetInSource = lastOffset - top.Entry->Offset;

        if (!res.empty() &&
            res.back().Source == top.Entry &&
            res.back().End() == lastOffset)
        {
            // extend last entry
            res.back().Length += partLength;
        } else {
            res.emplace_back(
                top.Entry, // source
                offsetInSource,
                lastOffset,
                partLength);
        }

        // cut part of the top
        lastOffset += partLength;
        return lastOffset;
    };

    ui64 cutEnd = 0;
    for (const auto& point: points) {
        if (point.IsEnd) {
            // cut at every interval's end
            cutEnd = cutTop(cutEnd, point.Offset);
        } else {
            if (heap.empty()) {
                // no intervals before, start from scratch
                cutEnd = point.Offset;
            } else {
                if (point.Order > heap.front().Order) {
                    // new interval is now on top
                    cutEnd = cutTop(cutEnd, point.Offset);
                }
            }

            // add to heap
            heap.push_back(point);
            std::push_heap(heap.begin(), heap.end(), heapComparator);
        }

        // remove affected (cut) entries
        while (!heap.empty()) {
            const auto& top = heap.front();
            if (cutEnd < top.Entry->End()) {
                break;
            }

            // remove from heap
            std::pop_heap(heap.begin(), heap.end(), heapComparator);
            heap.pop_back();
        }
    }

    return res;
}

////////////////////////////////////////////////////////////////////////////////

TWriteBackCachePtr CreateWriteBackCache(
    IFileStorePtr session,
    ISchedulerPtr scheduler,
    ITimerPtr timer,
    TDuration automaticFlushPeriod,
    const TString& filePath,
    ui32 capacityBytes)
{
    return std::make_unique<TWriteBackCache>(
        session,
        scheduler,
        timer,
        automaticFlushPeriod,
        filePath,
        capacityBytes);
}

}   // namespace NCloud::NFileStore::NFuse
