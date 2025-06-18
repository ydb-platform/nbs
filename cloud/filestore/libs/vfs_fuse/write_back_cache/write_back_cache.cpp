#include "write_back_cache.h"

#include "session_sequencer.h"

#include <cloud/filestore/libs/service/context.h>

#include <cloud/storage/core/libs/common/file_ring_buffer.h>

#include <library/cpp/threading/future/subscription/wait_all.h>

#include <util/generic/hash_set.h>
#include <util/generic/intrlist.h>
#include <util/generic/mem_copy.h>
#include <util/generic/strbuf.h>
#include <util/generic/vector.h>
#include <util/stream/mem.h>
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

    // All fields below should be protected by this lock
    TMutex Lock;

    TIntrusiveListWithAutoDelete<TWriteDataEntry, TDelete>
        WriteDataEntriesQueue;

    TIntrusiveList<TWriteDataEntry> PendingWriteDataEntriesQueue;

    // Must be synchronized with WriteDataEntriesQueue
    TFileRingBuffer WriteDataRequestsPersistentQueue;

    struct THandleEntry
    {
        // Entries with statues Cached and CachedFlushRequested
        // The order corresponds to WriteDataEntriesQueue
        TDeque<TWriteDataEntry*> WriteDataEntries;

        // Entries with statuses Pending and PendingFlushRequested
        // The order corresponds to PendingWriteDataEntriesQueue
        TDeque<TWriteDataEntry*> PendingWriteDataEntries;

        size_t WriteDataEntriesWithFlushRequested = 0;
        size_t PendingWriteDataEntriesWithFlushRequested = 0;
    };

    THashMap<ui64, THandleEntry> WriteDataEntriesByHandle;

    THashSet<ui64> HandlesWithUpdates;

    struct TFlushState
    {
        const ui64 Handle = 0;
        TVector<std::shared_ptr<NProto::TWriteDataRequest>> WriteRequests;
        TVector<std::shared_ptr<NProto::TWriteDataRequest>> FailedWriteRequests;
        size_t AffectedWriteDataEntriesCount = 0;
        size_t RemainingWriteRequestsCount = 0;

        explicit TFlushState(ui64 handle)
            : Handle(handle)
        {}
    };

    THashMap<ui64, std::unique_ptr<TFlushState>> FlushStateByHandle;

    // Accumulate operations to be executed ouside |Lock|
    struct TPendingOperations
    {
        TVector<TFlushState*> FlushOperations;
        TVector<NThreading::TPromise<NProto::TWriteDataResponse>> PromisesToSet;
        TVector<NThreading::TPromise<void>> FinishedPromisesToSet;
    };

public:
    TImpl(
        IFileStorePtr session,
        ISchedulerPtr scheduler,
        ITimerPtr timer,
        const TString& filePath,
        ui64 capacityBytes,
        TDuration automaticFlushPeriod)
        : Session(CreateSessionSequencer(std::move(session)))
        , Scheduler(std::move(scheduler))
        , Timer(std::move(timer))
        , AutomaticFlushPeriod(automaticFlushPeriod)
        , WriteDataRequestsPersistentQueue(filePath, capacityBytes)
    {
        // should fit 1 MiB of data plus some headers (assume 1 KiB is enough)
        Y_ABORT_UNLESS(capacityBytes >= 1024 * 1024 + 1024);

        WriteDataRequestsPersistentQueue.Visit(
            [&](auto, TStringBuf serializedRequest)
            {
                ui32 bufferSize = 0;
                TMemoryInput mi(serializedRequest);
                mi.Read(&bufferSize, sizeof(ui32));
                mi.Skip(bufferSize);

                auto parsedRequest =
                    std::make_shared<NProto::TWriteDataRequest>();
                Y_ABORT_UNLESS(parsedRequest->ParseFromArray(
                    mi.Buf(),
                    static_cast<int>(mi.Avail())));

                auto entry =
                    std::make_unique<TWriteDataEntry>(std::move(parsedRequest));
                entry->Status = EWriteDataEntryStatus::Cached;
                entry->Buffer = TStringBuf(
                    serializedRequest.Data() + sizeof(ui32),
                    bufferSize);

                auto handle = entry->Request->GetHandle();
                auto& handleEntry = WriteDataEntriesByHandle[handle];

                handleEntry.WriteDataEntries.emplace_back(entry.get());
                WriteDataEntriesQueue.PushBack(entry.release());
            });

        ScheduleAutomaticFlushIfNeeded();
    }

    void ScheduleAutomaticFlushIfNeeded()
    {
        if (!AutomaticFlushPeriod) {
            return;
        }

        Scheduler->Schedule(
            Timer->Now() + AutomaticFlushPeriod,
            [ptr = weak_from_this()] () {
                if (auto self = ptr.lock()) {
                    TPendingOperations pendingOperations;
                    with_lock (self->Lock) {
                        self->RequestFlushAll(pendingOperations);
                    }
                    self->StartPendingOperations(pendingOperations);
                    self->ScheduleAutomaticFlushIfNeeded();
                }
            });
    }

    // should be protected by |Lock|
    TVector<TWriteDataEntryPart> CalculateCachedDataPartsToRead(
        ui64 handle,
        ui64 startingFromOffset,
        ui64 length)
    {
        auto entriesIter = WriteDataEntriesByHandle.find(handle);
        if (entriesIter == WriteDataEntriesByHandle.end()) {
            return {};
        }

        return TWriteBackCache::CalculateDataPartsToRead(
            entriesIter->second.WriteDataEntries,
            startingFromOffset,
            length);
    }

    // should be protected by |Lock|
    TVector<TWriteDataEntryPart> CalculateCachedDataPartsToRead(ui64 handle)
    {
        return CalculateCachedDataPartsToRead(handle, 0, 0);
    }

    // should be protected by |Lock|
    void ReadDataPart(
        TWriteDataEntryPart part,
        ui64 startingFromOffset,
        TString* out)
    {
        const char* from = part.Source->Buffer.data();
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
            parts = CalculateCachedDataPartsToRead(
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

            // cache is not sufficient to serve the request - read all the data
            // and apply cache on top of it
            return Session->ReadData(std::move(callContext), request).Apply(
                [handle = request->GetHandle(),
                 startingFromOffset = request->GetOffset(),
                 length = request->GetLength(),
                 buffer = std::move(buffer),
                 parts = std::move(parts)] (auto future)
                {
                    auto response = future.ExtractValue();

                    // TODO(svartmetal): handle response error
                    Y_ABORT_UNLESS(SUCCEEDED(response.GetError().GetCode()));

                    if (response.GetBuffer().empty()) {
                        *response.MutableBuffer() = std::move(buffer);
                        return response;
                    }

                    Y_ABORT_UNLESS(
                        length >= response.GetBuffer().length(),
                        "expected length %lu to be >= than %lu",
                        length,
                        response.GetBuffer().length());
                    // TODO(svartmetal): get rid of reallocation here
                    response.MutableBuffer()->resize(length, 0);

                    // TODO(svartmetal): support buffer offsetting
                    Y_ABORT_UNLESS(0 == response.GetBufferOffset());

                    // be careful and don't touch |part.Source| here as it may
                    // be already deleted
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
    }

    // should be protected by |Lock|
    bool TryAddWriteDataEntryToPersistentQueue(TWriteDataEntry* entry)
    {
        if (entry->Buffer.size() >= Max<ui32>()) {
            return false;
        }

        int serializedRequestSize = entry->Request->ByteSize();
        ui32 bufferSize = static_cast<ui32>(entry->Buffer.size());
        auto totalSize = sizeof(ui32) + bufferSize +
            static_cast<size_t>(serializedRequestSize);

        char* ptr = WriteDataRequestsPersistentQueue.AllocateBack(totalSize);
        if (!ptr) {
            return false;
        }

        // TODO(nasonov): perform copy of enter->Buffer to the persistent queue
        // outside lock
        TMemoryOutput mo(ptr, totalSize);
        mo.Write(&bufferSize, sizeof(bufferSize));
        mo.Write(entry->Buffer);
        Y_ABORT_UNLESS(entry->Request->SerializeToArray(
            mo.Buf(),
            static_cast<int>(mo.Avail())));

        entry->Buffer = TStringBuf(ptr + sizeof(ui32), bufferSize);
        entry->RequestBuffer.clear();

        WriteDataRequestsPersistentQueue.CompleteAllocation(ptr);
        return true;
    }

    TFuture<NProto::TWriteDataResponse> WriteData(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TWriteDataRequest> request)
    {
        if (request->GetFileSystemId().empty()) {
            request->SetFileSystemId(callContext->FileSystemId);
        }

        auto entry = std::make_unique<TWriteDataEntry>(std::move(request));
        entry->Promise = NewPromise<NProto::TWriteDataResponse>();
        auto future = entry->Promise.GetFuture();

        TPendingOperations pendingOperations;

        with_lock (Lock) {
            auto handle = entry->Request->GetHandle();
            auto& handleEntry = WriteDataEntriesByHandle[handle];

            if (handleEntry.PendingWriteDataEntries.empty() &&
                TryAddWriteDataEntryToPersistentQueue(entry.get()))
            {
                entry->Promise.SetValue({});
                entry->Status = EWriteDataEntryStatus::Cached;
                handleEntry.WriteDataEntries.emplace_back(entry.get());
                WriteDataEntriesQueue.PushBack(entry.release());
                HandlesWithUpdates.insert(handle);
            } else {
                entry->Status = EWriteDataEntryStatus::Pending;
                handleEntry.PendingWriteDataEntries.emplace_back(entry.get());
                PendingWriteDataEntriesQueue.PushBack(entry.release());
                RequestFlushAll(pendingOperations);
            }
        }

        StartPendingOperations(pendingOperations);

        return future;
    }

    // should be protected by |Lock|
    void RequestFlush(ui64 handle, TPendingOperations& pendingOperations)
    {
        auto& flushState = FlushStateByHandle[handle];
        if (!flushState) {
            flushState = std::make_unique<TFlushState>(handle);
            pendingOperations.FlushOperations.push_back(flushState.get());
        }
    }

    // should be protected by |Lock|
    void RequestFlushAll(TPendingOperations& pendingOperations)
    {
        for (auto handle: HandlesWithUpdates) {
            auto handleEntryIter = WriteDataEntriesByHandle.find(handle);
            if (handleEntryIter == WriteDataEntriesByHandle.end()) {
                continue;
            }
            auto& handleEntry = handleEntryIter->second;
            if (handleEntry.WriteDataEntries.empty()) {
                continue;
            }
            auto* entry = handleEntry.WriteDataEntries.back();
            if (entry->Status == EWriteDataEntryStatus::Cached) {
                entry->Status = EWriteDataEntryStatus::CachedFlushRequested;
                handleEntry.WriteDataEntriesWithFlushRequested++;
            }
            RequestFlush(handle, pendingOperations);
        }
        HandlesWithUpdates.clear();
    }

    // should be executed outside |Lock|
    void StartPendingOperations(TPendingOperations& pendingOperations)
    {
        // TODO(nasonov): execute asynchronously
        for (auto* flushState: pendingOperations.FlushOperations) {
            PerformFlush(flushState);
        }
        for (auto& promise: pendingOperations.PromisesToSet) {
            promise.SetValue({});
        }

        for (auto& promise: pendingOperations.FinishedPromisesToSet) {
            promise.SetValue();
        }
    }

    // should be protected by |Lock|
    void OnEntriesFlushed(
        ui64 handle,
        size_t entriesCount,
        TPendingOperations& pendingOperations)
    {
        auto handleEntry = WriteDataEntriesByHandle[handle];
        Y_ABORT_UNLESS(entriesCount <= handleEntry.WriteDataEntries.size());

        while (entriesCount-- > 0) {
            auto* entry = handleEntry.WriteDataEntries.front();

            Y_ABORT_UNLESS(
                entry->Status == EWriteDataEntryStatus::Cached ||
                entry->Status == EWriteDataEntryStatus::CachedFlushRequested);

            if (entry->Status == EWriteDataEntryStatus::CachedFlushRequested) {
                Y_ABORT_UNLESS(
                    handleEntry.WriteDataEntriesWithFlushRequested > 0);
                handleEntry.WriteDataEntriesWithFlushRequested--;
            }
            entry->Status = EWriteDataEntryStatus::Finished;

            if (entry->FinishedPromise.Initialized()) {
                pendingOperations.FinishedPromisesToSet.push_back(
                    std::move(entry->FinishedPromise));
            }

            handleEntry.WriteDataEntries.pop_front();
            if (handleEntry.WriteDataEntries.empty() &&
                handleEntry.PendingWriteDataEntries.empty())
            {
                WriteDataEntriesByHandle.erase(handle);
            }
        }

        while (!WriteDataEntriesQueue.Empty() &&
               WriteDataEntriesQueue.Front()->Status ==
                   EWriteDataEntryStatus::Finished)
        {
            WriteDataEntriesQueue.PopFront();
            WriteDataRequestsPersistentQueue.PopFront();
        }

        bool isWriteDataEntriesPersistentQueueFull = false;

        while (!PendingWriteDataEntriesQueue.Empty()) {
            auto* entry = PendingWriteDataEntriesQueue.Front();

            Y_ABORT_UNLESS(
                entry->Status == EWriteDataEntryStatus::Pending ||
                entry->Status == EWriteDataEntryStatus::PendingFlushRequested);

            if (!TryAddWriteDataEntryToPersistentQueue(entry)) {
                isWriteDataEntriesPersistentQueueFull = true;
                break;
            }

            auto handle = entry->Request->GetHandle();
            auto handleEntry = WriteDataEntriesByHandle[handle];

            handleEntry.PendingWriteDataEntries.pop_front();
            handleEntry.WriteDataEntries.push_back(entry);

            PendingWriteDataEntriesQueue.PopFront();
            WriteDataEntriesQueue.PushFront(entry);
            HandlesWithUpdates.insert(handle);

            if (entry->Status == EWriteDataEntryStatus::PendingFlushRequested) {
                Y_ABORT_UNLESS(
                    handleEntry.PendingWriteDataEntriesWithFlushRequested > 0);
                handleEntry.PendingWriteDataEntriesWithFlushRequested--;
                handleEntry.WriteDataEntriesWithFlushRequested++;
                entry->Status = EWriteDataEntryStatus::CachedFlushRequested;
                RequestFlush(handle, pendingOperations);
            } else {
                entry->Status = EWriteDataEntryStatus::Cached;
            }

            if (entry->Promise.Initialized()) {
                pendingOperations.PromisesToSet.push_back(
                    std::move(entry->Promise));
            }
        }

        if (isWriteDataEntriesPersistentQueueFull) {
            RequestFlushAll(pendingOperations);
        }
    }

    // should be protected by |Lock|
    TVector<std::shared_ptr<NProto::TWriteDataRequest>>
    MakeWriteDataRequestsForFlush(
        ui64 handle,
        const TVector<TWriteDataEntryPart>& parts)
    {
        TVector<std::shared_ptr<NProto::TWriteDataRequest>> res;

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
            request->SetFileSystemId(
                parts[partIndex].Source->Request->GetFileSystemId());
            *request->MutableHeaders() =
                parts[partIndex].Source->Request->GetHeaders();
            request->SetHandle(handle);
            request->SetOffset(parts[partIndex].Offset);
            request->SetBuffer(std::move(buffer));

            res.push_back(std::move(request));

            partIndex = rangeEndIndex;
        }

        return res;
    }

    void PerformFlush(TFlushState* flushState)
    {
        if (flushState->WriteRequests.empty()) {
            TVector<TWriteDataEntryPart> parts;

            with_lock (Lock) {
                auto entriesIter =
                    WriteDataEntriesByHandle.find(flushState->Handle);
                if (entriesIter == WriteDataEntriesByHandle.end()) {
                    FlushStateByHandle.erase(flushState->Handle);
                    return;
                }

                auto& handleEntry = entriesIter->second;
                if (handleEntry.WriteDataEntries.empty()) {
                    FlushStateByHandle.erase(flushState->Handle);
                    return;
                }

                parts = CalculateCachedDataPartsToRead(flushState->Handle);
                flushState->AffectedWriteDataEntriesCount =
                    handleEntry.WriteDataEntries.size();
            }

            flushState->WriteRequests =
                MakeWriteDataRequestsForFlush(flushState->Handle, parts);

            if (flushState->WriteRequests.empty()) {
                CompleteFlush(flushState);
                return;
            }
        }

        flushState->RemainingWriteRequestsCount =
            flushState->WriteRequests.size();

        for (size_t i = 0; i < flushState->WriteRequests.size(); i++) {
            auto responseHandler =
                [flushState, i, ptr = weak_from_this()](
                    NThreading::TFuture<NProto::TWriteDataResponse> future)
            {
                auto self = ptr.lock();
                if (!self) {
                    return;
                }

                auto response = future.ExtractValue();

                with_lock (self->Lock) {
                    if (FAILED(response.GetError().GetCode())) {
                        flushState->FailedWriteRequests.push_back(
                            std::move(flushState->WriteRequests[i]));
                    }

                    Y_ABORT_UNLESS(flushState->RemainingWriteRequestsCount > 0);
                    if (--flushState->RemainingWriteRequestsCount > 0) {
                        return;
                    }
                }

                if (!flushState->FailedWriteRequests.empty()) {
                    swap(
                        flushState->WriteRequests,
                        flushState->FailedWriteRequests);
                    flushState->FailedWriteRequests.clear();

                    // TODO(nasonov): retry policy
                    self->Scheduler->Schedule(
                        self->Timer->Now() + TDuration::MilliSeconds(100),
                        [flushState, ptr]()
                        {
                            if (auto self = ptr.lock()) {
                                self->PerformFlush(flushState);
                            }
                        });

                    return;
                }

                self->CompleteFlush(flushState);
            };

            auto callContext = MakeIntrusive<TCallContext>(
                flushState->WriteRequests[i]->GetFileSystemId());
            Session
                ->WriteData(
                    std::move(callContext),
                    flushState->WriteRequests[i])
                .Subscribe(std::move(responseHandler));
        }
    }

    // flushState becomes unusable after this call
    void CompleteFlush(TFlushState *flushState)
    {
        TPendingOperations pendingOperations;

        with_lock (Lock) {
            auto iter = FlushStateByHandle.find(flushState->Handle);
            Y_ABORT_UNLESS(iter != FlushStateByHandle.end());

            auto ptr = std::move(iter->second);
            FlushStateByHandle.erase(iter);

            OnEntriesFlushed(
                flushState->Handle,
                flushState->AffectedWriteDataEntriesCount,
                pendingOperations);
        }
        StartPendingOperations(pendingOperations);
    }

    // should be protected by |Lock|
    TFuture<void> RequestFlushData(
        ui64 handle,
        TPendingOperations& pendingOperations)
    {
        auto handleEntryIter = WriteDataEntriesByHandle.find(handle);
        if (handleEntryIter == WriteDataEntriesByHandle.end()) {
            return NThreading::MakeFuture();
        }
        auto& handleEntry = handleEntryIter->second;

        if (!handleEntry.PendingWriteDataEntries.empty()) {
            auto* entry = handleEntry.PendingWriteDataEntries.back();
            if (entry->Status == EWriteDataEntryStatus::Pending) {
                entry->Status = EWriteDataEntryStatus::PendingFlushRequested;
                handleEntry.PendingWriteDataEntriesWithFlushRequested++;
                RequestFlush(handle, pendingOperations);
            }
            if (!entry->FinishedPromise.Initialized()) {
                entry->FinishedPromise = NewPromise();
            }
            return entry->FinishedPromise.GetFuture();
        }

        if (!handleEntry.WriteDataEntries.empty()) {
            auto* entry = handleEntry.WriteDataEntries.back();
            if (entry->Status == EWriteDataEntryStatus::Cached) {
                entry->Status = EWriteDataEntryStatus::CachedFlushRequested;
                handleEntry.WriteDataEntriesWithFlushRequested++;
                RequestFlush(handle, pendingOperations);
            }
            if (!entry->FinishedPromise.Initialized()) {
                entry->FinishedPromise = NewPromise();
            }
            return entry->FinishedPromise.GetFuture();
        }

        return NThreading::MakeFuture();
    }

    TFuture<void> FlushData(ui64 handle)
    {
        TFuture<void> result;
        TPendingOperations pendingOperations;

        with_lock (Lock) {
            result = RequestFlushData(handle, pendingOperations);
        }
        StartPendingOperations(pendingOperations);
        return result;
    }

    TFuture<void> FlushAllData()
    {
        TVector<TFuture<void>> futures;
        TPendingOperations pendingOperations;

        with_lock (Lock) {
            for (const auto& [handle, _]: WriteDataEntriesByHandle) {
                futures.push_back(RequestFlushData(handle, pendingOperations));
            }
        }
        StartPendingOperations(pendingOperations);
        return NWait::WaitAll(futures);
    }
};

////////////////////////////////////////////////////////////////////////////////

TWriteBackCache::TWriteBackCache() = default;

TWriteBackCache::TWriteBackCache(
        IFileStorePtr session,
        ISchedulerPtr scheduler,
        ITimerPtr timer,
        const TString& filePath,
        ui32 capacityBytes,
        TDuration automaticFlushPeriod)
    : Impl(
        new TImpl(
            session,
            scheduler,
            timer,
            filePath,
            capacityBytes,
            automaticFlushPeriod))
{
    Impl->ScheduleAutomaticFlushIfNeeded();
}

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

}   // namespace NCloud::NFileStore::NFuse
