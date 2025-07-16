#include "write_back_cache_impl.h"

#include <cloud/filestore/libs/service/context.h>

#include <cloud/storage/core/libs/common/file_ring_buffer.h>

#include <library/cpp/threading/future/subscription/wait_all.h>

#include <util/generic/hash.h>
#include <util/generic/intrlist.h>
#include <util/generic/mem_copy.h>
#include <util/generic/strbuf.h>
#include <util/generic/vector.h>
#include <util/system/mutex.h>
#include <util/stream/mem.h>

#include <algorithm>
#include <atomic>

namespace NCloud::NFileStore::NFuse {

using namespace NCloud::NFileStore::NVFS;
using namespace NThreading;

////////////////////////////////////////////////////////////////////////////////

TWriteBackCache::TImpl::TImpl(
        IFileStorePtr session,
        ISchedulerPtr scheduler,
        ITimerPtr timer,
        const TString& filePath,
        ui32 capacityBytes,
        TDuration automaticFlushPeriod)
    : Session(CreateSessionSequencer(std::move(session)))
    , Scheduler(std::move(scheduler))
    , Timer(std::move(timer))
    , AutomaticFlushPeriod(automaticFlushPeriod)
    , WriteDataRequestsQueue(filePath, capacityBytes)
{
    // should fit 1 MiB of data plus some headers (assume 1 KiB is enough)
    Y_ABORT_UNLESS(capacityBytes >= 1024*1024 + 1024);

    WriteDataRequestsQueue.Visit([&] (auto, auto serializedRequest) {
        auto entry = std::make_unique<TWriteDataEntry>(serializedRequest);

        if (entry->GetBuffer().Empty()) {
            // This may happen when a buffer was corrupted
            // Skip this entry from processing
            // TODO(nasonov): report it
            entry->FlushStatus = EFlushStatus::Finished;
        }

        AddWriteDataEntry(std::move(entry));
    });
}

void TWriteBackCache::TImpl::ScheduleAutomaticFlushIfNeeded()
{
    if (!AutomaticFlushPeriod) {
        return;
    }

    Scheduler->Schedule(
        Timer->Now() + AutomaticFlushPeriod,
        [ptr = weak_from_this()] () {
            if (auto self = ptr.lock()) {
                self->FlushAllData().Subscribe(
                    [ptr = self->weak_from_this()] (auto) {
                        if (auto self = ptr.lock()) {
                            self->ScheduleAutomaticFlushIfNeeded();
                        }
                    });
            }
        });
}

void TWriteBackCache::TImpl::AddWriteDataEntry(
    std::unique_ptr<TWriteDataEntry> entry)
{
    WriteDataEntriesByHandle[entry->GetHandle()].emplace_back(
        entry.get());
    WriteDataEntries.PushBack(entry.release());
}

// should be protected by |Lock|
auto TWriteBackCache::TImpl::CalculateCachedDataPartsToRead(
    ui64 handle,
    ui64 startingFromOffset,
    ui64 length) -> TVector<TWriteDataEntryPart>
{
    auto entriesIter = WriteDataEntriesByHandle.find(handle);
    if (entriesIter == WriteDataEntriesByHandle.end()) {
        return {};
    }

    return CalculateDataPartsToRead(
        entriesIter->second,
        startingFromOffset,
        length);
}

// should be protected by |Lock|
auto TWriteBackCache::TImpl::CalculateCachedDataPartsToRead(
    ui64 handle) -> TVector<TWriteDataEntryPart>
{
    return CalculateCachedDataPartsToRead(handle, 0, 0);
}

// should be protected by |Lock|
void TWriteBackCache::TImpl::ReadDataPart(
    TWriteDataEntryPart part,
    ui64 startingFromOffset,
    TString* out)
{
    const char* from = part.Source->GetBuffer().data();
    from += part.OffsetInSource;

    char* to = out->begin();
    to += (part.Offset - startingFromOffset);

    MemCopy(to, from, part.Length);
}

TFuture<NProto::TReadDataResponse> TWriteBackCache::TImpl::ReadData(
    TCallContextPtr callContext,
    std::shared_ptr<NProto::TReadDataRequest> request)
{
    TString buffer(request->GetLength(), 0);
    TVector<TWriteDataEntryPart> parts;

    auto guard = Guard(Lock);

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
            parts = std::move(parts)] (auto future) mutable
        {
            auto response = future.ExtractValue();

            // TODO(svartmetal): handle response error
            Y_ABORT_UNLESS(SUCCEEDED(response.GetError().GetCode()));

            if (response.GetBuffer().empty()) {
                *response.MutableBuffer() = std::move(buffer);
                return response;
            }

            char* responseBufferData =
                response.MutableBuffer()->begin() + response.GetBufferOffset();

            auto responseBufferLength =
                response.GetBuffer().length() - response.GetBufferOffset();

            Y_ABORT_UNLESS(
                responseBufferLength <= length,
                "response buffer length %lu is expected to be <= than %lu",
                responseBufferLength,
                length);

            // Determine if it is better to apply cached data parts on top
            // of the ReadData response or copy non-cached data from the
            // response to the buffer with cached data parts
            bool useResponseBuffer = responseBufferLength == length;
            if (useResponseBuffer) {
                size_t sumPartsSize = 0;
                for (const auto& part: parts) {
                    sumPartsSize += part.Length;
                }
                if (sumPartsSize > responseBufferLength / 2) {
                    useResponseBuffer = false;
                }
            }

            if (useResponseBuffer) {
                // be careful and don't touch |part.Source| here as it may
                // be already deleted
                for (const auto& part: parts) {
                    ui64 offset = part.Offset - startingFromOffset;
                    const char* from = buffer.data() + offset;
                    char* to = responseBufferData + offset;
                    MemCopy(to, from, part.Length);
                }
            } else {
                // Note that responseBufferLength may be < length
                parts = InvertDataParts(
                    parts,
                    startingFromOffset,
                    responseBufferLength);

                for (const auto& part: parts) {
                    ui64 offset = part.Offset - startingFromOffset;
                    const char* from = responseBufferData + offset;
                    char* to = buffer.begin() + offset;
                    MemCopy(to, from, part.Length);
                }
                response.MutableBuffer()->swap(buffer);
                response.ClearBufferOffset();
            }

            return response;
        });
}

TFuture<NProto::TWriteDataResponse> TWriteBackCache::TImpl::WriteData(
    TCallContextPtr callContext,
    std::shared_ptr<NProto::TWriteDataRequest> request)
{
    if (request->GetFileSystemId().empty()) {
        request->SetFileSystemId(callContext->FileSystemId);
    }

    auto entry = std::make_unique<TWriteDataEntry>(std::move(request));
    auto serializedSize = entry->GetSerializedSize();

    Y_ABORT_UNLESS(
        serializedSize <= WriteDataRequestsQueue.MaxAllocationSize(),
        "Serialized request size %lu is expected to be <= %lu",
        serializedSize,
        WriteDataRequestsQueue.MaxAllocationSize());

    auto promise = NewPromise<NProto::TWriteDataResponse>();
    auto future = promise.GetFuture();

    with_lock (Lock) {
        char* cachePtr = nullptr;
        bool allocatedInCache =
            WriteDataRequestsQueue.AllocateBack(serializedSize, &cachePtr);

        if (allocatedInCache) {
            Y_ABORT_UNLESS(cachePtr != nullptr);
            entry->SerializeToCache(cachePtr);
            WriteDataRequestsQueue.CompleteAllocation(cachePtr);
            AddWriteDataEntry(std::move(entry));
            promise.SetValue({});
        } else {
            auto pending = std::make_unique<TPendingWriteDataRequest>(
                std::move(entry),
                std::move(promise));
            PendingWriteDataRequests.PushBack(pending.release());
            FlushAllData(); // flushes asynchronously
        }
    }

    return future;
}

void TWriteBackCache::TImpl::OnEntriesFlushed(
    const TVector<TWriteDataEntry*>& entries)
{
    with_lock (Lock) {
        for (auto* entry: entries) {
            entry->FlushStatus = EFlushStatus::Finished;
        }

        while (!WriteDataEntries.Empty()) {
            auto* entry = WriteDataEntries.Front();
            if (entry->FlushStatus != EFlushStatus::Finished) {
                return;
            }

            WriteDataRequestsQueue.PopFront();

            auto& entries = WriteDataEntriesByHandle[entry->GetHandle()];
            Erase(entries, entry);
            if (entries.empty()) {
                WriteDataEntriesByHandle.erase(entry->GetHandle());
            }

            std::unique_ptr<TWriteDataEntry> holder(entry);
            entry->Unlink();
        }
    }
}

// should be protected by |Lock|
auto TWriteBackCache::TImpl::MakeWriteDataRequestsForFlush(
    ui64 handle) -> TVector<std::shared_ptr<NProto::TWriteDataRequest>>
{
    TVector<std::shared_ptr<NProto::TWriteDataRequest>> res;

    auto parts = CalculateCachedDataPartsToRead(handle);
    EraseIf(
        parts,
        [=] (const auto& part) {
            const auto* entry = part.Source;
            return entry->FlushStatus == EFlushStatus::Started ||
                entry->FlushStatus == EFlushStatus::Finished;
        });

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
            parts[partIndex].Source->GetRequest()->GetFileSystemId());
        *request->MutableHeaders() =
            parts[partIndex].Source->GetRequest()->GetHeaders();
        request->SetHandle(handle);
        request->SetOffset(parts[partIndex].Offset);
        request->SetBuffer(std::move(buffer));

        res.push_back(std::move(request));

        partIndex = rangeEndIndex;
    }

    return res;
}

TFuture<void> TWriteBackCache::TImpl::FlushData(ui64 handle)
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

        const auto alreadyFlushingOrFlushed = [] (const TWriteDataEntry* e)
        {
            return e->FlushStatus == EFlushStatus::Started ||
                e->FlushStatus == EFlushStatus::Finished;
        };

        // TODO(svartmetal): optimise, can be done in O(1)
        if (AllOf(entries, alreadyFlushingOrFlushed)) {
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

        writeRequests = MakeWriteDataRequestsForFlush(handle);

        for (auto* entry: entries) {
            if (alreadyFlushingOrFlushed(entry)) {
                continue;
            }

            entry->FlushStatus = EFlushStatus::Started;
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
        auto callContext = MakeIntrusive<TCallContext>(
            request->GetFileSystemId());
        Session->WriteData(
            std::move(callContext),
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

TFuture<void> TWriteBackCache::TImpl::FlushAllData()
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
        const auto alreadyFlushingOrFlushed = [] (const TWriteDataEntry& e)
        {
            return e.FlushStatus == EFlushStatus::Started ||
                e.FlushStatus == EFlushStatus::Finished;
        };

        // TODO(svartmetal): optimise, can be done in O(1)
        if (AllOf(WriteDataEntries, alreadyFlushingOrFlushed)) {
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
                auto serializedSize = pending->Entry->GetSerializedSize();
                char* cachePtr = nullptr;
                bool allocatedInCache =
                    self->WriteDataRequestsQueue.AllocateBack(
                        serializedSize,
                        &cachePtr);

                if (!allocatedInCache) {
                    self->FlushAllData(); // flushes asynchronously
                    return;
                }

                Y_ABORT_UNLESS(cachePtr != nullptr);
                pending->Entry->SerializeToCache(cachePtr);
                self->WriteDataRequestsQueue.CompleteAllocation(cachePtr);
                self->AddWriteDataEntry(std::move(pending->Entry));

                pending->Promise.SetValue({});

                std::unique_ptr<TPendingWriteDataRequest> holder(pending);
                pending->Unlink();
            }
        }
    });

    return future;
}

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

////////////////////////////////////////////////////////////////////////////////

ui64 TWriteBackCache::TImpl::TFileRingBuffer::MaxAllocationSize() const
{
    return 1024 * 1024;
}

bool TWriteBackCache::TImpl::TFileRingBuffer::AllocateBack(
    size_t size,
    char** ptr)
{
    TString tmp(size, 0);
    if (PushBack(tmp)) {
        *ptr = const_cast<char*>(Back().data());
        return true;
    } else {
        *ptr = nullptr;
        return false;
    }
}

void TWriteBackCache::TImpl::TFileRingBuffer::CompleteAllocation(char* ptr)
{
    Y_UNUSED(ptr);
}

////////////////////////////////////////////////////////////////////////////////

TWriteBackCache::TImpl::TWriteDataEntry::TWriteDataEntry(
        std::shared_ptr<NProto::TWriteDataRequest> request)
    : Request(std::move(request))
{
    RequestBuffer.swap(*Request->MutableBuffer());
    Buffer = TStringBuf(RequestBuffer).Skip(Request->GetBufferOffset());
    Request->ClearBufferOffset();
}

TWriteBackCache::TImpl::TWriteDataEntry::TWriteDataEntry(
    TStringBuf serializedRequest)
{
    TMemoryInput mi(serializedRequest);

    ui32 bufferSize = 0;
    mi.Read(&bufferSize, sizeof(ui32));
    if (bufferSize == 0) {
        // Buffer corruption
        // TODO(nasonov): report and handle
        return;
    }

    const char* bufferPtr = mi.Buf();

    if (mi.Skip(bufferSize) != bufferSize) {
        // Buffer corruption
        // TODO(nasonov): report and handle
        return;
    }

    auto parsedRequest = std::make_shared<NProto::TWriteDataRequest>();
    if (!parsedRequest->ParseFromArray(mi.Buf(), static_cast<int>(mi.Avail()))) {
        // Buffer corruption
        // TODO(nasonov): report and handle
        return;
    }

    Request.swap(parsedRequest);
    Buffer = TStringBuf(bufferPtr, bufferSize);
}

size_t TWriteBackCache::TImpl::TWriteDataEntry::GetSerializedSize() const
{
    return Request->ByteSizeLong() + sizeof(ui32) + Buffer.size();
}

void TWriteBackCache::TImpl::TWriteDataEntry::SerializeToCache(char* cachePtr)
{
    Y_ABORT_UNLESS(cachePtr != nullptr);
    Y_ABORT_UNLESS(Buffer.size() <= Max<ui32>());

    ui32 bufferSize = static_cast<ui32>(Buffer.size());
    auto serializedSize = GetSerializedSize();

    TMemoryOutput mo(cachePtr, serializedSize);
    mo.Write(&bufferSize, sizeof(bufferSize));
    mo.Write(Buffer);

    Y_ABORT_UNLESS(
        Request->SerializeToArray(mo.Buf(), static_cast<int>(mo.Avail())));

    Buffer = TStringBuf(cachePtr + sizeof(ui32), bufferSize);
    RequestBuffer.clear();
}

}   // namespace NCloud::NFileStore::NFuse
