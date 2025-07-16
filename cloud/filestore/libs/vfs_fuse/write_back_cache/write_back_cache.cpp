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
    , CachedEntriesPersistentQueue(filePath, capacityBytes)
{
    // should fit 1 MiB of data plus some headers (assume 1 KiB is enough)
    Y_ABORT_UNLESS(capacityBytes >= 1024*1024 + 1024);

    CachedEntriesPersistentQueue.Visit([&](auto, TStringBuf serializedRequest) {
        auto entry = std::make_unique<TWriteDataEntry>(serializedRequest);

        if (entry->GetStatus() == EWriteDataEntryStatus::Invalid) {
            // This may happen when a buffer was corrupted.
            // We should add this entry to a queue like a normal entry because
            // there is 1-by-1 correspondence between
            // CachedEntriesPersistentQueue and CachedEntries.
            // TODO(nasonov): report it
        }

        auto& handleEntry = EntriesByHandle[entry->GetHandle()];
        handleEntry.CachedEntries.emplace_back(entry.get());
        CachedEntries.PushBack(entry.release());
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

// should be protected by |Lock|
auto TWriteBackCache::TImpl::CalculateCachedDataPartsToRead(
    ui64 handle,
    ui64 startingFromOffset,
    ui64 length) -> TVector<TWriteDataEntryPart>
{
    auto entriesIter = EntriesByHandle.find(handle);
    if (entriesIter == EntriesByHandle.end()) {
        return {};
    }

    return CalculateDataPartsToRead(
        entriesIter->second.CachedEntries,
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
    TPendingOperations pendingOperations;
    TFuture<NProto::TReadDataResponse> readDataFuture;

    auto guard = Guard(Lock);

    parts = CalculateCachedDataPartsToRead(
        request->GetHandle(),
        request->GetOffset(),
        request->GetLength());

    // Pin the buffers so they can be safely referenced outside lock section
    for (auto& part: parts) {
        part.Source->IncrementRefCount();
    }

    bool sufficient = true;

    if (!parts.empty() && parts.front().Offset == request->GetOffset() &&
        parts.back().End() == request->GetOffset() + request->GetLength())
    {
        for (size_t i = 1; i < parts.size(); i++) {
            Y_DEBUG_ABORT_UNLESS(parts[i-1].End() <= parts[i].Offset);

            if (parts[i-1].End() != parts[i].Offset) {
                sufficient = false;
                break;
            }
        }
    } else {
        sufficient = false;
    }

    // It is important to initiate ReadData operation in the same Lock section
    // in order to ensure that no writes will be done into the read region.
    // (this is managed by TSessionSequencer)
    if (!sufficient) {
        readDataFuture = Session->ReadData(std::move(callContext), request);
    }

    {
        // Copy data from buffers without lock
        auto unguard = Unguard(guard);

        for (const auto& part: parts) {
            ReadDataPart(part, request->GetOffset(), &buffer);
        }
    }

    // Unpin buffers
    for (const auto& part: parts) {
        part.Source->DecrementRefCount();
    }
    ClearFinishedEntries(pendingOperations);

    guard.Release();

    StartPendingOperations(pendingOperations);

    if (sufficient) {
        // serve request from cache
        NProto::TReadDataResponse response;
        response.SetBuffer(std::move(buffer));
        return MakeFuture(std::move(response));
    }

    // cache is not sufficient to serve the request - read all the data
    // and combine the result with cached parts
    return readDataFuture.Apply(
        [handle = request->GetHandle(),
         startingFromOffset = request->GetOffset(),
         length = request->GetLength(),
         buffer = std::move(buffer),
         parts = std::move(parts)](auto future) mutable
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
        serializedSize <= CachedEntriesPersistentQueue.MaxAllocationSize(),
        "Serialized request size %lu is expected to be <= %lu",
        serializedSize,
        CachedEntriesPersistentQueue.MaxAllocationSize());

    auto future = entry->GetFuture();

    TPendingOperations pendingOperations;

    with_lock (Lock) {
        auto& handleEntry = EntriesByHandle[entry->GetHandle()];
        char* cachePtr = nullptr;
        bool allocatedInCache = handleEntry.PendingEntries.empty() &&
            CachedEntriesPersistentQueue.AllocateBack(serializedSize, &cachePtr);

        if (allocatedInCache) {
            Y_ABORT_UNLESS(cachePtr != nullptr);
            entry->LinkWithCache(cachePtr, pendingOperations);
            handleEntry.CachedEntries.emplace_back(entry.get());
            CachedEntries.PushBack(entry.release());
        } else {
            handleEntry.PendingEntries.emplace_back(entry.get());
            PendingEntries.PushBack(entry.release());
            RequestFlushAll(pendingOperations);
        }
    }

    StartPendingOperations(pendingOperations);

    return future;
}

TFuture<void> TWriteBackCache::TImpl::FlushData(ui64 handle)
{
    TFuture<void> result;
    TPendingOperations pendingOperations;

    with_lock (Lock) {
        result = RequestFlushData(handle, pendingOperations);
    }
    StartPendingOperations(pendingOperations);
    return result;
}

TFuture<void> TWriteBackCache::TImpl::FlushAllData()
{
    TVector<TFuture<void>> futures;
    TPendingOperations pendingOperations;

    with_lock (Lock) {
        for (const auto& [handle, _]: EntriesByHandle) {
            futures.push_back(RequestFlushData(handle, pendingOperations));
        }
    }
    StartPendingOperations(pendingOperations);
    return NWait::WaitAll(futures);
}

// should be protected by |Lock|
void TWriteBackCache::TImpl::RequestFlush(
    ui64 handle,
    TPendingOperations& pendingOperations)
{
    auto& flushState = FlushStateByHandle[handle];
    if (!flushState) {
        flushState = std::make_unique<TFlushOperation>(handle);
        pendingOperations.FlushOperations.push_back(flushState.get());
    }
}

// should be protected by |Lock|
void TWriteBackCache::TImpl::RequestFlushAll(
    TPendingOperations& pendingOperations)
{
    for (auto handle: HandlesWithNewCachedEntries) {
        auto handleEntryIter = EntriesByHandle.find(handle);
        if (handleEntryIter == EntriesByHandle.end()) {
            continue;
        }
        auto& handleEntry = handleEntryIter->second;
        if (handleEntry.CachedEntries.empty()) {
            continue;
        }
        auto* entry = handleEntry.CachedEntries.back();
        if (entry->RequestFlush()) {
            handleEntry.EntriesWithFlushRequested++;
        }
        RequestFlush(handle, pendingOperations);
    }
    HandlesWithNewCachedEntries.clear();
}

// should be executed outside |Lock|
// TODO(nasonov): research possibity to execute asynchonously
void TWriteBackCache::TImpl::StartPendingOperations(
    TPendingOperations& pendingOperations)
{
    if (!pendingOperations.EntriesMovingToCache.empty()) {
        for (auto* entry: pendingOperations.EntriesMovingToCache) {
            entry->SerializeToCache();
        }
        with_lock(Lock) {
            for (auto* entry: pendingOperations.EntriesMovingToCache) {
                auto* ptr = entry->CompleteMovingToCache(pendingOperations);
                CachedEntriesPersistentQueue.CompleteAllocation(ptr);

                auto& handleEntry = EntriesByHandle[entry->GetHandle()];
                if (handleEntry.ShouldFlush()) {
                    RequestFlush(entry->GetHandle(), pendingOperations);
                }
                HandlesWithNewCachedEntries.insert(entry->GetHandle());
            }
        }
    }

    for (auto* flushState: pendingOperations.FlushOperations) {
        flushState->Start(this);
    }

    for (auto& promise: pendingOperations.PromisesToSet) {
        promise.SetValue({});
    }

    for (auto& promise: pendingOperations.FinishedPromisesToSet) {
        promise.SetValue();
    }
}

// should be protected by |Lock|
void TWriteBackCache::TImpl::OnEntriesFlushed(
    ui64 handle,
    size_t entriesCount,
    TPendingOperations& pendingOperations)
{
    auto handleEntry = EntriesByHandle[handle];
    Y_ABORT_UNLESS(entriesCount <= handleEntry.CachedEntries.size());

    while (entriesCount-- > 0) {
        auto* entry = handleEntry.CachedEntries.front();
        handleEntry.CachedEntries.pop_front();

        if (entry->FlushRequested()) {
            Y_ABORT_UNLESS(handleEntry.EntriesWithFlushRequested > 0);
            handleEntry.EntriesWithFlushRequested--;
        }

        entry->Finish(pendingOperations);
    }

    ClearFinishedEntries(pendingOperations);

    if (handleEntry.Empty()) {
        EntriesByHandle.erase(handle);
    }

    if (handleEntry.ShouldFlush()) {
        RequestFlush(handle, pendingOperations);
    }
}

void TWriteBackCache::TImpl::ClearFinishedEntries(
    TPendingOperations& pendingOperations)
{
    bool hasClearedEntries = false;

    while (!CachedEntries.Empty() && CachedEntries.Front()->CanBeCleared())
    {
        CachedEntries.PopFront();
        CachedEntriesPersistentQueue.PopFront();
        hasClearedEntries = true;
    }

    if (!hasClearedEntries) {
        return;
    }

    bool isWriteDataEntriesPersistentQueueFull = false;

    while (!PendingEntries.Empty()) {
        auto* entry = PendingEntries.Front();
        char* cachePtr = nullptr;
        bool allocatedInCache = CachedEntriesPersistentQueue.AllocateBack(
            entry->GetSerializedSize(), &cachePtr);

        if (!allocatedInCache) {
            isWriteDataEntriesPersistentQueueFull = true;
            break;
        }

        Y_ABORT_UNLESS(cachePtr != nullptr);

        entry->LinkWithCache(cachePtr, pendingOperations);

        auto handleEntry = EntriesByHandle[entry->GetHandle()];

        handleEntry.PendingEntries.pop_front();
        handleEntry.CachedEntries.push_back(entry);

        PendingEntries.PopFront();
        CachedEntries.PushFront(entry);

        HandlesWithNewCachedEntries.insert(entry->GetHandle());
    }

    if (isWriteDataEntriesPersistentQueueFull) {
        RequestFlushAll(pendingOperations);
    }
}

// should be protected by |Lock|
auto TWriteBackCache::TImpl::MakeWriteDataRequestsForFlush(
    ui64 handle,
    const TVector<TWriteDataEntryPart>& parts)
    -> TVector<std::shared_ptr<NProto::TWriteDataRequest>>
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

// should be protected by |Lock|
TFuture<void> TWriteBackCache::TImpl::RequestFlushData(
    ui64 handle,
    TPendingOperations& pendingOperations)
{
    auto handleEntryIter = EntriesByHandle.find(handle);
    if (handleEntryIter == EntriesByHandle.end()) {
        return NThreading::MakeFuture();
    }
    auto& handleEntry = handleEntryIter->second;

    auto* entry = handleEntry.GetLastEntry();
    if (entry == nullptr) {
        return NThreading::MakeFuture();
    }

    if (entry->RequestFlush()) {
        handleEntry.EntriesWithFlushRequested++;
        RequestFlush(handle, pendingOperations);
    }

    return entry->GetFinishedFuture();
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
    , Status(EWriteDataEntryStatus::Pending)
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
    Status = EWriteDataEntryStatus::Cached;
}

// should be protected by |Lock|
void TWriteBackCache::TImpl::TWriteDataEntry::IncrementRefCount()
{
    RefCount++;
}

// should be protected by |Lock|
void TWriteBackCache::TImpl::TWriteDataEntry::DecrementRefCount()
{
    Y_ABORT_UNLESS(--RefCount >= 0);
    if (RefCount == 0 && CachePtr != nullptr && !RequestBuffer.Empty()) {
        Buffer = { CachePtr + sizeof(ui32), RequestBuffer.Size() };
        RequestBuffer.clear();
    }
}

size_t TWriteBackCache::TImpl::TWriteDataEntry::GetSerializedSize() const
{
    return Request->ByteSizeLong() + sizeof(ui32) + Buffer.size();
}

// should be protected by |Lock|
void TWriteBackCache::TImpl::TWriteDataEntry::LinkWithCache(
    char* cachePtr,
    TPendingOperations& pendingOperations)
{
    Y_ABORT_UNLESS(CachePtr == nullptr);
    Y_ABORT_UNLESS(cachePtr != nullptr);

    CachePtr = cachePtr;

    ui32 bufferSize = 0;
    TMemoryOutput mo(cachePtr, sizeof(bufferSize));
    mo.Write(&bufferSize, sizeof(bufferSize));

    pendingOperations.EntriesMovingToCache.push_back(this);

    IncrementRefCount();
}

void TWriteBackCache::TImpl::TWriteDataEntry::SerializeToCache()
{
    Y_ABORT_UNLESS(CachePtr != nullptr);
    Y_ABORT_UNLESS(Buffer.size() <= Max<ui32>());

    ui32 bufferSize = static_cast<ui32>(Buffer.size());
    auto serializedSize = GetSerializedSize();

    TMemoryOutput mo(CachePtr, serializedSize);
    mo.Write(&bufferSize, sizeof(bufferSize));
    mo.Write(Buffer);

    Y_ABORT_UNLESS(
        Request->SerializeToArray(mo.Buf(), static_cast<int>(mo.Avail())));
}

// should be protected by |Lock|
char* TWriteBackCache::TImpl::TWriteDataEntry::CompleteMovingToCache(
    TPendingOperations& pendingOperations)
{
    Y_ABORT_UNLESS(CachePtr != nullptr);

    switch (Status) {
        case EWriteDataEntryStatus::Pending:
            Status = EWriteDataEntryStatus::Cached;
            break;
        case EWriteDataEntryStatus::PendingFlushRequested:
            Status = EWriteDataEntryStatus::CachedFlushRequested;
            break;
        default:
            Y_ABORT();
    }

    if (Promise.Initialized()) {
        pendingOperations.PromisesToSet.push_back(std::move(Promise));
    }

    DecrementRefCount();

    return CachePtr;
}

void TWriteBackCache::TImpl::TWriteDataEntry::Finish(
    TWriteBackCache::TImpl::TPendingOperations& pendingOperations)
{
    Y_ABORT_UNLESS(
        Status == EWriteDataEntryStatus::Cached ||
        Status == EWriteDataEntryStatus::CachedFlushRequested);

    Status = EWriteDataEntryStatus::Finished;
    Buffer.Clear();

    if (FinishedPromise.Initialized()) {
        pendingOperations.FinishedPromisesToSet.push_back(
            std::move(FinishedPromise));
    }
}

bool TWriteBackCache::TImpl::TWriteDataEntry::FlushRequested() const
{
    return Status == EWriteDataEntryStatus::CachedFlushRequested ||
           Status == EWriteDataEntryStatus::PendingFlushRequested;
}

bool TWriteBackCache::TImpl::TWriteDataEntry::RequestFlush()
{
    switch (Status) {
        case EWriteDataEntryStatus::Cached:
            Status = EWriteDataEntryStatus::CachedFlushRequested;
            return true;

        case EWriteDataEntryStatus::Pending:
            Status = EWriteDataEntryStatus::PendingFlushRequested;
            return true;

        case EWriteDataEntryStatus::CachedFlushRequested:
        case EWriteDataEntryStatus::PendingFlushRequested:
            return false;

        default:
            Y_ABORT();
    }
}

auto TWriteBackCache::TImpl::TWriteDataEntry::GetFuture()
    -> NThreading::TFuture<NProto::TWriteDataResponse>
{
    if (!Promise.Initialized()) {
        switch (Status) {
            case EWriteDataEntryStatus::Cached:
            case EWriteDataEntryStatus::CachedFlushRequested:
            case EWriteDataEntryStatus::Finished:
                return MakeFuture<NProto::TWriteDataResponse>({});

            case EWriteDataEntryStatus::Pending:
            case EWriteDataEntryStatus::PendingFlushRequested:
                Promise = NewPromise<NProto::TWriteDataResponse>();
                break;

            default:
                Y_ABORT();
        }
    }
    return Promise.GetFuture();
}

auto TWriteBackCache::TImpl::TWriteDataEntry::GetFinishedFuture()
    -> NThreading::TFuture<void>
{
    if (!FinishedPromise.Initialized()) {
        if (Status == EWriteDataEntryStatus::Finished) {
            return MakeFuture();
        }
        FinishedPromise = NewPromise();
    }
    return FinishedPromise.GetFuture();
}

////////////////////////////////////////////////////////////////////////////////

bool TWriteBackCache::TImpl::THandleEntry::Empty() const
{
    return CachedEntries.empty() && PendingEntries.empty();
}

bool TWriteBackCache::TImpl::THandleEntry::ShouldFlush() const
{
    return !CachedEntries.empty() && EntriesWithFlushRequested > 0;
}

auto TWriteBackCache::TImpl::THandleEntry::GetLastEntry() const
    -> TWriteBackCache::TImpl::TWriteDataEntry*
{
    if (!PendingEntries.empty()) {
        return PendingEntries.back();
    }
    if (!CachedEntries.empty()) {
        return CachedEntries.back();
    }
    return nullptr;
}

////////////////////////////////////////////////////////////////////////////////

void TWriteBackCache::TImpl::TFlushOperation::Start(TImpl* impl)
{
    if (!Prepare(impl)) {
        return;
    }

    Y_ABORT_UNLESS(!WriteRequests.empty());

    RemainingWriteRequestsCount = WriteRequests.size();

    for (size_t i = 0; i < WriteRequests.size(); i++) {
        auto callContext =
            MakeIntrusive<TCallContext>(WriteRequests[i]->GetFileSystemId());

        impl->Session->WriteData(std::move(callContext), WriteRequests[i])
            .Subscribe(
                [this, i, ptr = impl->weak_from_this()](auto future)
                {
                    auto impl = ptr.lock();
                    if (impl) {
                        WriteDataRequestCompleted(
                            impl.get(),
                            i,
                            future.GetValue());
                    }
                });
    }
}

bool TWriteBackCache::TImpl::TFlushOperation::Prepare(TImpl* impl)
{
    if (!WriteRequests.empty()) {
        return true;
    }

    TVector<TWriteDataEntryPart> parts;

    with_lock (impl->Lock) {
        auto entriesIter = impl->EntriesByHandle.find(Handle);
        if (entriesIter == impl->EntriesByHandle.end()) {
            impl->FlushStateByHandle.erase(Handle);
            return false;
        }

        auto& handleEntry = entriesIter->second;
        if (handleEntry.CachedEntries.empty()) {
            impl->FlushStateByHandle.erase(Handle);
            return false;
        }

        parts = impl->CalculateCachedDataPartsToRead(Handle);
        AffectedWriteDataEntriesCount = handleEntry.CachedEntries.size();
    }

    WriteRequests = impl->MakeWriteDataRequestsForFlush(Handle, parts);

    if (WriteRequests.empty()) {
        Complete(impl);
        return false;
    }

    return true;
}

void TWriteBackCache::TImpl::TFlushOperation::WriteDataRequestCompleted(
    TImpl* impl,
    size_t index,
    const NProto::TWriteDataResponse& response)
{
    with_lock (impl->Lock) {
        if (FAILED(response.GetError().GetCode())) {
            FailedWriteRequests.push_back(
                std::move(WriteRequests[index]));
        }

        Y_ABORT_UNLESS(RemainingWriteRequestsCount > 0);
        if (--RemainingWriteRequestsCount > 0) {
            return;
        }
    }

    if (!FailedWriteRequests.empty()) {
        swap(WriteRequests, FailedWriteRequests);
        FailedWriteRequests.clear();
        ScheduleRetry(impl);
        return;
    }

    Complete(impl);
}

void TWriteBackCache::TImpl::TFlushOperation::ScheduleRetry(TImpl* impl)
{
    // TODO(nasonov): use retry policy
    impl->Scheduler->Schedule(
        impl->Timer->Now() + TDuration::MilliSeconds(100),
        [this, ptr = impl->weak_from_this()]()
        {
            auto self = ptr.lock();
            if (self) {
                Start(self.get());
            }
        });
}

// |this| becomes unusable after this call
void TWriteBackCache::TImpl::TFlushOperation::Complete(TImpl* impl)
{
    TPendingOperations pendingOperations;

    with_lock (impl->Lock) {
        auto iter = impl->FlushStateByHandle.find(Handle);
        Y_ABORT_UNLESS(iter != impl->FlushStateByHandle.end());

        // Keep alive for |this|
        auto ptr = std::move(iter->second);
        impl->FlushStateByHandle.erase(iter);

        impl->OnEntriesFlushed(
            Handle,
            AffectedWriteDataEntriesCount,
            pendingOperations);
    }

    impl->StartPendingOperations(pendingOperations);
}

}   // namespace NCloud::NFileStore::NFuse
