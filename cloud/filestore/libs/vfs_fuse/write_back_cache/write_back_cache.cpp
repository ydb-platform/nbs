#include "write_back_cache_impl.h"

#include "session_sequencer.h"

#include <cloud/filestore/libs/service/context.h>

#include <cloud/storage/core/libs/common/file_ring_buffer.h>

#include <library/cpp/threading/future/subscription/wait_all.h>

#include <util/generic/hash_set.h>
#include <util/generic/intrlist.h>
#include <util/generic/vector.h>
#include <util/stream/mem.h>
#include <util/system/mutex.h>

#include <algorithm>

namespace NCloud::NFileStore::NFuse {

using namespace NCloud::NFileStore::NVFS;
using namespace NThreading;

////////////////////////////////////////////////////////////////////////////////

TWriteBackCache::TImpl::TImpl(
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
    , CachedEntriesPersistentQueue(filePath, capacityBytes)
{
    // should fit 1 MiB of data plus some headers (assume 1 KiB is enough)
    Y_ABORT_UNLESS(capacityBytes >= 1024 * 1024 + 1024);

    CachedEntriesPersistentQueue.Visit(
        [&](auto, TStringBuf serializedRequest)
        {
            auto entry = TWriteDataEntry::DeserializeCachedRequest(
                serializedRequest);
            Y_ABORT_UNLESS(
                entry->GetStatus() == EWriteDataEntryStatus::Cached);

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
TVector<TWriteBackCache::TImpl::TWriteDataEntryPart>
TWriteBackCache::TImpl::CalculateCachedDataPartsToRead(
    ui64 handle,
    ui64 startingFromOffset,
    ui64 length)
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
TVector<TWriteBackCache::TImpl::TWriteDataEntryPart>
TWriteBackCache::TImpl::CalculateCachedDataPartsToRead(ui64 handle)
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
                return MakeFuture(std::move(response));
            }
        }

        // cache is not sufficient to serve the request - read all the data
        // and combine the result with cached parts
        return Session->ReadData(std::move(callContext), request).Apply(
            [handle = request->GetHandle(),
             startingFromOffset = request->GetOffset(),
             length = request->GetLength(),
             buffer = std::move(buffer),
             parts = std::move(parts)] (auto future) mutable
            {
                NProto::TReadDataResponse response = future.ExtractValue();

                if (HasError(response.GetError())) {
                    return response;
                }

                if (response.GetBuffer().empty()) {
                    *response.MutableBuffer() = std::move(buffer);
                    return response;
                }

                char* responseBufferData = response.MutableBuffer()->begin() +
                                           response.GetBufferOffset();

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

                useResponseBuffer = false;

                if (useResponseBuffer) {
                    // be careful and don't touch |part.Source| here as it may
                    // be already deleted
                    for (const auto& part: parts) {
                        ui64 offset = part.Offset - startingFromOffset;
                        const char* from = buffer.data() + offset;
                        char *to = responseBufferData + offset;
                        MemCopy(to, from, part.Length);
                    }
                } else {
                    // Note that responseBufferLength may be < length
                    parts = InvertDataParts(parts, startingFromOffset, responseBufferLength);
                    for (const auto& part: parts) {
                        ui64 offset = part.Offset - startingFromOffset;
                        const char* from = responseBufferData + offset;
                        char *to = buffer.begin() + offset;
                        MemCopy(to, from, part.Length);
                    }
                    response.MutableBuffer()->swap(buffer);
                    response.ClearBufferOffset();
                }

                return response;
            });
    }
}

// should be protected by |Lock|
bool TWriteBackCache::TImpl::TryAddEntryToPersistentQueue(
    TWriteDataEntry* entry,
    TPendingOperations& pendingOperations)
{
    if (entry->GetBuffer().size() >= Max<ui32>()) {
        return false;
    }

    auto size = entry->GetSerializedSize();
    char* ptr = CachedEntriesPersistentQueue.AllocateBack(size);
    if (!ptr) {
        return false;
    }

    entry->MoveToCache(ptr, pendingOperations);
    return true;
}

TFuture<NProto::TWriteDataResponse> TWriteBackCache::TImpl::WriteData(
    TCallContextPtr callContext,
    std::shared_ptr<NProto::TWriteDataRequest> request)
{
    if (request->GetFileSystemId().empty()) {
        request->SetFileSystemId(callContext->FileSystemId);
    }

    auto entry = TWriteDataEntry::CreatePendingRequest(std::move(request));
    auto future = entry->GetFuture();

    TPendingOperations pendingOperations;

    with_lock (Lock) {
        auto& handleEntry = EntriesByHandle[entry->GetHandle()];
        if (handleEntry.PendingEntries.empty() &&
            TryAddEntryToPersistentQueue(entry.get(), pendingOperations))
        {
            handleEntry.CachedEntries.emplace_back(entry.get());
            HandlesWithNewCachedEntries.insert(entry->GetHandle());
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
void TWriteBackCache::TImpl::RequestFlushAll(TPendingOperations& pendingOperations)
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
void TWriteBackCache::TImpl::StartPendingOperations(
    TPendingOperations& pendingOperations)
{
    // TODO(nasonov): execute asynchronously
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

    while (!CachedEntries.Empty() &&
            CachedEntries.Front()->GetStatus() ==
                EWriteDataEntryStatus::Finished)
    {
        CachedEntries.PopFront();
        CachedEntriesPersistentQueue.PopFront();
    }

    bool isWriteDataEntriesPersistentQueueFull = false;

    while (!PendingEntries.Empty()) {
        auto* entry = PendingEntries.Front();

        if (!TryAddEntryToPersistentQueue(entry, pendingOperations)) {
            isWriteDataEntriesPersistentQueueFull = true;
            break;
        }

        auto handleEntry = EntriesByHandle[entry->GetHandle()];

        handleEntry.PendingEntries.pop_front();
        handleEntry.CachedEntries.push_back(entry);

        PendingEntries.PopFront();
        CachedEntries.PushFront(entry);

        HandlesWithNewCachedEntries.insert(handle);

        if (handleEntry.ShouldFlush()) {
            RequestFlush(entry->GetHandle(), pendingOperations);
        }
    }

    if (handleEntry.ShouldFlush()) {
        RequestFlush(handle, pendingOperations);
    }

    if (handleEntry.Empty()) {
        EntriesByHandle.erase(handle);
    }

    if (isWriteDataEntriesPersistentQueueFull) {
        RequestFlushAll(pendingOperations);
    }
}

// should be protected by |Lock|
TVector<std::shared_ptr<NProto::TWriteDataRequest>>
TWriteBackCache::TImpl::MakeWriteDataRequestsForFlush(
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

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<TWriteBackCache::TImpl::TWriteDataEntry>
TWriteBackCache::TImpl::TWriteDataEntry::CreatePendingRequest(
    std::shared_ptr<NProto::TWriteDataRequest> request)
{
    auto res = std::make_unique<TWriteDataEntry>();
    res->Status = EWriteDataEntryStatus::Pending;
    res->Request.swap(request);
    res->RequestBuffer.swap(*res->Request->MutableBuffer());
    res->Buffer =
        TStringBuf(res->RequestBuffer).Skip(res->Request->GetBufferOffset());
    res->Request->ClearBufferOffset();
    return res;
}

std::unique_ptr<TWriteBackCache::TImpl::TWriteDataEntry>
TWriteBackCache::TImpl::TWriteDataEntry::DeserializeCachedRequest(
    TStringBuf serializedRequest)
{
    ui32 bufferSize = 0;
    TMemoryInput mi(serializedRequest);
    mi.Read(&bufferSize, sizeof(ui32));
    mi.Skip(bufferSize);

    auto res = std::make_unique<TWriteDataEntry>();
    auto parsedRequest = std::make_shared<NProto::TWriteDataRequest>();
    if (parsedRequest->ParseFromArray(mi.Buf(), static_cast<int>(mi.Avail()))) {
        res->Status = EWriteDataEntryStatus::Cached;
        res->Buffer =
            TStringBuf(serializedRequest.Data() + sizeof(ui32), bufferSize);
        res->Request.swap(parsedRequest);
    }

    return res;
}

size_t TWriteBackCache::TImpl::TWriteDataEntry::GetSerializedSize() const
{
    return Request->ByteSizeLong() + sizeof(ui32) + Buffer.size();
}

void TWriteBackCache::TImpl::TWriteDataEntry::MoveToCache(
    char* data,
    TWriteBackCache::TImpl::TPendingOperations& pendingOperations)
{
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

    Y_ABORT_UNLESS(Buffer.size() < Max<ui32>());

    ui32 bufferSize = static_cast<ui32>(Buffer.size());
    auto serializedSize = GetSerializedSize();

    TMemoryOutput mo(data, serializedSize);
    mo.Write(&bufferSize, sizeof(bufferSize));
    // TODO(nasonov): copy Buffer to cache outside lock
    mo.Write(Buffer);

    Y_ABORT_UNLESS(
        Request->SerializeToArray(mo.Buf(), static_cast<int>(mo.Avail())));

    Buffer = TStringBuf(data + sizeof(ui32), bufferSize);
    RequestBuffer.clear();

    if (Promise.Initialized()) {
        pendingOperations.PromisesToSet.push_back(std::move(Promise));
    }
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

NThreading::TFuture<NProto::TWriteDataResponse>
TWriteBackCache::TImpl::TWriteDataEntry::GetFuture()
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

NThreading::TFuture<void>
TWriteBackCache::TImpl::TWriteDataEntry::GetFinishedFuture()
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

TWriteBackCache::TImpl::TWriteDataEntry*
TWriteBackCache::TImpl::THandleEntry::GetLastEntry() const
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
