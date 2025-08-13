#include "write_back_cache_impl.h"

#include "read_write_range_lock.h"

#include <cloud/filestore/libs/service/context.h>

#include <cloud/storage/core/libs/common/file_ring_buffer.h>

#include <library/cpp/threading/future/subscription/wait_all.h>

#include <util/generic/hash_set.h>
#include <util/generic/intrlist.h>
#include <util/generic/mem_copy.h>
#include <util/generic/strbuf.h>
#include <util/generic/vector.h>
#include <util/system/mutex.h>
#include <util/stream/mem.h>

namespace NCloud::NFileStore::NFuse {

using namespace NCloud::NFileStore::NVFS;
using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

// TODO(nasonov): remove this wrapper when TFileRingBuffer supports
// in-place allocation
class TFileRingBuffer: public NCloud::TFileRingBuffer
{
    using NCloud::TFileRingBuffer::TFileRingBuffer;

private:
    ui64 Capacity = 0;

public:
    TFileRingBuffer(const TString& filePath, ui64 capacity)
        : NCloud::TFileRingBuffer(filePath, capacity)
        , Capacity(capacity)
    {}

    ui64 MaxAllocationSize() const
    {
        // Capacity - sizeof(TEntryHeader)
        return Capacity - 8;
    }

    bool AllocateBack(size_t size, char** ptr)
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

    void CommitAllocation(char* ptr)
    {
        Y_UNUSED(ptr);
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TPendingReadDataRequest
{
    TCallContextPtr CallContext;
    std::shared_ptr<NProto::TReadDataRequest> Request;
    TPromise<NProto::TReadDataResponse> Promise;
};

} // namespace

////////////////////////////////////////////////////////////////////////////////

struct TWriteBackCache::TFlushState
{
    TVector<TWriteDataEntryPart> CachedWriteDataParts;
    TVector<std::shared_ptr<NProto::TWriteDataRequest>> WriteRequests;
    TVector<std::shared_ptr<NProto::TWriteDataRequest>> FailedWriteRequests;
    size_t AffectedWriteDataEntriesCount = 0;
    size_t InFlightWriteRequestsCount = 0;
    bool Executing = false;
};

////////////////////////////////////////////////////////////////////////////////

struct TWriteBackCache::THandleState
{
    const ui64 Handle = 0;

    // Entries from TWriteBackCache::TImpl::CachedEntries
    // with statuses Cached and CachedFlushRequested filtered by handle
    // appearing in the same order
    TDeque<TWriteBackCache::TWriteDataEntry*> CachedEntries;

    // Count entries in TWriteBackCache::TImpl::PendingEntries
    // with status Pending filtered by handle
    size_t PendingEntriesCount = 0;

    // Count entries in CachedEntries with status CachedFlushRequested
    size_t EntriesWithFlushRequested = 0;

    // Prevent from concurrent read and write requests with overlapping ranges
    TReadWriteRangeLock RangeLock;

    TFlushState FlushState;

    explicit THandleState(ui64 handle)
        : Handle(handle)
    {}

    bool Empty() const
    {
        return CachedEntries.empty() && PendingEntriesCount == 0 &&
               RangeLock.Empty() && !FlushState.Executing;
    }

    bool ShouldFlush() const
    {
        return EntriesWithFlushRequested > 0;
    }
};

////////////////////////////////////////////////////////////////////////////////

// Accumulate operations to execute after completing the main operation.
// 1. Set promises exposed to the user code outside lock sections.
// 2. Avoid recursion chains in future completion callbacks.
struct TWriteBackCache::TPendingOperations
{
    // Used to prevent recursive calls of ExecutePendingOperations
    bool Executing = false;

    // The flag is set when an element is popped from
    // |TImpl::CachedEntriesPersistentQueue| and free space is increased.
    // We should try to push pending entries to the persistent queue.
    bool ShouldProcessPendingEntries = false;

    // Pending ReadData requests that have acquired |THandleState::RangeLock|
    TVector<TPendingReadDataRequest> ReadData;

    // Pending WriteData requests that have acquired |THandleState::RangeLock|
    TVector<std::unique_ptr<TWriteDataEntry>> WriteData;

    // Flush operations that have been scheduled but not yet started
    TVector<THandleState*> Flush;

    // Report WriteData requests as completed
    TVector<TPromise<NProto::TWriteDataResponse>> WriteDataCompleted;

    // Report FlushData and FlushAll requests as completed
    TVector<TPromise<void>> FlushCompleted;

    bool Empty() const
    {
        return ReadData.empty() && WriteData.empty() && Flush.empty() &&
               WriteDataCompleted.empty() && FlushCompleted.empty();
    }
};

////////////////////////////////////////////////////////////////////////////////

class TWriteBackCache::TImpl final
    : public std::enable_shared_from_this<TImpl>
{
private:
    using TWriteDataEntry = TWriteBackCache::TWriteDataEntry;
    using TWriteDataEntryPart = TWriteBackCache::TWriteDataEntryPart;

    const IFileStorePtr Session;
    const ISchedulerPtr Scheduler;
    const ITimerPtr Timer;
    const TDuration AutomaticFlushPeriod;

    // All fields below should be protected by this lock
    TMutex Lock;

    // Entries with Cached and CachedFlushRequested statuses
    TIntrusiveListWithAutoDelete<TWriteDataEntry, TDelete> CachedEntries;

    // Serialized entries from |CachedEntries| with the same order
    TFileRingBuffer CachedEntriesPersistentQueue;

    // WriteData entries and Flush states grouped by handle
    THashMap<ui64, std::unique_ptr<THandleState>> HandleStates;

    // Entries with Pending status
    TIntrusiveList<TWriteDataEntry> PendingEntries;

    // Handles with new cached WriteData entries since last FlushAll
    THashSet<ui64> HandlesWithNewCachedEntries;

    // Operations to execute after completing the main operation
    TPendingOperations PendingOperations;

public:
    TImpl(
            IFileStorePtr session,
            ISchedulerPtr scheduler,
            ITimerPtr timer,
            const TString& filePath,
            ui32 capacityBytes,
            TDuration automaticFlushPeriod)
        : Session(std::move(session))
        , Scheduler(std::move(scheduler))
        , Timer(std::move(timer))
        , AutomaticFlushPeriod(automaticFlushPeriod)
        , CachedEntriesPersistentQueue(filePath, capacityBytes)
    {
        // File ring buffer should be able to store any valid TWriteDataRequest.
        // Inability to store it will cause this and future requests to remain
        // in the pending queue forever (including requests with smaller size).
        // Should fit 1 MiB of data plus some headers (assume 1 KiB is enough).
        Y_ABORT_UNLESS(
            CachedEntriesPersistentQueue.MaxAllocationSize() >=
            1024 * 1024 + 1016);

        CachedEntriesPersistentQueue.Visit(
            [&](auto checksum, auto serializedRequest)
            {
                auto entry = std::make_unique<TWriteDataEntry>(
                    checksum,
                    serializedRequest);

                if (entry->IsFinished()) {
                    // This may happen when a buffer was corrupted.
                    // We should add this entry to a queue like a normal entry
                    // because there is 1-by-1 correspondence between
                    // CachedEntriesPersistentQueue and CachedEntries.
                    // TODO(nasonov): report it
                } else {
                    auto* handleState =
                        GetOrCreateHandleState(entry->GetHandle());
                    handleState->CachedEntries.emplace_back(entry.get());
                }

                CachedEntries.PushBack(entry.release());
            });
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
    TVector<TWriteDataEntryPart> CalculateCachedDataPartsToRead(
        ui64 handle,
        ui64 startingFromOffset,
        ui64 length)
    {
        auto entriesIter = HandleStates.find(handle);
        if (entriesIter == HandleStates.end()) {
            return {};
        }

        return TUtil::CalculateDataPartsToRead(
            entriesIter->second->CachedEntries,
            startingFromOffset,
            length);
    }

    // should be protected by |Lock|
    TVector<TWriteDataEntryPart> CalculateCachedDataPartsToRead(ui64 handle)
    {
        return CalculateCachedDataPartsToRead(handle, 0, 0);
    }

    // should be protected by |Lock|
    static void ReadDataPart(
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

    TFuture<NProto::TReadDataResponse> ReadData(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TReadDataRequest> request)
    {
        if (request->GetLength() == 0) {
            NProto::TReadDataResponse response;
            *response.MutableError() =
                MakeError(E_ARGUMENT, "ReadData request has zero length");
            return MakeFuture(std::move(response));
        }

        auto handle = request->GetHandle();
        auto offset = request->GetOffset();
        auto end = request->GetOffset() + request->GetLength();

        auto unlocker =
            [ptr = weak_from_this(), handle, offset, end](const auto&)
        {
            if (auto self = ptr.lock()) {
                auto guard = Guard(self->Lock);
                auto* handleState = self->GetHandleState(handle);
                handleState->RangeLock.UnlockRead(offset, end);
                self->DeleteHandleStateIfEmpty(handleState);
                self->ExecutePendingOperations(guard);
            }
        };

        TPendingReadDataRequest pendingRequest = {
            .CallContext = std::move(callContext),
            .Request = std::move(request),
            .Promise = NewPromise<NProto::TReadDataResponse>()};

        auto result = pendingRequest.Promise.GetFuture();
        result.Subscribe(std::move(unlocker));

        auto locker = [ptr = weak_from_this(),
                       pendingRequest = std::move(pendingRequest)]() mutable
        {
            auto self = ptr.lock();
            // Lock action is invoked immediately or from
            // UnlockRead/UnlockWrite calls that can be made only when
            // TImpl is alive
            Y_DEBUG_ABORT_UNLESS(self);
            if (self) {
                self->PendingOperations.ReadData.push_back(
                    std::move(pendingRequest));
            }
        };

        auto guard = Guard(Lock);

        auto* handleState = GetOrCreateHandleState(handle);
        handleState->RangeLock.LockRead(offset, end, std::move(locker));

        ExecutePendingOperations(guard);

        return result;
    }

    TFuture<NProto::TWriteDataResponse> WriteData(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TWriteDataRequest> request)
    {
        if (request->GetFileSystemId().empty()) {
            request->SetFileSystemId(callContext->FileSystemId);
        }

        auto entry = std::make_unique<TWriteDataEntry>(std::move(request));
        if (entry->GetBuffer().Size() == 0) {
            NProto::TWriteDataResponse response;
            *response.MutableError() =
                MakeError(E_ARGUMENT, "WriteData request has zero length");
            return MakeFuture(std::move(response));
        }

        auto serializedSize = entry->GetSerializedSize();

        Y_ABORT_UNLESS(
            serializedSize <= CachedEntriesPersistentQueue.MaxAllocationSize(),
            "Serialized request size %lu is expected to be <= %lu",
            serializedSize,
            CachedEntriesPersistentQueue.MaxAllocationSize());

        auto handle = entry->GetHandle();
        auto offset = entry->Offset();
        auto end = entry->End();

        auto unlocker =
            [ptr = weak_from_this(), handle, offset, end](const auto&)
        {
            if (auto self = ptr.lock()) {
                auto guard = Guard(self->Lock);
                auto* handleState = self->GetHandleState(handle);
                handleState->RangeLock.UnlockWrite(offset, end);
                self->DeleteHandleStateIfEmpty(handleState);
                self->ExecutePendingOperations(guard);
            }
        };

        auto future = entry->GetCachedFuture();
        future.Subscribe(std::move(unlocker));

        auto locker =
            [ptr = weak_from_this(), entry = entry.release()]() mutable
        {
            auto self = ptr.lock();
            // Lock action is invoked immediately or from
            // UnlockRead/UnlockWrite calls that can be made only when
            // TImpl is alive
            Y_ABORT_UNLESS(self);
            if (self) {
                self->PendingOperations.WriteData.push_back(
                    std::unique_ptr<TWriteDataEntry>(entry));
            }
        };

        auto guard = Guard(Lock);

        auto* handleState = GetOrCreateHandleState(handle);
        handleState->RangeLock.LockWrite(offset, end, std::move(locker));

        ExecutePendingOperations(guard);

        return future;
    }

    // should be protected by |Lock|
    static auto MakeWriteDataRequestsForFlush(
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

    TFuture<void> FlushData(ui64 handle)
    {
        auto guard = Guard(Lock);

        auto* handleState = GetHandleStateOrDefault(handle);
        auto future = RequestFlushData(handleState);

        ExecutePendingOperations(guard);

        return future;
    }

    TFuture<void> FlushAllData()
    {
        TVector<TFuture<void>> futures;
        {
            auto guard = Guard(Lock);

            for (const auto& [_, handleState]: HandleStates) {
                futures.push_back(RequestFlushData(handleState.get()));
            }

            ExecutePendingOperations(guard);
        }
        return NWait::WaitAll(futures);
    }

    // should be protected by |Lock|
    void ScheduleFlushIfNeeded(THandleState* handleState)
    {
        if (handleState->ShouldFlush() && !handleState->FlushState.Executing) {
            handleState->FlushState.Executing = true;
            PendingOperations.Flush.push_back(handleState);
        }
    }

    // should be protected by |Lock|
    void ScheduleFlushAll()
    {
        for (auto handle: HandlesWithNewCachedEntries) {
            auto handleEntryIter = HandleStates.find(handle);
            if (handleEntryIter == HandleStates.end()) {
                continue;
            }

            auto* handleState = handleEntryIter->second.get();
            if (handleState->CachedEntries.empty()) {
                continue;
            }

            auto* entry = handleState->CachedEntries.back();
            if (entry->RequestFlush()) {
                handleState->EntriesWithFlushRequested++;
                ScheduleFlushIfNeeded(handleState);
            }
        }

        HandlesWithNewCachedEntries.clear();
    }

    // should be protected by |Lock|
    TFuture<void> RequestFlushData(THandleState* handleState)
    {
        if (handleState == nullptr || handleState->CachedEntries.empty()) {
            return NThreading::MakeFuture();
        }

        auto* entry = handleState->CachedEntries.back();
        if (entry->RequestFlush()) {
            handleState->EntriesWithFlushRequested++;
            ScheduleFlushIfNeeded(handleState);
        }

        return entry->GetFlushedFuture();
    }

private:
    THandleState* GetHandleStateOrDefault(ui64 handle)
    {
        auto it = HandleStates.find(handle);
        return it != HandleStates.end() ? it->second.get() : nullptr;
    }

    THandleState* GetOrCreateHandleState(ui64 handle)
    {
        auto& ptr = HandleStates[handle];
        if (!ptr) {
            ptr = std::make_unique<THandleState>(handle);
        }
        return ptr.get();
    }

    THandleState* GetHandleState(ui64 handle)
    {
        const auto& ptr = HandleStates[handle];
        Y_ABORT_UNLESS(ptr);
        return ptr.get();
    }

    void DeleteHandleStateIfEmpty(THandleState* handleState)
    {
        if (handleState != nullptr && handleState->Empty()) {
            auto erased = HandleStates.erase(handleState->Handle);
            Y_DEBUG_ABORT_UNLESS(erased);
        }
    }

    // NOLINTNEXTLINE(misc-no-recursion)
    void ExecutePendingOperations(TGuard<TMutex>& guard)
    {
        Y_DEBUG_ABORT_UNLESS(guard.WasAcquired());

        // Prevent recursive calls
        if (PendingOperations.Executing) {
            return;
        }

        PendingOperations.Executing = true;

        while (true) {
            if (PendingOperations.ShouldProcessPendingEntries) {
                PendingOperations.ShouldProcessPendingEntries = false;
                ProcessPendingEntries();
            }

            if (PendingOperations.Empty()) {
                PendingOperations.Executing = false;
                return;
            }

            TVector<TPendingReadDataRequest> readData;
            TVector<std::unique_ptr<TWriteDataEntry>> writeData;
            TVector<THandleState*> flush;
            TVector<TPromise<NProto::TWriteDataResponse>> writeDataCompleted;
            TVector<TPromise<void>> flushCompleted;

            swap(readData, PendingOperations.ReadData);
            swap(writeData, PendingOperations.WriteData);
            swap(flush, PendingOperations.Flush);
            swap(writeDataCompleted, PendingOperations.WriteDataCompleted);
            swap(flushCompleted, PendingOperations.FlushCompleted);

            auto unguard = Unguard(guard);

            for (auto& request: readData) {
                StartPendingReadDataRequest(std::move(request));
            }

            for (auto& entry: writeData) {
                StartPendingWriteDataRequest(std::move(entry));
            }

            for (auto* handleState: flush) {
                StartFlushOperation(handleState);
            }

            for (auto& promise: writeDataCompleted) {
                promise.SetValue({});
            }

            for (auto& promise: flushCompleted) {
                promise.SetValue();
            }
        }
    }

    // should be protected by |Lock|
    void ProcessPendingEntries()
    {
        while (!PendingEntries.Empty()) {
            auto* entry = PendingEntries.Front();
            auto serializedSize = entry->GetSerializedSize();

            char* allocationPtr = nullptr;
            bool allocated = CachedEntriesPersistentQueue.AllocateBack(
                serializedSize,
                &allocationPtr);

            if (!allocated) {
                ScheduleFlushAll();
                break;
            }

            Y_ABORT_UNLESS(allocationPtr != nullptr);

            entry->SerializeAndMoveRequestBuffer(
                allocationPtr,
                PendingOperations);

            CachedEntriesPersistentQueue.CommitAllocation(allocationPtr);

            PendingEntries.PopFront();
            CachedEntries.PushBack(entry);

            auto* handleState = GetHandleState(entry->GetHandle());
            handleState->CachedEntries.emplace_back(entry);
            handleState->PendingEntriesCount--;

            HandlesWithNewCachedEntries.insert(entry->GetHandle());
        }
    }

    TVector<TWriteDataEntryPart> CalculateDataPartsToReadAndFillBuffer(
        ui64 handle,
        ui64 startingFromOffset,
        ui64 length,
        TString* buffer)
    {
        *buffer = TString(length, 0);

        with_lock (Lock) {
            auto parts = CalculateCachedDataPartsToRead(
                handle,
                startingFromOffset,
                length);

            for (const auto& part: parts)  {
                ReadDataPart(part, startingFromOffset, buffer);
            }

            return parts;
        }
    }

    static bool IsIntervalFullyCoveredByParts(
        const TVector<TWriteDataEntryPart>& parts,
        ui64 offset,
        ui64 length)
    {
        if (parts.empty() || parts.front().Offset != offset ||
            parts.back().End() != offset + length)
        {
            return false;
        }

        for (size_t i = 1; i < parts.size(); i++) {
            Y_DEBUG_ABORT_UNLESS(parts[i - 1].End() <= parts[i].Offset);
            if (parts[i - 1].End() != parts[i].Offset) {
                return false;
            }
        }

        return true;
    }

    struct TReadDataState
    {
        ui64 StartingFromOffset = 0;
        ui64 Length = 0;
        TString Buffer;
        TVector<TWriteDataEntryPart> Parts;
        TPromise<NProto::TReadDataResponse> Promise;
    };

    void StartPendingReadDataRequest(TPendingReadDataRequest request)
    {
        auto handle = request.Request->GetHandle();

        TReadDataState state;
        state.StartingFromOffset = request.Request->GetOffset();
        state.Length = request.Request->GetLength();
        state.Promise = std::move(request.Promise);

        state.Parts = CalculateDataPartsToReadAndFillBuffer(
            handle,
            state.StartingFromOffset,
            state.Length,
            &state.Buffer);

        if (IsIntervalFullyCoveredByParts(
                state.Parts,
                state.StartingFromOffset,
                state.Length))
        {
            // Serve request from cache
            NProto::TReadDataResponse response;
            response.SetBuffer(std::move(state.Buffer));
            state.Promise.SetValue(std::move(response));
            return;
        }

        // Cache is not sufficient to serve the request - read all the data
        // and merge with the cached parts
        auto future = Session->ReadData(
            std::move(request.CallContext),
            std::move(request.Request));

        future.Subscribe(
            [state = std::move(state)](auto future) mutable
            {
                auto response = future.ExtractValue();

                if (HasError(response)) {
                    state.Promise.SetValue(std::move(response));
                } else {
                    HandleReadDataResponse(
                        std::move(state),
                        std::move(response));
                }
            });
    }

    static void HandleReadDataResponse(
        TReadDataState state,
        NProto::TReadDataResponse response)
    {
        if (response.GetBuffer().empty()) {
            *response.MutableBuffer() = std::move(state.Buffer);
            state.Promise.SetValue(std::move(response));
            return;
        }

        char* responseBufferData =
            response.MutableBuffer()->begin() + response.GetBufferOffset();

        Y_ABORT_UNLESS(
            response.GetBuffer().length() >= response.GetBufferOffset(),
            "reponse buffer length %lu is expected to be >= buffer offset %u",
            response.GetBuffer().length(),
            response.GetBufferOffset());

        auto responseBufferLength =
            response.GetBuffer().length() - response.GetBufferOffset();

        Y_ABORT_UNLESS(
            responseBufferLength <= state.Length,
            "response buffer length %lu is expected to be <= request length %lu",
            responseBufferLength,
            state.Length);

        // Determine if it is better to apply cached data parts on
        // top of the ReadData response or copy non-cached data from
        // the response to the buffer with cached data parts
        bool useResponseBuffer = responseBufferLength == state.Length;
        if (useResponseBuffer) {
            size_t sumPartsSize = 0;
            for (const auto& part: state.Parts) {
                sumPartsSize += part.Length;
            }
            if (sumPartsSize > responseBufferLength / 2) {
                useResponseBuffer = false;
            }
        }

        if (useResponseBuffer) {
            // be careful and don't touch |part.Source| here as it
            // may be already deleted
            for (const auto& part: state.Parts) {
                ui64 offset = part.Offset - state.StartingFromOffset;
                const char* from = state.Buffer.data() + offset;
                char* to = responseBufferData + offset;
                MemCopy(to, from, part.Length);
            }
        } else {
            // Note that responseBufferLength may be < length
            auto parts = TUtil::InvertDataParts(
                state.Parts,
                state.StartingFromOffset,
                responseBufferLength);

            for (const auto& part: parts) {
                ui64 offset = part.Offset - state.StartingFromOffset;
                const char* from = responseBufferData + offset;
                char* to = state.Buffer.begin() + offset;
                MemCopy(to, from, part.Length);
            }
            response.MutableBuffer()->swap(state.Buffer);
            response.ClearBufferOffset();
        }

        state.Promise.SetValue(std::move(response));
    }

    void StartPendingWriteDataRequest(std::unique_ptr<TWriteDataEntry> entry)
    {
        auto serializedSize = entry->GetSerializedSize();
        auto guard = Guard(Lock);

        auto* handleState = GetHandleState(entry->GetHandle());

        if (handleState->PendingEntriesCount > 0)
        {
            handleState->PendingEntriesCount++;
            PendingEntries.PushBack(entry.release());
            return;
        }

        char* allocationPtr = nullptr;
        bool allocated = CachedEntriesPersistentQueue.AllocateBack(
            serializedSize,
            &allocationPtr);

        if (allocated) {
            Y_ABORT_UNLESS(allocationPtr != nullptr);

            entry->SerializeAndMoveRequestBuffer(
                allocationPtr,
                PendingOperations);

            CachedEntriesPersistentQueue.CommitAllocation(allocationPtr);
            handleState->CachedEntries.emplace_back(entry.get());
            HandlesWithNewCachedEntries.insert(handleState->Handle);
            CachedEntries.PushBack(entry.release());
        } else {
            handleState->PendingEntriesCount++;
            PendingEntries.PushBack(entry.release());
            ScheduleFlushAll();
        }
    }

    // |handleState| becomes unusable if the function returns false
    bool PrepareFlushOperation(THandleState* handleState)
    {
        Y_ABORT_UNLESS(handleState->FlushState.Executing);

        if (!handleState->FlushState.WriteRequests.empty()) {
            // Flush is scheduled after WriteData failure
            return true;
        }

        TVector<TWriteDataEntryPart> parts;

        with_lock (Lock) {
            if (!handleState->ShouldFlush()) {
                handleState->FlushState.Executing = false;
                DeleteHandleStateIfEmpty(handleState);
                return false;
            }

            parts = TUtil::CalculateDataPartsToRead(
                handleState->CachedEntries,
                0,
                0);

            // Non-empty CachedEntries cannot produce empty parts
            Y_ABORT_UNLESS(!parts.empty());

            handleState->FlushState.AffectedWriteDataEntriesCount =
                handleState->CachedEntries.size();
        }

        handleState->FlushState.WriteRequests =
            MakeWriteDataRequestsForFlush(handleState->Handle, parts);

        // Non-empty parts cannot produce empty WriteRequests
        Y_ABORT_UNLESS(!handleState->FlushState.WriteRequests.empty());

        return true;
    }

    void StartFlushOperation(THandleState* handleState)
    {
        if (!PrepareFlushOperation(handleState)) {
            return;
        }

        auto& state = handleState->FlushState;

        Y_ABORT_UNLESS(state.Executing);
        Y_ABORT_UNLESS(!state.WriteRequests.empty());
        Y_ABORT_UNLESS(state.FailedWriteRequests.empty());
        Y_ABORT_UNLESS(state.AffectedWriteDataEntriesCount > 0);
        Y_ABORT_UNLESS(state.InFlightWriteRequestsCount == 0);

        state.InFlightWriteRequestsCount = state.WriteRequests.size();

        for (size_t i = 0; i < state.WriteRequests.size(); i++) {
            auto callContext = MakeIntrusive<TCallContext>(
                state.WriteRequests[i]->GetFileSystemId());

            Session->WriteData(std::move(callContext), state.WriteRequests[i])
                .Subscribe(
                    [handleState, i, ptr = weak_from_this()](auto future)
                    {
                        auto self = ptr.lock();
                        if (self) {
                            self->OnWriteDataRequestCompleted(
                                handleState,
                                i,
                                future.GetValue());
                        }
                    });
        }
    }

    void OnWriteDataRequestCompleted(
        THandleState* handleState,
        size_t index,
        const NProto::TWriteDataResponse& response)
    {
        auto& state = handleState->FlushState;

        with_lock (Lock) {
            if (HasError(response)) {
                state.FailedWriteRequests.push_back(
                    std::move(state.WriteRequests[index]));
            }

            Y_ABORT_UNLESS(state.InFlightWriteRequestsCount > 0);
            if (--state.InFlightWriteRequestsCount > 0) {
                return;
            }
        }

        swap(state.WriteRequests, state.FailedWriteRequests);
        state.FailedWriteRequests.clear();

        if (state.WriteRequests.empty()) {
            CompleteFlushOperation(handleState);
        } else {
            ScheduleRetryFlushOperation(handleState);
        }
    }

    // |handleState| becomes unusable after this call
    void CompleteFlushOperation(THandleState* handleState)
    {
        Y_ABORT_UNLESS(handleState->FlushState.Executing);
        Y_ABORT_UNLESS(handleState->FlushState.FailedWriteRequests.empty());
        Y_ABORT_UNLESS(handleState->FlushState.InFlightWriteRequestsCount == 0);

        auto guard = Guard(Lock);

        while (handleState->FlushState.AffectedWriteDataEntriesCount > 0) {
            handleState->FlushState.AffectedWriteDataEntriesCount--;

            auto* entry = handleState->CachedEntries.front();
            handleState->CachedEntries.pop_front();

            if (entry->IsFlushRequested()) {
                Y_ABORT_UNLESS(handleState->EntriesWithFlushRequested > 0);
                handleState->EntriesWithFlushRequested--;
            }

            entry->Finish(PendingOperations);
        }

        // Clear finished entries form the persistent queue
        while (!CachedEntries.Empty() && CachedEntries.Front()->IsFinished())
        {
            CachedEntries.PopFront();
            CachedEntriesPersistentQueue.PopFront();
            PendingOperations.ShouldProcessPendingEntries = true;
        }

        handleState->FlushState.Executing = false;

        if (handleState->ShouldFlush()) {
            ScheduleFlushIfNeeded(handleState);
        } else {
            DeleteHandleStateIfEmpty(handleState);
        }

        ExecutePendingOperations(guard);
    }

    void ScheduleRetryFlushOperation(THandleState* handleState)
    {
        // TODO(nasonov): use retry policy
        Scheduler->Schedule(
            Timer->Now() + TDuration::MilliSeconds(100),
            [handleState, ptr = weak_from_this()]()
            {
                auto self = ptr.lock();
                if (self) {
                    self->StartFlushOperation(handleState);
                }
            });
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

////////////////////////////////////////////////////////////////////////////////

TWriteBackCache::TWriteDataEntry::TWriteDataEntry(
        std::shared_ptr<NProto::TWriteDataRequest> request)
    : Request(std::move(request))
    , CachedPromise(NewPromise<NProto::TWriteDataResponse>())
    , Status(EWriteDataEntryStatus::Pending)
{
    RequestBuffer.swap(*Request->MutableBuffer());
    BufferRef = TStringBuf(RequestBuffer).Skip(Request->GetBufferOffset());
    Request->ClearBufferOffset();
}

TWriteBackCache::TWriteDataEntry::TWriteDataEntry(
    ui32 checksum,
    TStringBuf serializedRequest)
{
    // TODO(nasonov): validate checksum
    Y_UNUSED(checksum);

    TMemoryInput mi(serializedRequest);

    ui32 bufferSize = 0;
    mi.Read(&bufferSize, sizeof(ui32));
    if (bufferSize == 0) {
        // TODO(nasonov): replace this with Y_DEBUG_ABORT_UNLESS when
        // TFileRingBuffer fully supports in-place allocation.
        // Currently this may happen when execution stopped between allocation
        // and Serialization. In future, this can happen only as a result of
        // corruption
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
    BufferRef = TStringBuf(bufferPtr, bufferSize);
    Status = EWriteDataEntryStatus::Cached;
}

size_t TWriteBackCache::TWriteDataEntry::GetSerializedSize() const
{
    return Request->ByteSizeLong() + sizeof(ui32) + BufferRef.size();
}

void TWriteBackCache::TWriteDataEntry::SerializeAndMoveRequestBuffer(
    char* allocationPtr,
    TPendingOperations& pendingOperations)
{
    Y_ABORT_UNLESS(AllocationPtr == nullptr);
    Y_ABORT_UNLESS(allocationPtr != nullptr);
    Y_ABORT_UNLESS(BufferRef.size() <= Max<ui32>());
    Y_ABORT_UNLESS(Status == EWriteDataEntryStatus::Pending);

    AllocationPtr = allocationPtr;

    ui32 bufferSize = static_cast<ui32>(BufferRef.size());
    auto serializedSize = GetSerializedSize();

    TMemoryOutput mo(allocationPtr, serializedSize);
    mo.Write(&bufferSize, sizeof(bufferSize));
    mo.Write(BufferRef);

    Y_ABORT_UNLESS(
        Request->SerializeToArray(mo.Buf(), static_cast<int>(mo.Avail())));

    BufferRef = TStringBuf(allocationPtr + sizeof(ui32), bufferSize);
    RequestBuffer.clear();

    Status = EWriteDataEntryStatus::Cached;

    if (CachedPromise.Initialized()) {
        pendingOperations.WriteDataCompleted.push_back(
            std::move(CachedPromise));
    }
}

void TWriteBackCache::TWriteDataEntry::Finish(
    TPendingOperations& pendingOperations)
{
    Y_ABORT_UNLESS(
        Status == EWriteDataEntryStatus::Cached ||
        Status == EWriteDataEntryStatus::CachedFlushRequested);

    Status = EWriteDataEntryStatus::Flushed;
    BufferRef.Clear();

    if (FlushedPromise.Initialized()) {
        pendingOperations.FlushCompleted.push_back(
            std::move(FlushedPromise));
    }
}

bool TWriteBackCache::TWriteDataEntry::IsFlushRequested() const
{
    return Status == EWriteDataEntryStatus::CachedFlushRequested;
}

bool TWriteBackCache::TWriteDataEntry::RequestFlush()
{
    switch (Status) {
        case EWriteDataEntryStatus::Cached:
            Status = EWriteDataEntryStatus::CachedFlushRequested;
            return true;

        case EWriteDataEntryStatus::CachedFlushRequested:
            return false;

        default:
            Y_ABORT();
    }
}

auto TWriteBackCache::TWriteDataEntry::GetCachedFuture()
    -> TFuture<NProto::TWriteDataResponse>
{
    Y_ABORT_UNLESS(CachedPromise.Initialized());
    return CachedPromise.GetFuture();
}

TFuture<void> TWriteBackCache::TWriteDataEntry::GetFlushedFuture()
{
    if (!FlushedPromise.Initialized()) {
        if (Status == EWriteDataEntryStatus::Flushed) {
            return MakeFuture();
        }
        FlushedPromise = NewPromise();
    }
    return FlushedPromise.GetFuture();
}

}   // namespace NCloud::NFileStore::NFuse
