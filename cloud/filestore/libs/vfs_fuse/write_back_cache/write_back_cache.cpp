#include "write_back_cache_impl.h"

#include "read_write_range_lock.h"

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

#include <atomic>

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

// TODO(nasonov): remove after rewriting Flush logic
// Will be referenced directly via
// THashMap<ui64, std::unique_ptr<THandleState>> HandleStates;
class TGlobalReadWriteRangeLock
    : public std::enable_shared_from_this<TGlobalReadWriteRangeLock>
{
public:
    using TAction = std::function<void()>;

private:
    TMutex Lock;
    THashMap<ui64, TReadWriteRangeLock> RangeLockByHandle;
    TVector<TAction> PendingActions;

public:
    void LockRead(ui64 handle, ui64 begin, ui64 end, TAction action)
    {
        TVector<TAction> pendingActions;

        with_lock (Lock) {
            auto& rangeLock = RangeLockByHandle[handle];
            rangeLock.LockRead(
                begin,
                end,
                [ptr = weak_from_this(), action = std::move(action)]() mutable
                {
                    if (auto self = ptr.lock()) {
                        self->PendingActions.push_back(std::move(action));
                    }
                });
            if (rangeLock.Empty()) {
                RangeLockByHandle.erase(handle);
            }
            swap(pendingActions, PendingActions);
        }

        for (const auto& action: pendingActions) {
            action();
        }
    }

    void LockWrite(ui64 handle, ui64 begin, ui64 end, TAction action)
    {
        TVector<TAction> pendingActions;

        with_lock (Lock) {
            auto& rangeLock = RangeLockByHandle[handle];
            rangeLock.LockWrite(
                begin,
                end,
                [ptr = weak_from_this(), action = std::move(action)]() mutable
                {
                    if (auto self = ptr.lock()) {
                        self->PendingActions.push_back(std::move(action));
                    }
                });
            if (rangeLock.Empty()) {
                RangeLockByHandle.erase(handle);
            }
            swap(pendingActions, PendingActions);
        }

        for (const auto& action: pendingActions) {
            action();
        }
    }

    void UnlockRead(ui64 handle, ui64 begin, ui64 end)
    {
        TVector<TAction> pendingActions;

        with_lock (Lock) {
            auto& rangeLock = RangeLockByHandle[handle];
            rangeLock.UnlockRead(begin, end);
            if (rangeLock.Empty()) {
                RangeLockByHandle.erase(handle);
            }
            swap(pendingActions, PendingActions);
        }

        for (const auto& action: pendingActions) {
            action();
        }
    }

    void UnlockWrite(ui64 handle, ui64 begin, ui64 end)
    {
        TVector<TAction> pendingActions;

        with_lock (Lock) {
            auto& rangeLock = RangeLockByHandle[handle];
            rangeLock.UnlockWrite(begin, end);
            if (rangeLock.Empty()) {
                RangeLockByHandle.erase(handle);
            }
            swap(pendingActions, PendingActions);
        }

        for (const auto& action: pendingActions) {
            action();
        }
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

    struct TPendingWriteDataRequest
        : public TIntrusiveListItem<TPendingWriteDataRequest>
    {
        std::unique_ptr<TWriteDataEntry> Entry;
        TPromise<NProto::TWriteDataResponse> Promise;

        TPendingWriteDataRequest(
                std::unique_ptr<TWriteDataEntry> entry,
                TPromise<NProto::TWriteDataResponse> promise)
            : Entry(std::move(entry))
            , Promise(std::move(promise))
        {}
    };

    std::shared_ptr<TGlobalReadWriteRangeLock> RangeLock;

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
            const TString& filePath,
            ui32 capacityBytes,
            TDuration automaticFlushPeriod)
        : Session(std::move(session))
        , Scheduler(std::move(scheduler))
        , Timer(std::move(timer))
        , AutomaticFlushPeriod(automaticFlushPeriod)
        , RangeLock(std::make_shared<TGlobalReadWriteRangeLock>())
        , WriteDataRequestsQueue(filePath, capacityBytes)
    {
        // File ring buffer should be able to store any valid TWriteDataRequest.
        // Inability to store it will cause this and future requests to remain
        // in the pending queue forever (including requests with smaller size).
        // Should fit 1 MiB of data plus some headers (assume 1 KiB is enough).
        Y_ABORT_UNLESS(
            WriteDataRequestsQueue.MaxAllocationSize() >= 1024 * 1024 + 1016);

        WriteDataRequestsQueue.Visit([&] (auto checksum, auto serializedRequest)
        {
            auto entry = std::make_unique<TWriteDataEntry>(
                checksum,
                serializedRequest);

            if (entry->GetBuffer().Empty()) {
                // This may happen when a buffer was corrupted
                // Skip this entry from processing
                // TODO(nasonov): report it
                entry->FlushStatus = EFlushStatus::Finished;
            }

            AddWriteDataEntry(std::move(entry));
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

    void AddWriteDataEntry(std::unique_ptr<TWriteDataEntry> entry)
    {
        WriteDataEntriesByHandle[entry->GetHandle()].emplace_back(
            entry.get());
        WriteDataEntries.PushBack(entry.release());
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

        return TUtil::CalculateDataPartsToRead(
            entriesIter->second,
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
                self->RangeLock->UnlockRead(handle, offset, end);
            }
        };

        TPendingReadDataRequest pendingRequest = {
            .CallContext = std::move(callContext),
            .Request = std::move(request),
            .Promise = NewPromise<NProto::TReadDataResponse>()};

        auto result = pendingRequest.Promise.GetFuture();
        result.Subscribe(std::move(unlocker));

        RangeLock->LockRead(handle, offset, end,
            [ptr = weak_from_this(),
             pendingRequest = std::move(pendingRequest)]() mutable
            {
                auto self = ptr.lock();
                // Lock action is invoked immediately or from
                // UnlockRead/UnlockWrite calls that can be made only when
                // TImpl is alive
                Y_DEBUG_ABORT_UNLESS(self);
                if (self) {
                    self->StartPendingReadDataRequest(
                        std::move(pendingRequest));
                }
            });

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
            serializedSize <= WriteDataRequestsQueue.MaxAllocationSize(),
            "Serialized request size %lu is expected to be <= %lu",
            serializedSize,
            WriteDataRequestsQueue.MaxAllocationSize());

        auto unlocker = [ptr = weak_from_this(),
                         handle = entry->GetHandle(),
                         offset = entry->Offset(),
                         end = entry->End()](const auto&)
        {
            if (auto self = ptr.lock()) {
                self->RangeLock->UnlockWrite(handle, offset, end);
            }
        };

        auto promise = NewPromise<NProto::TWriteDataResponse>();
        auto future = promise.GetFuture();
        future.Subscribe(std::move(unlocker));

        RangeLock->LockWrite(
            entry->GetHandle(),
            entry->Offset(),
            entry->End(),
            [ptr = weak_from_this(),
             promise = std::move(promise),
             entry = entry.release()]() mutable
            {
                auto self = ptr.lock();
                // Lock action is invoked immediately or from
                // UnlockRead/UnlockWrite calls that can be made only when
                // TImpl is alive
                Y_DEBUG_ABORT_UNLESS(self);
                if (self) {
                    self->StartPendingWriteDataRequest(
                        std::unique_ptr<TWriteDataEntry>(entry),
                        std::move(promise));
                }
            });

        return future;
    }

    void OnEntriesFlushed(const TVector<TWriteDataEntry*>& entries)
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
    TVector<std::shared_ptr<NProto::TWriteDataRequest>>
    MakeWriteDataRequestsForFlush(ui64 handle)
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
                    char* allocationPtr = nullptr;
                    bool allocated = self->WriteDataRequestsQueue.AllocateBack(
                        serializedSize,
                        &allocationPtr);

                    if (!allocated) {
                        self->FlushAllData(); // flushes asynchronously
                        return;
                    }

                    Y_ABORT_UNLESS(allocationPtr != nullptr);
                    pending->Entry->SerializeAndMoveRequestBuffer(allocationPtr);
                    self->WriteDataRequestsQueue.CommitAllocation(allocationPtr);
                    self->AddWriteDataEntry(std::move(pending->Entry));

                    pending->Promise.SetValue({});

                    std::unique_ptr<TPendingWriteDataRequest> holder(pending);
                    pending->Unlink();
                }
            }
        });

        return future;
    }

private:
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

    void StartPendingWriteDataRequest(
        std::unique_ptr<TWriteDataEntry> entry,
        TPromise<NProto::TWriteDataResponse> promise)
    {
        auto serializedSize = entry->GetSerializedSize();
        auto guard = Guard(Lock);

        char* allocationPtr = nullptr;
        bool allocated = WriteDataRequestsQueue.AllocateBack(
            serializedSize,
            &allocationPtr);

        if (allocated) {
            Y_ABORT_UNLESS(allocationPtr != nullptr);
            entry->SerializeAndMoveRequestBuffer(allocationPtr);
            WriteDataRequestsQueue.CommitAllocation(allocationPtr);
            AddWriteDataEntry(std::move(entry));
            promise.SetValue({});
        } else {
            auto pending = std::make_unique<TPendingWriteDataRequest>(
                std::move(entry),
                std::move(promise));
            PendingWriteDataRequests.PushBack(pending.release());
            FlushAllData();   // flushes asynchronously
        }
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
}

size_t TWriteBackCache::TWriteDataEntry::GetSerializedSize() const
{
    return Request->ByteSizeLong() + sizeof(ui32) + BufferRef.size();
}

void TWriteBackCache::TWriteDataEntry::SerializeAndMoveRequestBuffer(
    char* allocationPtr)
{
    Y_ABORT_UNLESS(allocationPtr != nullptr);
    Y_ABORT_UNLESS(BufferRef.size() <= Max<ui32>());

    ui32 bufferSize = static_cast<ui32>(BufferRef.size());
    auto serializedSize = GetSerializedSize();

    TMemoryOutput mo(allocationPtr, serializedSize);
    mo.Write(&bufferSize, sizeof(bufferSize));
    mo.Write(BufferRef);

    Y_ABORT_UNLESS(
        Request->SerializeToArray(mo.Buf(), static_cast<int>(mo.Avail())));

    BufferRef = TStringBuf(allocationPtr + sizeof(ui32), bufferSize);
    RequestBuffer.clear();
}

}   // namespace NCloud::NFileStore::NFuse
