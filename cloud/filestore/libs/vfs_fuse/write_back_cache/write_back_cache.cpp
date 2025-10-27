#include "write_back_cache_impl.h"

#include "read_write_range_lock.h"

#include <cloud/filestore/libs/service/context.h>

#include <cloud/storage/core/libs/common/file_ring_buffer.h>

#include <library/cpp/threading/future/subscription/wait_all.h>

#include <util/generic/hash_set.h>
#include <util/generic/mem_copy.h>
#include <util/generic/intrlist.h>
#include <util/generic/strbuf.h>
#include <util/generic/vector.h>
#include <util/system/mutex.h>
#include <util/stream/mem.h>

namespace NCloud::NFileStore::NFuse {

using namespace NCloud::NFileStore::NVFS;
using namespace NThreading;

using EReadDataRequestCacheStatus =
    IWriteBackCacheStats::EReadDataRequestCacheStatus;

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
    TInstant PendingTime = TInstant::Zero();
};

////////////////////////////////////////////////////////////////////////////////

struct TFlushConfig
{
    TDuration AutomaticFlushPeriod;
    TDuration FlushRetryPeriod;
    ui32 MaxWriteRequestSize = 0;
    ui32 MaxWriteRequestsCount = 0;
    ui32 MaxSumWriteRequestsSize = 0;
};

}   // namespace

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

struct TWriteBackCache::TNodeState
{
    const ui64 NodeId = 0;

    // Entries from |TImpl::CachedEntries| with statuses Cached, FlushRequested
    // and Flushing filtered by nodeId appearing in the same order.
    // Note: entries with status Flushed reside only in |TImpl::CachedEntries|.
    TDeque<TWriteBackCache::TWriteDataEntry*> CachedEntries;

    // Efficient calculation of TWriteDataEntryParts from CachedEntries
    TWriteDataEntryIntervalMap CachedEntryIntervalMap;

    // Prevent from concurrent read and write requests with overlapping ranges
    TReadWriteRangeLock RangeLock;

    TFlushState FlushState;

    explicit TNodeState(ui64 nodeId)
        : NodeId(nodeId)
    {}

    bool Empty() const
    {
        return CachedEntries.empty() && RangeLock.Empty() &&
               !FlushState.Executing;
    }

    bool ShouldFlush() const
    {
        return !CachedEntries.empty() &&
               CachedEntries.front()->IsFlushRequested();
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
    // |TImpl::CachedEntriesPersistentQueue| and free space is increased or
    // an element is pushed to empty |TImpl::CachedEntriesPersistentQueue|.
    // We should try to push pending entries to the persistent queue.
    bool ShouldProcessPendingEntries = false;

    // Pending ReadData requests that have acquired |TNodeState::RangeLock|
    TVector<TPendingReadDataRequest> ReadData;

    // Flush operations that have been scheduled but not yet started
    TVector<TNodeState*> Flush;

    // Report WriteData requests as completed
    TVector<TPromise<NProto::TWriteDataResponse>> WriteDataCompleted;

    // Report FlushData and FlushAll requests as completed
    TVector<TPromise<void>> FlushCompleted;

    bool Empty() const
    {
        return ReadData.empty() && Flush.empty() &&
               WriteDataCompleted.empty() && FlushCompleted.empty();
    }
};

////////////////////////////////////////////////////////////////////////////////

// Reads a sequence of contiguous write data entry parts as a single buffer.
// Provides a method to read data across multiple entry parts efficiently.
class TWriteBackCache::TContiguousWriteDataEntryPartsReader
{
public:
    using TIterator = TVector<TWriteDataEntryPart>::const_iterator;

private:
    TIterator Current;
    ui64 CurrentReadOffset = 0;
    ui64 RemainingSize = 0;

public:
    TContiguousWriteDataEntryPartsReader(
            TIterator begin,
            TIterator end)
        : Current(begin)
    {
        if (begin < end) {
            Y_DEBUG_ABORT_UNLESS(Validate(begin, end));
            RemainingSize = std::prev(end)->End() - begin->Offset;
        }
    }

    ui64 GetOffset() const
    {
        return Current->Offset + CurrentReadOffset;
    }

    ui64 GetRemainingSize() const
    {
        return RemainingSize;
    }

    TString Read(ui64 bytesCount)
    {
        Y_ENSURE(
            bytesCount <= RemainingSize,
            "Trying to read more data ("
                << bytesCount << ") than is remaining (" << RemainingSize
                << ")");

        TString buffer(bytesCount, 0);

        while (bytesCount > 0) {
            // Nagivate to the next element if the current one is fully read.
            if (CurrentReadOffset == Current->Length) {
                Current++;
                CurrentReadOffset = 0;

                // The next element is guaranteed to be valid if the contiguous
                // buffer hasn't fully read
                Y_DEBUG_ABORT_UNLESS(RemainingSize > 0);

                // The next element is guaranteed to be non-empty - no need to
                // skip more elements
                Y_DEBUG_ABORT_UNLESS(Current->Length > 0);
            }

            const char* from = Current->Source->GetBuffer().data();
            from += Current->OffsetInSource + CurrentReadOffset;

            char* to = buffer.vend() - bytesCount;

            auto len = Min(Current->Length - CurrentReadOffset, bytesCount);
            MemCopy(to, from, len);

            bytesCount -= len;
            CurrentReadOffset += len;
            RemainingSize -= len;
        }

        return buffer;
    }

    // Validates a range of data entry parts to ensure they form a contiguous
    // sequence. Additionally checks that each part has non-zero length.
    static bool Validate(TIterator begin, TIterator end)
    {
        if (begin == end) {
            return true;
        }

        for (const auto* it = begin; it != end; it = std::next(it)) {
            if (it->Length == 0) {
                return false;
            }
        }

        const auto* prev = begin;
        for (const auto* it = std::next(begin); it != end; it = std::next(it)) {
            if (prev->End() != it->Offset) {
                return false;
            }
            prev = it;
        }

        return true;
    }
};

////////////////////////////////////////////////////////////////////////////////

class TWriteBackCache::TImpl final
    : public std::enable_shared_from_this<TImpl>
{
private:
    using TWriteDataEntry = TWriteBackCache::TWriteDataEntry;
    using TWriteDataEntryPart = TWriteBackCache::TWriteDataEntryPart;

    friend class TWriteBackCache::TWriteDataEntry;

    const IFileStorePtr Session;
    const ISchedulerPtr Scheduler;
    const ITimerPtr Timer;
    const IWriteBackCacheStatsPtr Stats;
    const TFlushConfig FlushConfig;

    // All fields below should be protected by this lock
    TMutex Lock;

    // Entries that are stored in the persistent queue - entries with
    // Cached, FlushRequested, Flushing and Flushed statuses
    TDeque<std::unique_ptr<TWriteDataEntry>> CachedEntries;

    // Serialized entries from |CachedEntries| with the same order
    TFileRingBuffer CachedEntriesPersistentQueue;

    // WriteData entries and Flush states grouped by nodeId
    THashMap<ui64, std::unique_ptr<TNodeState>> NodeStates;

    // Waiting queue for the available space in the cache - entries
    // with Pending status that have acquired write lock.
    TDeque<std::unique_ptr<TWriteDataEntry>> PendingEntries;

    // Nodes with new cached WriteData entries since last FlushAll
    THashSet<ui64> NodesWithNewCachedEntries;

    // Operations to execute after completing the main operation
    TPendingOperations PendingOperations;

    // Stats processing - entries filtered by status
    TIntrusiveList<TWriteDataEntry> PendingStatusEntries;
    TIntrusiveList<TWriteDataEntry> CachedStatusEntries;
    TIntrusiveList<TWriteDataEntry> FlushRequestedStatusEntries;
    TIntrusiveList<TWriteDataEntry> FlushingStatusEntries;
    TIntrusiveList<TWriteDataEntry> FlushedStatusEntries;

public:
    TImpl(
            IFileStorePtr session,
            ISchedulerPtr scheduler,
            ITimerPtr timer,
            IWriteBackCacheStatsPtr stats,
            const TString& filePath,
            ui32 capacityBytes,
            TFlushConfig flushConfig)
        : Session(std::move(session))
        , Scheduler(std::move(scheduler))
        , Timer(std::move(timer))
        , Stats(std::move(stats))
        , FlushConfig(flushConfig)
        , CachedEntriesPersistentQueue(filePath, capacityBytes)
    {
        // File ring buffer should be able to store any valid TWriteDataRequest.
        // Inability to store it will cause this and future requests to remain
        // in the pending queue forever (including requests with smaller size).
        // Should fit 1 MiB of data plus some headers (assume 1 KiB is enough).
        Y_ABORT_UNLESS(
            CachedEntriesPersistentQueue.MaxAllocationSize() >=
            1024 * 1024 + 1016);

        Stats->ResetNonDerivativeCounters();

        CachedEntriesPersistentQueue.Visit(
            [&](auto checksum, auto serializedRequest)
            {
                auto entry = std::make_unique<TWriteDataEntry>(
                    checksum,
                    serializedRequest,
                    this);

                if (entry->IsCorrupted()) {
                    // This may happen when a buffer was corrupted.
                    // We should add this entry to a queue like a normal entry
                    // because there is 1-by-1 correspondence between
                    // CachedEntriesPersistentQueue and CachedEntries.
                    // TODO(nasonov): report it
                    CachedEntries.push_back(std::move(entry));
                } else {
                    auto* nodeState = GetOrCreateNodeState(entry->GetNodeId());
                    AddCachedEntry(nodeState, std::move(entry));
                }
            });

        UpdatePersistentQueueStats();
    }

    void ScheduleAutomaticFlushIfNeeded()
    {
        if (!FlushConfig.AutomaticFlushPeriod) {
            return;
        }

        Scheduler->Schedule(
            Timer->Now() + FlushConfig.AutomaticFlushPeriod,
            [ptr = weak_from_this()] () {
                if (auto self = ptr.lock()) {
                    self->RequestAutomaticFlush();
                }
            });
    }

    void RequestAutomaticFlush()
    {
        auto guard = Guard(Lock);
        RequestFlushAll();
        ExecutePendingOperations(guard);
        ScheduleAutomaticFlushIfNeeded();
    }

    // should be protected by |Lock|
    TVector<TWriteDataEntryPart> CalculateCachedDataPartsToRead(
        ui64 nodeId,
        ui64 startingFromOffset,
        ui64 length)
    {
        auto entriesIter = NodeStates.find(nodeId);
        if (entriesIter == NodeStates.end()) {
            return {};
        }

        return TUtil::CalculateDataPartsToRead(
            entriesIter->second->CachedEntryIntervalMap,
            startingFromOffset,
            length);
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

        auto nodeId = request->GetNodeId();
        auto offset = request->GetOffset();
        auto end = request->GetOffset() + request->GetLength();

        auto unlocker =
            [ptr = weak_from_this(), nodeId, offset, end](const auto&)
        {
            if (auto self = ptr.lock()) {
                auto guard = Guard(self->Lock);
                auto* nodeState = self->GetNodeState(nodeId);
                nodeState->RangeLock.UnlockRead(offset, end);
                self->DeleteNodeStateIfEmpty(nodeState);
                self->ExecutePendingOperations(guard);
            }
        };

        TPendingReadDataRequest pendingRequest = {
            .CallContext = std::move(callContext),
            .Request = std::move(request),
            .Promise = NewPromise<NProto::TReadDataResponse>(),
            .PendingTime = Timer->Now()};

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

        auto* nodeState = GetOrCreateNodeState(nodeId);
        nodeState->RangeLock.LockRead(offset, end, std::move(locker));

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

        auto nodeId = entry->GetNodeId();
        auto offset = entry->Offset();
        auto end = entry->End();

        auto unlocker =
            [ptr = weak_from_this(), nodeId, offset, end](const auto&)
        {
            if (auto self = ptr.lock()) {
                auto guard = Guard(self->Lock);
                auto* nodeState = self->GetNodeState(nodeId);
                nodeState->RangeLock.UnlockWrite(offset, end);
                self->DeleteNodeStateIfEmpty(nodeState);
                self->ExecutePendingOperations(guard);
            }
        };

        auto future = entry->GetCachedFuture();
        future.Subscribe(std::move(unlocker));

        auto* entryPtr = entry.release();

        auto locker =
            [ptr = weak_from_this(), entry = entryPtr]() mutable
        {
            auto self = ptr.lock();
            // Lock action is invoked immediately or from
            // UnlockRead/UnlockWrite calls that can be made only when
            // TImpl is alive
            Y_ABORT_UNLESS(self);
            if (self) {
                if (self->PendingEntries.empty()) {
                    self->PendingOperations.ShouldProcessPendingEntries = true;
                }
                self->PendingEntries.push_back(
                    std::unique_ptr<TWriteDataEntry>(entry));
            }
        };

        auto guard = Guard(Lock);

        entryPtr->SetPending(this);

        auto* nodeState = GetOrCreateNodeState(nodeId);
        nodeState->RangeLock.LockWrite(offset, end, std::move(locker));

        ExecutePendingOperations(guard);

        return future;
    }

    // should be protected by |Lock|
    auto MakeWriteDataRequestsForFlush(
        ui64 nodeId,
        ui64 handle,
        const TVector<TWriteDataEntryPart>& parts) const
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

            TContiguousWriteDataEntryPartsReader reader(
                parts.begin() + partIndex,
                parts.begin() + rangeEndIndex);

            while (reader.GetRemainingSize() > 0) {
                auto size = Min(
                    reader.GetRemainingSize(),
                    static_cast<ui64>(FlushConfig.MaxWriteRequestSize));

                auto offset = reader.GetOffset();
                auto buffer = reader.Read(size);

                auto request = std::make_shared<NProto::TWriteDataRequest>();
                request->SetFileSystemId(
                    parts[partIndex].Source->GetRequest()->GetFileSystemId());
                *request->MutableHeaders() =
                    parts[partIndex].Source->GetRequest()->GetHeaders();
                request->SetNodeId(nodeId);
                request->SetHandle(handle);
                request->SetOffset(offset);
                request->SetBuffer(std::move(buffer));

                res.push_back(std::move(request));
            }

            partIndex = rangeEndIndex;
        }

        return res;
    }

    TFuture<void> FlushNodeData(ui64 nodeId)
    {
        auto guard = Guard(Lock);

        auto* nodeState = GetNodeStateOrNull(nodeId);
        auto future = RequestFlushAndGetFuture(nodeState);

        ExecutePendingOperations(guard);

        return future;
    }

    TFuture<void> FlushAllData()
    {
        TVector<TFuture<void>> futures;
        {
            auto guard = Guard(Lock);

            for (const auto& [_, nodeState]: NodeStates) {
                futures.push_back(RequestFlushAndGetFuture(nodeState.get()));
            }

            ExecutePendingOperations(guard);
        }
        return NWait::WaitAll(futures);
    }

    // should be protected by |Lock|
    void RequestFlush(TNodeState* nodeState)
    {
        auto it = nodeState->CachedEntries.rbegin();
        while (it != nodeState->CachedEntries.rend() &&
               (*it)->RequestFlush(this))
        {
            ++it;
        }

        if (nodeState->ShouldFlush() && !nodeState->FlushState.Executing) {
            nodeState->FlushState.Executing = true;
            PendingOperations.Flush.push_back(nodeState);
        }
    }

    // should be protected by |Lock|
    void RequestFlushAll()
    {
        for (auto nodeId: NodesWithNewCachedEntries) {
            auto nodeEntryIter = NodeStates.find(nodeId);
            if (nodeEntryIter == NodeStates.end()) {
                continue;
            }

            auto* nodeState = nodeEntryIter->second.get();
            if (nodeState->CachedEntries.empty()) {
                continue;
            }

            RequestFlush(nodeState);
        }

        NodesWithNewCachedEntries.clear();
    }

    // should be protected by |Lock|
    TFuture<void> RequestFlushAndGetFuture(TNodeState* nodeState)
    {
        if (nodeState == nullptr || nodeState->CachedEntries.empty()) {
            return NThreading::MakeFuture();
        }

        RequestFlush(nodeState);

        return nodeState->CachedEntries.back()->GetFlushFuture();
    }

private:
    TNodeState* GetNodeStateOrNull(ui64 nodeId)
    {
        auto it = NodeStates.find(nodeId);
        return it != NodeStates.end() ? it->second.get() : nullptr;
    }

    TNodeState* GetOrCreateNodeState(ui64 nodeId)
    {
        auto& ptr = NodeStates[nodeId];
        if (!ptr) {
            ptr = std::make_unique<TNodeState>(nodeId);
            Stats->IncrementNodeCount();
        }
        return ptr.get();
    }

    TNodeState* GetNodeState(ui64 nodeId)
    {
        const auto& ptr = NodeStates[nodeId];
        Y_ABORT_UNLESS(ptr);
        return ptr.get();
    }

    void DeleteNodeStateIfEmpty(TNodeState* nodeState)
    {
        if (nodeState != nullptr && nodeState->Empty()) {
            auto erased = NodeStates.erase(nodeState->NodeId);
            Y_DEBUG_ABORT_UNLESS(erased);
            Stats->DecrementNodeCount();
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
            TVector<TNodeState*> flush;
            TVector<TPromise<NProto::TWriteDataResponse>> writeDataCompleted;
            TVector<TPromise<void>> flushCompleted;

            swap(readData, PendingOperations.ReadData);
            swap(flush, PendingOperations.Flush);
            swap(writeDataCompleted, PendingOperations.WriteDataCompleted);
            swap(flushCompleted, PendingOperations.FlushCompleted);

            // Unguard works as an inverse lock - it releases lock held
            // by a lock guard and acquires it back at the end of the section
            auto unguard = Unguard(guard);

            for (auto& request: readData) {
                StartPendingReadDataRequest(std::move(request));
            }

            for (auto* nodeState: flush) {
                StartFlush(nodeState);
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
        while (!PendingEntries.empty()) {
            auto& entry = PendingEntries.front();
            auto serializedSize = entry->GetSerializedSize();

            char* allocationPtr = nullptr;
            bool allocated = CachedEntriesPersistentQueue.AllocateBack(
                serializedSize,
                &allocationPtr);

            if (!allocated) {
                RequestFlushAll();
                break;
            }

            Y_ABORT_UNLESS(allocationPtr != nullptr);

            entry->SerializeAndMoveRequestBuffer(
                allocationPtr,
                PendingOperations,
                this);

            CachedEntriesPersistentQueue.CommitAllocation(allocationPtr);

            auto* nodeState = GetNodeState(entry->GetNodeId());
            AddCachedEntry(nodeState, std::move(entry));

            PendingEntries.pop_front();
        }

        UpdatePersistentQueueStats();
    }

    TVector<TWriteDataEntryPart> CalculateDataPartsToReadAndFillBuffer(
        ui64 nodeId,
        ui64 startingFromOffset,
        ui64 length,
        TString* buffer)
    {
        *buffer = TString(length, 0);

        with_lock (Lock) {
            auto parts = CalculateCachedDataPartsToRead(
                nodeId,
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
        auto nodeId = request.Request->GetNodeId();

        TReadDataState state;
        state.StartingFromOffset = request.Request->GetOffset();
        state.Length = request.Request->GetLength();
        state.Promise = std::move(request.Promise);

        auto waitDuration = Timer->Now() - request.PendingTime;

        state.Parts = CalculateDataPartsToReadAndFillBuffer(
            nodeId,
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
            Stats->AddReadDataStats(
                EReadDataRequestCacheStatus::FullHit,
                waitDuration);
            state.Promise.SetValue(std::move(response));
            return;
        }

        auto cacheState = state.Parts.empty()
            ? EReadDataRequestCacheStatus::Miss
            : EReadDataRequestCacheStatus::PartialHit;

        Stats->AddReadDataStats(cacheState, waitDuration);

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

    // |nodeState| becomes unusable if the function returns false
    void PrepareFlush(TNodeState* nodeState)
    {
        Y_ABORT_UNLESS(nodeState->FlushState.Executing);
        Y_ABORT_UNLESS(nodeState->FlushState.WriteRequests.empty());

        if (!nodeState->FlushState.FailedWriteRequests.empty()) {
            // Retry write requests failed at the previous Flush attempt
            swap(
                nodeState->FlushState.WriteRequests,
                nodeState->FlushState.FailedWriteRequests);
            return;
        }

        TVector<TWriteDataEntryPart> parts;
        ui64 handle = InvalidHandle;

        with_lock (Lock) {
            // Flush cannot be scheduled when CachedEntries is empty
            Y_ABORT_UNLESS(!nodeState->CachedEntries.empty());

            // Use any valid handle for generating write requests
            handle = nodeState->CachedEntries.front()->GetHandle();

            auto entryCount = TUtil::CalculateEntriesCountToFlush(
                nodeState->CachedEntries,
                FlushConfig.MaxWriteRequestSize,
                FlushConfig.MaxWriteRequestsCount,
                FlushConfig.MaxSumWriteRequestsSize);

            if (entryCount == 0) {
                // Even a single entry is too large to flush
                // TODO(nasonov): report and try to flush it anyway
                entryCount = 1;
            }

            parts = TUtil::CalculateDataPartsToFlush(
                nodeState->CachedEntries,
                entryCount);

            // Non-empty CachedEntries cannot produce empty parts
            Y_ABORT_UNLESS(!parts.empty());

            nodeState->FlushState.AffectedWriteDataEntriesCount = entryCount;

            for (size_t i = 0; i < entryCount; i++) {
                auto* entry = nodeState->CachedEntries[i];
                entry->StartFlush(this);
            }

            Stats->FlushStarted();
        }

        nodeState->FlushState.WriteRequests =
            MakeWriteDataRequestsForFlush(nodeState->NodeId, handle, parts);
    }

    void StartFlush(TNodeState* nodeState)
    {
        PrepareFlush(nodeState);

        auto& state = nodeState->FlushState;

        Y_ABORT_UNLESS(state.Executing);
        Y_ABORT_UNLESS(!state.WriteRequests.empty());
        Y_ABORT_UNLESS(state.FailedWriteRequests.empty());
        Y_ABORT_UNLESS(state.AffectedWriteDataEntriesCount > 0);
        Y_ABORT_UNLESS(state.InFlightWriteRequestsCount == 0);

        state.InFlightWriteRequestsCount = state.WriteRequests.size();

        for (auto& request: state.WriteRequests) {
            auto callContext =
                MakeIntrusive<TCallContext>(request->GetFileSystemId());
            callContext->RequestType = EFileStoreRequest::WriteData;
            callContext->RequestSize = request->GetBuffer().size();

            Session->WriteData(std::move(callContext), request)
                .Subscribe(
                    [nodeState,
                     request = std::move(request),
                     ptr = weak_from_this()](auto future)
                    {
                        auto self = ptr.lock();
                        if (self) {
                            self->OnWriteDataRequestCompleted(
                                nodeState,
                                std::move(request),
                                future.GetValue());
                        }
                    })
                .IgnoreResult();
        }
    }

    void OnWriteDataRequestCompleted(
        TNodeState* nodeState,
        std::shared_ptr<NProto::TWriteDataRequest> request,
        const NProto::TWriteDataResponse& response)
    {
        auto& state = nodeState->FlushState;

        with_lock (Lock) {
            if (HasError(response)) {
                state.FailedWriteRequests.push_back(std::move(request));
            }

            Y_ABORT_UNLESS(state.InFlightWriteRequestsCount > 0);
            if (--state.InFlightWriteRequestsCount > 0) {
                return;
            }
        }

        state.WriteRequests.clear();

        if (state.FailedWriteRequests.empty()) {
            Stats->FlushCompleted();
            CompleteFlush(nodeState);
        } else {
            Stats->FlushFailed();
            ScheduleRetryFlush(nodeState);
        }
    }

    // |nodeState| becomes unusable after this call
    void CompleteFlush(TNodeState* nodeState)
    {
        Y_ABORT_UNLESS(nodeState->FlushState.Executing);
        Y_ABORT_UNLESS(nodeState->FlushState.FailedWriteRequests.empty());
        Y_ABORT_UNLESS(nodeState->FlushState.InFlightWriteRequestsCount == 0);

        auto guard = Guard(Lock);

        while (nodeState->FlushState.AffectedWriteDataEntriesCount > 0) {
            nodeState->FlushState.AffectedWriteDataEntriesCount--;

            auto* entry = RemoveFrontCachedEntry(nodeState);
            entry->FinishFlush(PendingOperations, this);
        }

        // Clear flushed entries from the persistent queue
        while (!CachedEntries.empty() && CachedEntries.front()->IsFlushed()) {
            auto* entry = CachedEntries.front().get();
            entry->Complete(this);

            CachedEntries.pop_front();
            CachedEntriesPersistentQueue.PopFront();
            PendingOperations.ShouldProcessPendingEntries = true;
        }

        UpdatePersistentQueueStats();

        if (nodeState->ShouldFlush()) {
            PendingOperations.Flush.push_back(nodeState);
        } else {
            nodeState->FlushState.Executing = false;
            DeleteNodeStateIfEmpty(nodeState);
        }

        ExecutePendingOperations(guard);
    }

    void ScheduleRetryFlush(TNodeState* nodeState)
    {
        // TODO(nasonov): better retry policy
        Scheduler->Schedule(
            Timer->Now() + FlushConfig.FlushRetryPeriod,
            [nodeState, ptr = weak_from_this()]()
            {
                auto self = ptr.lock();
                if (self) {
                    self->StartFlush(nodeState);
                }
            });
    }

    void AddCachedEntry(
        TNodeState* nodeState,
        std::unique_ptr<TWriteDataEntry> entry)
    {
        Y_ABORT_UNLESS(nodeState != nullptr);
        Y_ABORT_UNLESS(nodeState->NodeId == entry->GetNodeId());

        nodeState->CachedEntryIntervalMap.Add(entry.get());
        nodeState->CachedEntries.push_back(entry.get());
        NodesWithNewCachedEntries.insert(nodeState->NodeId);
        CachedEntries.push_back(std::move(entry));
    }

    static TWriteDataEntry* RemoveFrontCachedEntry(TNodeState* nodeState)
    {
        Y_ABORT_UNLESS(nodeState != nullptr);
        Y_ABORT_UNLESS(!nodeState->CachedEntries.empty());

        auto* entry = nodeState->CachedEntries.front();
        nodeState->CachedEntries.pop_front();
        nodeState->CachedEntryIntervalMap.Remove(entry);

        return entry;
    }

    void UpdatePersistentQueueStats()
    {
        Stats->UpdatePersistentQueueStats(
            {.RawCapacity = CachedEntriesPersistentQueue.GetRawCapacity(),
             .RawUsedBytesCount =
                 CachedEntriesPersistentQueue.GetRawUsedBytesCount(),
             .MaxAllocationBytesCount =
                 CachedEntriesPersistentQueue.GetMaxAllocationBytesCount(),
             .IsCorrupted = CachedEntriesPersistentQueue.IsCorrupted()});
    }

    TIntrusiveList<TWriteDataEntry>& GetEntryListByStatus(
        EWriteDataRequestStatus status)
    {
        switch (status) {
            case EWriteDataRequestStatus::Pending:
                return PendingStatusEntries;
            case EWriteDataRequestStatus::Cached:
                return CachedStatusEntries;
            case EWriteDataRequestStatus::FlushRequested:
                return FlushRequestedStatusEntries;
            case EWriteDataRequestStatus::Flushing:
                return FlushingStatusEntries;
            case EWriteDataRequestStatus::Flushed:
                return FlushedStatusEntries;
            default:
                Y_ABORT(
                    "Invalid EWriteDataRequestStatus - %d",
                    static_cast<int>(status));
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

TWriteBackCache::TWriteBackCache() = default;

TWriteBackCache::TWriteBackCache(
        IFileStorePtr session,
        ISchedulerPtr scheduler,
        ITimerPtr timer,
        IWriteBackCacheStatsPtr stats,
        const TString& filePath,
        ui32 capacityBytes,
        TDuration automaticFlushPeriod,
        TDuration flushRetryPeriod,
        ui32 maxWriteRequestSize,
        ui32 maxWriteRequestsCount,
        ui32 maxSumWriteRequestsSize)
    : Impl(
        new TImpl(
            std::move(session),
            std::move(scheduler),
            std::move(timer),
            std::move(stats),
            filePath,
            capacityBytes,
            {.AutomaticFlushPeriod = automaticFlushPeriod,
             .FlushRetryPeriod = flushRetryPeriod,
             .MaxWriteRequestSize = maxWriteRequestSize,
             .MaxWriteRequestsCount = maxWriteRequestsCount,
             .MaxSumWriteRequestsSize = maxSumWriteRequestsSize}))
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

TFuture<void> TWriteBackCache::FlushNodeData(ui64 nodeId)
{
    return Impl->FlushNodeData(nodeId);
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
{
    RequestBuffer.swap(*Request->MutableBuffer());
    BufferRef = TStringBuf(RequestBuffer).Skip(Request->GetBufferOffset());
    Request->ClearBufferOffset();
}

TWriteBackCache::TWriteDataEntry::TWriteDataEntry(
    ui32 checksum,
    TStringBuf serializedRequest,
    TImpl* impl)
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
        SetStatus(EWriteDataRequestStatus::Corrupted, impl);
        return;
    }

    const char* bufferPtr = mi.Buf();

    if (mi.Skip(bufferSize) != bufferSize) {
        // Buffer corruption
        // TODO(nasonov): report and handle
        SetStatus(EWriteDataRequestStatus::Corrupted, impl);
        return;
    }

    auto parsedRequest = std::make_shared<NProto::TWriteDataRequest>();
    if (!parsedRequest->ParseFromArray(mi.Buf(), static_cast<int>(mi.Avail()))) {
        // Buffer corruption
        // TODO(nasonov): report and handle
        SetStatus(EWriteDataRequestStatus::Corrupted, impl);
        return;
    }

    Request.swap(parsedRequest);
    BufferRef = TStringBuf(bufferPtr, bufferSize);
    SetStatus(EWriteDataRequestStatus::Cached, impl);
}

size_t TWriteBackCache::TWriteDataEntry::GetSerializedSize() const
{
    return Request->ByteSizeLong() + sizeof(ui32) + BufferRef.size();
}

void TWriteBackCache::TWriteDataEntry::SetPending(TImpl* impl)
{
    Y_ABORT_UNLESS(Status == EWriteDataRequestStatus::Initial);
    SetStatus(EWriteDataRequestStatus::Pending, impl);
}

void TWriteBackCache::TWriteDataEntry::SerializeAndMoveRequestBuffer(
    char* allocationPtr,
    TPendingOperations& pendingOperations,
    TImpl* impl)
{
    Y_ABORT_UNLESS(AllocationPtr == nullptr);
    Y_ABORT_UNLESS(allocationPtr != nullptr);
    Y_ABORT_UNLESS(BufferRef.size() <= Max<ui32>());
    Y_ABORT_UNLESS(Status == EWriteDataRequestStatus::Pending);

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

    SetStatus(EWriteDataRequestStatus::Cached, impl);

    if (CachedPromise.Initialized()) {
        pendingOperations.WriteDataCompleted.push_back(
            std::move(CachedPromise));
    }
}

bool TWriteBackCache::TWriteDataEntry::RequestFlush(TImpl* impl)
{
    switch (Status) {
        case EWriteDataRequestStatus::Cached:
            SetStatus(EWriteDataRequestStatus::FlushRequested, impl);
            return true;

        case EWriteDataRequestStatus::FlushRequested:
        case EWriteDataRequestStatus::Flushing:
            return false;

        default:
            Y_ABORT(
                "It is not possible to request flush for entry with status %d",
                static_cast<int>(Status));
    }
}

void TWriteBackCache::TWriteDataEntry::StartFlush(TImpl* impl)
{
    Y_ABORT_UNLESS(
        Status == EWriteDataRequestStatus::Cached ||
        Status == EWriteDataRequestStatus::FlushRequested);

    SetStatus(EWriteDataRequestStatus::Flushing, impl);
}

void TWriteBackCache::TWriteDataEntry::FinishFlush(
    TPendingOperations& pendingOperations,
    TImpl* impl)
{
    Y_ABORT_UNLESS(Status == EWriteDataRequestStatus::Flushing);

    SetStatus(EWriteDataRequestStatus::Flushed, impl);
    BufferRef.Clear();

    if (FlushPromise.Initialized()) {
        pendingOperations.FlushCompleted.push_back(
            std::move(FlushPromise));
    }
}

void TWriteBackCache::TWriteDataEntry::Complete(TImpl* impl)
{
    Y_ABORT_UNLESS(Status == EWriteDataRequestStatus::Flushed);
    SetStatus(EWriteDataRequestStatus::Initial, impl);
}

auto TWriteBackCache::TWriteDataEntry::GetCachedFuture()
    -> TFuture<NProto::TWriteDataResponse>
{
    Y_ABORT_UNLESS(CachedPromise.Initialized());
    return CachedPromise.GetFuture();
}

TFuture<void> TWriteBackCache::TWriteDataEntry::GetFlushFuture()
{
    if (!FlushPromise.Initialized()) {
        if (Status == EWriteDataRequestStatus::Flushed) {
            return MakeFuture();
        }
        FlushPromise = NewPromise();
    }
    return FlushPromise.GetFuture();
}

void TWriteBackCache::TWriteDataEntry::SetStatus(
    EWriteDataRequestStatus status,
    TImpl* impl)
{
    Y_ABORT_UNLESS(Status != status);

    auto now = impl->Timer->Now();

    if (Status != EWriteDataRequestStatus::Initial &&
        Status != EWriteDataRequestStatus::Corrupted)
    {
        auto& list = impl->GetEntryListByStatus(Status);
        Y_DEBUG_ABORT_UNLESS(!list.Empty());

        auto prevMinTime = list.Front()->StatusChangeTime;
        list.Remove(this);

        impl->Stats->WriteDataRequestExitedStatus(
            Status,
            now - StatusChangeTime);

        auto minTime =
            list.Empty() ? TInstant::Zero() : list.Front()->StatusChangeTime;

        if (prevMinTime != minTime) {
            impl->Stats->WriteDataRequestUpdateMinTime(Status, minTime);
        }
    }

    Status = status;
    StatusChangeTime = now;

    if (Status != EWriteDataRequestStatus::Initial &&
        Status != EWriteDataRequestStatus::Corrupted)
    {
        auto& list = impl->GetEntryListByStatus(Status);

        impl->Stats->WriteDataRequestEnteredStatus(Status);

        if (list.Empty()) {
            impl->Stats->WriteDataRequestUpdateMinTime(Status, now);
        }

        list.PushBack(this);
    }
}

////////////////////////////////////////////////////////////////////////////////

void TWriteBackCache::TWriteDataEntryIntervalMap::Add(TWriteDataEntry* entry)
{
    TBase::VisitOverlapping(
        entry->Offset(),
        entry->End(),
        [this, entry](auto it)
        {
            auto prev = it->second;
            TBase::Remove(it);

            if (prev.Begin < entry->Offset()) {
                TBase::Add(prev.Begin, entry->Offset(), prev.Value);
            }

            if (entry->End() < prev.End) {
                TBase::Add(entry->End(), prev.End, prev.Value);
            }
        });

    TBase::Add(entry->Offset(), entry->End(), entry);
}

void TWriteBackCache::TWriteDataEntryIntervalMap::Remove(TWriteDataEntry* entry)
{
    TBase::VisitOverlapping(
        entry->Offset(),
        entry->End(),
        [&](auto it)
        {
            if (it->second.Value == entry) {
                TBase::Remove(it);
            }
        });
}

namespace {

////////////////////////////////////////////////////////////////////////////////

class TDummyWriteBackCacheStats
    : public IWriteBackCacheStats
{
public:
    void ResetNonDerivativeCounters() override
    {}

    void FlushStarted() override
    {}

    void FlushCompleted() override
    {}

    void FlushFailed() override
    {}

    void IncrementNodeCount() override
    {}

    void DecrementNodeCount() override
    {}

    void WriteDataRequestEnteredStatus(
        TWriteBackCache::EWriteDataRequestStatus status) override
    {
        Y_UNUSED(status);
    }

    void WriteDataRequestExitedStatus(
        TWriteBackCache::EWriteDataRequestStatus status,
        TDuration duration) override
    {
        Y_UNUSED(status);
        Y_UNUSED(duration);
    }

    void WriteDataRequestUpdateMinTime(
        TWriteBackCache::EWriteDataRequestStatus status,
        TInstant minTime) override
    {
        Y_UNUSED(status);
        Y_UNUSED(minTime);
    }

    void AddReadDataStats(
        IWriteBackCacheStats::EReadDataRequestCacheStatus status,
        TDuration duraton) override
    {
        Y_UNUSED(status);
        Y_UNUSED(duraton);
    }

    void UpdatePersistentQueueStats(
        const TWriteBackCache::TPersistentQueueStats& stats) override
    {
        Y_UNUSED(stats);
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IWriteBackCacheStatsPtr CreateDummyWriteBackCacheStats()
{
    return std::make_shared<TDummyWriteBackCacheStats>();
}

}   // namespace NCloud::NFileStore::NFuse
