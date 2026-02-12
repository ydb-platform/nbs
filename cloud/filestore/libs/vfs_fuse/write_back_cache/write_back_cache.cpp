#include "write_back_cache_impl.h"

#include "persistent_storage.h"
#include "read_write_range_lock.h"
#include "write_data_request_builder.h"

#include <cloud/filestore/libs/diagnostics/critical_events.h>
#include <cloud/filestore/libs/service/context.h>

#include <library/cpp/digest/crc32c/crc32c.h>
#include <library/cpp/threading/future/subscription/wait_all.h>

#include <util/generic/hash_set.h>
#include <util/generic/mem_copy.h>
#include <util/generic/intrlist.h>
#include <util/generic/set.h>
#include <util/generic/strbuf.h>
#include <util/generic/vector.h>
#include <util/system/mutex.h>
#include <util/stream/mem.h>

namespace NCloud::NFileStore::NFuse {

using namespace NCloud::NFileStore::NVFS;
using namespace NThreading;
using namespace NWriteBackCache;

namespace {

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
};

////////////////////////////////////////////////////////////////////////////////

struct TFlushRequest
{
    const ui64 RequestId = 0;
    TPromise<void> Promise = NewPromise();

    explicit TFlushRequest(ui64 requestId)
        : RequestId(requestId)
    {}
};

////////////////////////////////////////////////////////////////////////////////

struct TBufferWriter
{
    std::span<char> TargetBuffer;

    explicit TBufferWriter(std::span<char> targetBuffer)
        : TargetBuffer(targetBuffer)
    {}

    void Write(TStringBuf buffer)
    {
        Y_ABORT_UNLESS(
            buffer.size() <= TargetBuffer.size(),
            "Not enough space in the buffer to write %lu bytes, remaining: %lu",
            buffer.size(),
            TargetBuffer.size());

        buffer.copy(TargetBuffer.data(), buffer.size());
        TargetBuffer = TargetBuffer.subspan(buffer.size());
    }
};

////////////////////////////////////////////////////////////////////////////////

ui64 SaturationSub(ui64 x, ui64 y)
{
    return x > y ? x - y : 0;
}

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

    // Entries from |TImpl::AllEntries| filtered by |NodeId| and appearing in
    // the same order (RequestId is strictly increasing)
    TIntrusiveList<TWriteDataEntry, TNodeListTag> AllEntries;

    // Entries from |TImpl::CachedEntries| with statuses Cached and Flushing
    // filtered by |NodeId| appearing in the same order.
    // Note: entries with status Flushed reside only in |TImpl::CachedEntries|.
    // Note: each entry should appear in |AllEntries|, but the order is not
    // preserved (RequestId is not ordered)
    TDeque<TWriteBackCache::TWriteDataEntry*> CachedEntries;
    ui64 MaxCachedRequestId = 0;

    // Efficient calculation of TWriteDataEntryParts from CachedEntries
    TWriteDataEntryIntervalMap CachedEntryIntervalMap;

    // Prevent from concurrent read and write requests with overlapping ranges
    TReadWriteRangeLock RangeLock;

    TFlushState FlushState;

    // Flush requests are fulfilled when there are no entries in |AllEntries|
    // with RequestId less or equal than |TFlushRequest::RequestId|.
    // Flush requests are stored in chronological order: RequestId values are
    // strictly increasing so newer flush requests have larger RequestId.
    TDeque<TFlushRequest> FlushRequests;

    // All entries with RequestId <= |AutomaticFlushRequestId| are to be flushed
    ui64 AutomaticFlushRequestId = 0;

    // Cached data extends the node size but until the data is flushed,
    // the changes are not visible to the tablet. FileSystem requests that
    // return node attributes or rely on it (GetAttr, Lookup, Read, ReadDir)
    // should have the node size adjusted to this value.
    ui64 CachedNodeSize = 0;

    // Non-zero value indicates that the node state is to be deleted but it is
    // referenced from |TImpl::NodeStateRefs|. 'Referenced' means that there are
    // values in |TImpl::NodeStateRefs| that are less than |DeletionId|.
    ui64 DeletionId = 0;

    explicit TNodeState(ui64 nodeId)
        : NodeId(nodeId)
    {}

    bool CanBeDeleted() const
    {
        if (AllEntries.Empty() && RangeLock.Empty()) {
            Y_ABORT_UNLESS(CachedEntries.empty());
            Y_ABORT_UNLESS(!FlushState.Executing);
            return true;
        }
        return false;
    }

    bool ShouldFlush(ui64 maxFlushAllRequestId) const
    {
        if (CachedEntries.empty()) {
            return false;
        }

        Y_ABORT_UNLESS(!AllEntries.Empty());

        if (!FlushRequests.empty()) {
            return true;
        }

        const ui64 minRequestId = AllEntries.Front()->GetRequestId();
        return minRequestId <= maxFlushAllRequestId ||
               minRequestId <= AutomaticFlushRequestId;
    }
};

////////////////////////////////////////////////////////////////////////////////

// Accumulate operations to execute after completing the main operation.
// 1. Set promises exposed to the user code outside lock sections.
// 2. Avoid recursion chains in future completion callbacks.
struct TWriteBackCache::TQueuedOperations
{
    // Used to prevent recursive calls of ProcessQueuedOperations
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
    const NWriteBackCache::IWriteDataRequestBuilderPtr RequestBuilder;
    const TFlushConfig FlushConfig;
    const TLog Log;
    const TString LogTag;
    const TString FileSystemId;

    // Used to check AllEntries for emptiness without |Lock|
    std::atomic<bool> EmptyFlag = true;

    // All fields below should be protected by this lock
    TMutex Lock;

    // All entries with Pending, Cached and Flushing statuses (but not Flushed).
    // New entries are appended at the back of this list. Each entry is
    // assigned a strictly increasing RequestId, so the list preserves the
    // chronological order of requests (oldest at the front).
    TIntrusiveList<TWriteDataEntry, TGlobalListTag> AllEntries;
    ui64 NextRequestId = 1;

    // Entries stored in the persistent queue: entries with statuses
    // Cached, FlushRequested, Flushing and Flushed (but not Pending).
    // Each entry in |CachedEntries| has a corresponding entry in
    // |CachedEntriesPersistentQueue| (1:1 correspondence). The order of entries
    // may differ from the chronological order in |AllEntries|.
    TDeque<std::unique_ptr<TWriteDataEntry>> CachedEntries;
    IPersistentStoragePtr PersistentStorage;

    // WriteData entries and Flush states grouped by nodeId
    THashMap<ui64, std::unique_ptr<TNodeState>> NodeStates;

    // References to node states that prevents their deletion from |NodeStates|.
    // Used to keep MinNodeSize information for flushed nodes.
    TSet<ui64> NodeStateRefs;

    // Node states to be deleted but that are referenced from |NodeStateRefs|.
    // Key: |TNodeState::DeletionId|, Value: NodeId.
    TMap<ui64, ui64> DeletedNodeStates;

    // Waiting queue for the available space in the cache - entries
    // with Pending status that have acquired write lock.
    TDeque<std::unique_ptr<TWriteDataEntry>> PendingEntries;

    // Optimization: don't iterate over all node states during FlushAll;
    // only consider nodes that have new or changed entries.
    THashSet<ui64> NodesWithNewEntries;
    THashSet<ui64> NodesWithNewCachedEntries;

    // FlushAll requests are fulfilled when there are no entries in |AllEntries|
    // with RequestId less or equal than |TFlushRequest::RequestId|.
    // FlushAll requests are stored in chronological order: RequestId values are
    // strictly increasing so newer flush requests have larger RequestId.
    TDeque<TFlushRequest> FlushAllRequests;

    // Operations to execute after completing the main operation
    TQueuedOperations QueuedOperations;

    // Stats processing - entries filtered by status
    TIntrusiveList<TWriteDataEntry> PendingStatusEntries;
    TIntrusiveList<TWriteDataEntry> CachedStatusEntries;
    TIntrusiveList<TWriteDataEntry> FlushingStatusEntries;
    TIntrusiveList<TWriteDataEntry> FlushedStatusEntries;

public:
    TImpl(
            IFileStorePtr session,
            ISchedulerPtr scheduler,
            ITimerPtr timer,
            IWriteBackCacheStatsPtr stats,
            NWriteBackCache::IWriteDataRequestBuilderPtr requestBuilder,
            TLog log,
            const TString& fileSystemId,
            const TString& clientId,
            const TString& filePath,
            ui64 capacityBytes,
            TFlushConfig flushConfig)
        : Session(std::move(session))
        , Scheduler(std::move(scheduler))
        , Timer(std::move(timer))
        , Stats(std::move(stats))
        , RequestBuilder(std::move(requestBuilder))
        , FlushConfig(flushConfig)
        , Log(std::move(log))
        , LogTag(
              Sprintf("[f:%s][c:%s]", fileSystemId.c_str(), clientId.c_str()))
        , FileSystemId(fileSystemId)
    {
        auto createPersistentStorageResult =
            CreateFileRingBufferPersistentStorage(
                Stats,
                {.FilePath = filePath, .DataCapacity = capacityBytes});

        if (HasError(createPersistentStorageResult)) {
            ReportWriteBackCacheCorruptionError(
                TStringBuilder()
                << LogTag
                << " WriteBackCache persistent queue creation failed: "
                << createPersistentStorageResult.GetError());
            return;
        }

        PersistentStorage = createPersistentStorageResult.ExtractResult();

        // File ring buffer should be able to store any valid TWriteDataRequest.
        // Inability to store it will cause this and future requests to remain
        // in the pending queue forever (including requests with smaller size).
        // Should fit 1 MiB of data plus some headers (assume 1 KiB is enough).
        Y_ABORT_UNLESS(
            PersistentStorage->GetMaxSupportedAllocationByteCount() >=
            1024 * 1024 + 1016);

        Stats->ResetNonDerivativeCounters();

        TWriteDataEntryDeserializationStats deserializationStats;

        PersistentStorage->Visit(
            [&](TStringBuf serializedRequest)
            {
                auto entry = std::make_unique<TWriteDataEntry>(
                    serializedRequest,
                    deserializationStats,
                    this);

                if (entry->IsCorrupted()) {
                    // This may happen when a buffer was corrupted.
                    // We should add this entry to a queue like a normal entry
                    // because there is 1-by-1 correspondence between
                    // CachedEntriesPersistentQueue and CachedEntries.
                    CachedEntries.push_back(std::move(entry));
                } else {
                    RegisterWriteDataEntry(entry.get());
                    auto* nodeState = GetOrCreateNodeState(entry->GetNodeId());
                    AddCachedEntry(nodeState, std::move(entry));
                }
            });

        EmptyFlag = AllEntries.Empty();

        const auto persistentStorageStats = PersistentStorage->GetStats();

        STORAGE_INFO(
            LogTag << " WriteBackCache has been initialized "
            << "{\"FilePath\": " << filePath.Quote()
            << ", \"RawCapacityByteCount\": "
            << persistentStorageStats.RawCapacityByteCount
            << ", \"RawUsedByteCount\": "
            << persistentStorageStats.RawUsedByteCount
            << ", \"EntryCount\": "
            << persistentStorageStats.EntryCount << "}");

        if (deserializationStats.HasFailed()) {
            // Each deserialization failure event has been already reported
            // as a critical error - just write statistics to the log
            STORAGE_ERROR(
                LogTag << " WriteBackCache request deserialization failure "
                << "{\"EntrySizeMismatchCount\": "
                << deserializationStats.EntrySizeMismatchCount
                << ", \"ProtobufDeserializationErrorCount\": "
                << deserializationStats.ProtobufDeserializationErrorCount
                << "}");
        }

        if (persistentStorageStats.IsCorrupted) {
            ReportWriteBackCacheCorruptionError(
                LogTag + " WriteBackCache persistent queue is corrupted");
        }
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
        RequestFlushAllCachedData();
        ProcessQueuedOperations(guard);
        ScheduleAutomaticFlushIfNeeded();
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
        if (request->GetFileSystemId().empty()) {
            request->SetFileSystemId(callContext->FileSystemId);
        }

        auto error = TUtil::ValidateReadDataRequest(*request, FileSystemId);
        if (HasError(error)) {
            Y_DEBUG_ABORT_UNLESS(false, "%s", error.GetMessage().c_str());
            NProto::TReadDataResponse response;
            *response.MutableError() = error;
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
                self->DeleteNodeStateIfNeeded(nodeState);
                self->ProcessQueuedOperations(guard);
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
                self->QueuedOperations.ReadData.push_back(
                    std::move(pendingRequest));
            }
        };

        auto guard = Guard(Lock);

        auto* nodeState = GetOrCreateNodeState(nodeId);
        nodeState->RangeLock.LockRead(offset, end, std::move(locker));

        ProcessQueuedOperations(guard);

        return result;
    }

    TFuture<NProto::TWriteDataResponse> WriteData(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TWriteDataRequest> request)
    {
        if (request->GetFileSystemId().empty()) {
            request->SetFileSystemId(callContext->FileSystemId);
        }

        auto error = TUtil::ValidateWriteDataRequest(*request, FileSystemId);
        if (HasError(error)) {
            Y_DEBUG_ABORT_UNLESS(false, "%s", error.GetMessage().c_str());
            NProto::TWriteDataResponse response;
            *response.MutableError() = error;
            return MakeFuture(std::move(response));
        }

        auto entry = std::make_unique<TWriteDataEntry>(std::move(request));

        const auto nodeId = entry->GetNodeId();
        const auto offset = entry->GetOffset();
        const auto end = entry->GetEnd();

        Y_ABORT_UNLESS(offset < end);

        const auto serializedSize = entry->GetSerializedSize();
        const auto maxSerializedSize =
            PersistentStorage->GetMaxSupportedAllocationByteCount();

        Y_ABORT_UNLESS(
            serializedSize <= maxSerializedSize,
            "Serialized request size %lu is expected to be <= %lu",
            serializedSize,
            maxSerializedSize);

        auto unlocker =
            [ptr = weak_from_this(), nodeId, offset, end](const auto&)
        {
            if (auto self = ptr.lock()) {
                auto guard = Guard(self->Lock);
                auto* nodeState = self->GetNodeState(nodeId);
                nodeState->RangeLock.UnlockWrite(offset, end);
                self->DeleteNodeStateIfNeeded(nodeState);
                self->ProcessQueuedOperations(guard);
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
                    self->QueuedOperations.ShouldProcessPendingEntries = true;
                }
                self->PendingEntries.push_back(
                    std::unique_ptr<TWriteDataEntry>(entry));
            }
        };

        auto guard = Guard(Lock);

        entryPtr->SetPending(this);
        RegisterWriteDataEntry(entryPtr);
        EmptyFlag = false;

        auto* nodeState = GetOrCreateNodeState(nodeId);
        nodeState->RangeLock.LockWrite(offset, end, std::move(locker));

        ProcessQueuedOperations(guard);

        return future;
    }

    TFuture<void> FlushNodeData(ui64 nodeId)
    {
        auto guard = Guard(Lock);

        auto* nodeState = GetNodeStateOrNull(nodeId);
        if (nodeState == nullptr || nodeState->AllEntries.Empty()) {
            return NThreading::MakeFuture();
        }

        ui64 maxRequestId = nodeState->AllEntries.Back()->GetRequestId();

        if (!nodeState->FlushRequests.empty() &&
            nodeState->FlushRequests.back().RequestId >= maxRequestId)
        {
            // The previous FlushAllData already affects all entries
            return nodeState->FlushRequests.back().Promise.GetFuture();
        }

        auto future = nodeState->FlushRequests.emplace_back(maxRequestId)
                          .Promise.GetFuture();

        if (EnqueueFlushIfNeeded(nodeState)) {
            ProcessQueuedOperations(guard);
        }

        return future;
    }

    TFuture<void> FlushAllData()
    {
        auto guard = Guard(Lock);

        if (AllEntries.Empty()) {
            return NThreading::MakeFuture();
        }

        ui64 maxRequestId = AllEntries.Back()->GetRequestId();

        if (!FlushAllRequests.empty() &&
            FlushAllRequests.back().RequestId >= maxRequestId)
        {
            // The previous FlushAllData already affects all entries
            return FlushAllRequests.back().Promise.GetFuture();
        }

        auto future =
            FlushAllRequests.emplace_back(maxRequestId).Promise.GetFuture();

        bool queuedOperationsUpdated = false;
        for (auto nodeId: NodesWithNewEntries) {
            queuedOperationsUpdated |=
                EnqueueFlushIfNeeded(GetNodeState(nodeId));
        }
        NodesWithNewEntries.clear();

        if (queuedOperationsUpdated) {
            ProcessQueuedOperations(guard);
        }

        return future;
    }

    bool IsEmpty() const
    {
        return EmptyFlag.load();
    }

    ui64 AcquireNodeStateRef()
    {
        with_lock (Lock) {
            ui64 refId = NextRequestId++;
            NodeStateRefs.insert(refId);
            return refId;
        }
    }

    void ReleaseNodeStateRef(ui64 refId)
    {
        with_lock (Lock) {
            const bool erased = static_cast<bool>(NodeStateRefs.erase(refId));
            Y_DEBUG_ABORT_UNLESS(erased);

            const ui64 minRefId =
                NodeStateRefs.empty() ? Max<ui64>() : *NodeStateRefs.begin();

            for (auto it = DeletedNodeStates.begin();
                 it != DeletedNodeStates.end() && it->first <= minRefId;)
            {
                EraseNodeState(it->second);
                it = DeletedNodeStates.erase(it);
            }
        }
    }

    ui64 GetCachedNodeSize(ui64 nodeId) const
    {
        with_lock (Lock) {
            const auto* nodeStatePtr = NodeStates.FindPtr(nodeId);
            return nodeStatePtr ? (*nodeStatePtr)->CachedNodeSize : 0;
        }
    }

    void SetCachedNodeSize(ui64 nodeId, ui64 size)
    {
        with_lock (Lock) {
            if (auto* nodeStatePtr = NodeStates.FindPtr(nodeId)) {
                (*nodeStatePtr)->CachedNodeSize = size;
            }
        }
    }

private:
    // Check and enqueue Flush operation for the given node if needed.
    // If the node should be flushed and there is no flush currently executing
    // for the node, mark the node's flush state as executing and add it into
    // |PendingOperations.Flush| so that ExecutePendingOperations will start it.
    // Returns true when the node was enqueued for flushing, false otherwise.
    // should be protected by |Lock|
    bool EnqueueFlushIfNeeded(TNodeState* nodeState)
    {
        if (nodeState->FlushState.Executing) {
            return false;
        }

        const ui64 maxFlushAllRequestId =
            FlushAllRequests.empty() ? 0 : FlushAllRequests.back().RequestId;

        if (nodeState->ShouldFlush(maxFlushAllRequestId)) {
            nodeState->FlushState.Executing = true;
            QueuedOperations.Flush.push_back(nodeState);
            return true;
        }

        return false;
    }

    // should be protected by |Lock|
    void RequestFlushAllCachedData()
    {
        for (auto nodeId: NodesWithNewCachedEntries) {
            auto* nodeState = GetNodeState(nodeId);
            nodeState->AutomaticFlushRequestId = nodeState->MaxCachedRequestId;
            EnqueueFlushIfNeeded(nodeState);
        }

        NodesWithNewCachedEntries.clear();
    }

    // should be protected by |Lock|
    void RegisterWriteDataEntry(TWriteDataEntry* entry)
    {
        entry->SetRequestId(NextRequestId++);
        AllEntries.PushBack(entry);

        NodesWithNewEntries.insert(entry->GetNodeId());

        auto* nodeState = GetOrCreateNodeState(entry->GetNodeId());
        nodeState->AllEntries.PushBack(entry);
    }

    // should be protected by |Lock|
    TNodeState* GetNodeStateOrNull(ui64 nodeId) const
    {
        auto it = NodeStates.find(nodeId);
        return it != NodeStates.end() && it->second->DeletionId == 0
                   ? it->second.get()
                   : nullptr;
    }

    // should be protected by |Lock|
    TNodeState* GetOrCreateNodeState(ui64 nodeId)
    {
        auto& ptr = NodeStates[nodeId];
        if (!ptr) {
            ptr = std::make_unique<TNodeState>(nodeId);
            Stats->IncrementNodeCount();
        }
        if (ptr->DeletionId != 0) {
            // Revive deleted node state
            auto erased = DeletedNodeStates.erase(ptr->DeletionId);
            Y_DEBUG_ABORT_UNLESS(erased);
            ptr->DeletionId = 0;
        }
        return ptr.get();
    }

    // should be protected by |Lock|
    TNodeState* GetNodeState(ui64 nodeId)
    {
        const auto& ptr = NodeStates[nodeId];
        Y_ABORT_UNLESS(ptr);
        Y_ABORT_UNLESS(ptr->DeletionId == 0);
        return ptr.get();
    }

    // should be protected by |Lock|
    void EraseNodeState(ui64 nodeId)
    {
        const auto erased = static_cast<bool>(NodeStates.erase(nodeId));
        Y_DEBUG_ABORT_UNLESS(erased);
        Stats->DecrementNodeCount();
    }

    // should be protected by |Lock|
    void DeleteNodeStateIfNeeded(TNodeState* nodeState)
    {
        Y_DEBUG_ABORT_UNLESS(nodeState->DeletionId == 0);

        if (!nodeState->CanBeDeleted()) {
            return;
        }

        NodesWithNewCachedEntries.erase(nodeState->NodeId);
        NodesWithNewEntries.erase(nodeState->NodeId);

        if (NodeStateRefs.empty()) {
            EraseNodeState(nodeState->NodeId);
        } else {
            nodeState->DeletionId = NextRequestId++;
            DeletedNodeStates[nodeState->DeletionId] = nodeState->NodeId;
        }
    }

    // NOLINTNEXTLINE(misc-no-recursion)
    void ProcessQueuedOperations(TGuard<TMutex>& guard)
    {
        Y_DEBUG_ABORT_UNLESS(guard.WasAcquired());

        // Prevent recursive calls
        if (QueuedOperations.Executing) {
            return;
        }

        QueuedOperations.Executing = true;

        while (true) {
            if (QueuedOperations.ShouldProcessPendingEntries) {
                QueuedOperations.ShouldProcessPendingEntries = false;
                ProcessPendingEntries();
            }

            if (QueuedOperations.Empty()) {
                QueuedOperations.Executing = false;
                return;
            }

            TVector<TPendingReadDataRequest> readData;
            TVector<TNodeState*> flush;
            TVector<TPromise<NProto::TWriteDataResponse>> writeDataCompleted;
            TVector<TPromise<void>> flushCompleted;

            swap(readData, QueuedOperations.ReadData);
            swap(flush, QueuedOperations.Flush);
            swap(writeDataCompleted, QueuedOperations.WriteDataCompleted);
            swap(flushCompleted, QueuedOperations.FlushCompleted);

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

            auto allocationResult = PersistentStorage->Alloc(serializedSize);

            Y_ABORT_UNLESS(
                !HasError(allocationResult),
                "Failed to allocate memory in the persistent storage: %s",
                allocationResult.GetError().GetMessage().c_str());

            char* allocationPtr = allocationResult.GetResult();
            if (allocationPtr == nullptr) {
                RequestFlushAllCachedData();
                break;
            }

            entry->Serialize({allocationPtr, serializedSize});

            PersistentStorage->Commit();

            entry->MoveRequestBuffer(allocationPtr, QueuedOperations, this);

            auto* nodeState = GetNodeState(entry->GetNodeId());
            AddCachedEntry(nodeState, std::move(entry));

            // The entry transitioned from Pending to Cached status may be
            // requested for flushing. Recheck Flush condition for the node.
            EnqueueFlushIfNeeded(nodeState);

            PendingEntries.pop_front();
        }
    }

    TVector<TWriteDataEntryPart> CalculateDataPartsToReadAndFillBuffer(
        ui64 nodeId,
        ui64 offset,
        ui64 length,
        TString* buffer)
    {
        with_lock (Lock) {
            auto* nodeState = GetNodeStateOrNull(nodeId);
            if (nodeState == nullptr) {
                return {};
            }
            if (nodeState->CachedEntryIntervalMap.empty()) {
                return {};
            }
            const auto last = nodeState->CachedEntryIntervalMap.rbegin();
            if (last->second.End <= offset) {
                return {};
            }

            auto parts = TUtil::CalculateDataPartsToRead(
                nodeState->CachedEntryIntervalMap,
                offset,
                length);

            // TODO(nasonov): it is possible to completely get rid of copy
            // here by referencing the buffer directly in the cache and
            // adding reference count in order to prevent evicting buffer
            // from the cache
            *buffer = TString(Min(length, last->second.End - offset), 0);
            for (const auto& part: parts) {
                ReadDataPart(part, offset, buffer);
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
            parts.back().GetEnd() != offset + length)
        {
            return false;
        }

        for (size_t i = 1; i < parts.size(); i++) {
            Y_DEBUG_ABORT_UNLESS(parts[i - 1].GetEnd() <= parts[i].Offset);
            if (parts[i - 1].GetEnd() != parts[i].Offset) {
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
            Y_ABORT_UNLESS(state.Buffer.size() == state.Length);
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

        if (state.Buffer.empty()) {
            // Cache miss
            state.Promise.SetValue(std::move(response));
            return;
        }

        if (response.GetBuffer().empty()) {
            *response.MutableBuffer() = std::move(state.Buffer);
            state.Promise.SetValue(std::move(response));
            return;
        }

        char* responseBufferData =
            response.MutableBuffer()->begin() + response.GetBufferOffset();

        // Determine if it is better to apply cached data parts on
        // top of the ReadData response or copy non-cached data from
        // the response to the buffer with cached data parts
        bool useResponseBuffer = responseBufferLength >= state.Buffer.length();
        if (responseBufferLength == state.Buffer.length()) {
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

        size_t entryCount = 0;

        auto batchBuilder = RequestBuilder->CreateWriteDataRequestBatchBuilder(
            nodeState->NodeId);

        {
            auto guard = Guard(Lock);

            for (auto* entry: nodeState->CachedEntries) {
                if (!batchBuilder->AddRequest(
                        entry->GetHandle(),
                        entry->GetOffset(),
                        entry->GetBuffer()))
                {
                    break;
                }
                entryCount++;
                entry->StartFlush(this);
            }
        }

        auto writeDataBatch = batchBuilder->Build();

        // Flush cannot be scheduled when CachedEntries is empty
        Y_ABORT_UNLESS(writeDataBatch.AffectedRequestCount > 0);
        Y_ABORT_UNLESS(!writeDataBatch.Requests.empty());
        Y_ABORT_UNLESS(writeDataBatch.AffectedRequestCount == entryCount);

        Stats->FlushStarted();

        nodeState->FlushState.AffectedWriteDataEntriesCount = entryCount;
        nodeState->FlushState.WriteRequests =
            std::move(writeDataBatch.Requests);
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
            auto callContext = MakeIntrusive<TCallContext>(FileSystemId);
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

    // should be protected by |Lock|
    template <class TTag>
    void EnqueueFlushCompletions(
        TDeque<TFlushRequest>& flushRequests,
        TIntrusiveList<TWriteDataEntry, TTag>& entries)
    {
        if (flushRequests.empty()) {
            return;
        }
        if (entries.Empty()) {
            for (auto& flushRequest: flushRequests) {
                QueuedOperations.FlushCompleted.push_back(
                    std::move(flushRequest.Promise));
            }
            flushRequests.clear();
        } else {
            const ui64 minRequestId = entries.Front()->GetRequestId();
            while (!flushRequests.empty() &&
                   flushRequests.front().RequestId < minRequestId)
            {
                QueuedOperations.FlushCompleted.push_back(
                    std::move(flushRequests.front().Promise));
                flushRequests.pop_front();
            }
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
            entry->FinishFlush(this);

            nodeState->AllEntries.Remove(entry);
            AllEntries.Remove(entry);
        }

        EnqueueFlushCompletions(
            nodeState->FlushRequests,
            nodeState->AllEntries);

        EnqueueFlushCompletions(FlushAllRequests, AllEntries);

        EmptyFlag = AllEntries.Empty();

        // Clear flushed entries from the persistent queue
        while (!CachedEntries.empty() && CachedEntries.front()->IsFlushed()) {
            auto* entry = CachedEntries.front().get();
            PersistentStorage->Free(entry->GetAllocationPtr());
            entry->Complete(this);
            CachedEntries.pop_front();
            QueuedOperations.ShouldProcessPendingEntries = true;
        }

        nodeState->FlushState.Executing = false;
        if (!EnqueueFlushIfNeeded(nodeState)) {
            DeleteNodeStateIfNeeded(nodeState);
        }

        ProcessQueuedOperations(guard);
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

    // should be protected by |Lock|
    void AddCachedEntry(
        TNodeState* nodeState,
        std::unique_ptr<TWriteDataEntry> entry)
    {
        Y_ABORT_UNLESS(nodeState != nullptr);
        Y_ABORT_UNLESS(nodeState->NodeId == entry->GetNodeId());

        nodeState->CachedEntryIntervalMap.Add(entry.get());
        nodeState->CachedEntries.push_back(entry.get());
        nodeState->MaxCachedRequestId =
            Max(nodeState->MaxCachedRequestId, entry->GetRequestId());
        nodeState->CachedNodeSize =
            Max(nodeState->CachedNodeSize, entry->GetEnd());
        NodesWithNewCachedEntries.insert(nodeState->NodeId);
        CachedEntries.push_back(std::move(entry));
    }

    // should be protected by |Lock|
    static TWriteDataEntry* RemoveFrontCachedEntry(TNodeState* nodeState)
    {
        Y_ABORT_UNLESS(nodeState != nullptr);
        Y_ABORT_UNLESS(!nodeState->CachedEntries.empty());

        auto* entry = nodeState->CachedEntries.front();
        nodeState->CachedEntries.pop_front();
        nodeState->CachedEntryIntervalMap.Remove(entry);

        return entry;
    }

    TIntrusiveList<TWriteDataEntry>& GetEntryListByStatus(
        EWriteDataRequestStatus status)
    {
        switch (status) {
            case EWriteDataRequestStatus::Pending:
                return PendingStatusEntries;
            case EWriteDataRequestStatus::Cached:
                return CachedStatusEntries;
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
        TLog log,
        const TString& fileSystemId,
        const TString& clientId,
        const TString& filePath,
        ui64 capacityBytes,
        TDuration automaticFlushPeriod,
        TDuration flushRetryPeriod,
        ui32 maxWriteRequestSize,
        ui32 maxWriteRequestsCount,
        ui32 maxSumWriteRequestsSize,
        bool zeroCopyWriteEnabled)
    : Impl(new TImpl(
          std::move(session),
          std::move(scheduler),
          std::move(timer),
          std::move(stats),
          NWriteBackCache::CreateWriteDataRequestBuilder(
              {.FileSystemId = fileSystemId,
               .MaxWriteRequestSize = maxWriteRequestSize,
               .MaxWriteRequestsCount = maxWriteRequestsCount,
               .MaxSumWriteRequestsSize = maxSumWriteRequestsSize,
               .ZeroCopyWriteEnabled = zeroCopyWriteEnabled}),
          std::move(log),
          fileSystemId,
          clientId,
          filePath,
          capacityBytes,
          {.AutomaticFlushPeriod = automaticFlushPeriod,
           .FlushRetryPeriod = flushRetryPeriod}))
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

bool TWriteBackCache::IsEmpty() const
{
    return Impl->IsEmpty();
}

ui64 TWriteBackCache::AcquireNodeStateRef()
{
    return Impl->AcquireNodeStateRef();
}

void TWriteBackCache::ReleaseNodeStateRef(ui64 refId)
{
    Impl->ReleaseNodeStateRef(refId);
}

ui64 TWriteBackCache::GetCachedNodeSize(ui64 nodeId) const
{
    return Impl->GetCachedNodeSize(nodeId);
}

void TWriteBackCache::SetCachedNodeSize(ui64 nodeId, ui64 size)
{
    Impl->SetCachedNodeSize(nodeId, size);
}

////////////////////////////////////////////////////////////////////////////////

TWriteBackCache::TWriteDataEntry::TWriteDataEntry(
        std::shared_ptr<NProto::TWriteDataRequest> request)
    : PendingRequest(std::move(request))
    , ByteCount(
          NStorage::CalculateByteCount(*PendingRequest) -
          PendingRequest->GetBufferOffset())
    , CachedPromise(NewPromise<NProto::TWriteDataResponse>())
{
    Y_ABORT_UNLESS(ByteCount > 0);
}

TWriteBackCache::TWriteDataEntry::TWriteDataEntry(
        TStringBuf serializedRequest,
        TWriteDataEntryDeserializationStats& deserializationStats,
        TImpl* impl)
    : ByteCount(SaturationSub(
          serializedRequest.size(),
          sizeof(TCachedWriteDataRequest)))
{
    deserializationStats.EntryCount++;

    if (ByteCount == 0) {
        deserializationStats.EntrySizeMismatchCount++;
        ReportWriteBackCacheCorruptionError(Sprintf(
            "TWriteDataEntry deserialization error: entry size is too small, "
            "expected: > %lu, actual: %lu",
            sizeof(TCachedWriteDataRequest),
            serializedRequest.size()));
        SetStatus(EWriteDataRequestStatus::Corrupted, impl);
        return;
    }

    const auto* allocationPtr =
        reinterpret_cast<const TCachedWriteDataRequest*>(
            serializedRequest.data());

    CachedRequest = allocationPtr;
    SetStatus(EWriteDataRequestStatus::Cached, impl);
}

size_t TWriteBackCache::TWriteDataEntry::GetSerializedSize() const
{
    return sizeof(TCachedWriteDataRequest) + GetByteCount();
}

void TWriteBackCache::TWriteDataEntry::SetPending(TImpl* impl)
{
    Y_ABORT_UNLESS(Status == EWriteDataRequestStatus::Initial);
    Y_ABORT_UNLESS(PendingRequest);
    SetStatus(EWriteDataRequestStatus::Pending, impl);
}

void TWriteBackCache::TWriteDataEntry::Serialize(
    std::span<char> allocation) const
{
    Y_ABORT_UNLESS(PendingRequest);

    Y_ABORT_UNLESS(
        allocation.size() == sizeof(TCachedWriteDataRequest) + ByteCount,
        "Invalid allocation size, expected: %lu, actual: %lu",
        sizeof(TCachedWriteDataRequest) + ByteCount,
        allocation.size());

    auto* cachedRequest =
        reinterpret_cast<TCachedWriteDataRequest*>(allocation.data());

    cachedRequest->NodeId = PendingRequest->GetNodeId();
    cachedRequest->Handle = PendingRequest->GetHandle();
    cachedRequest->Offset = PendingRequest->GetOffset();

    TBufferWriter writer(allocation.subspan(sizeof(TCachedWriteDataRequest)));

    if (PendingRequest->GetIovecs().empty()) {
        writer.Write(TStringBuf(PendingRequest->GetBuffer())
                         .Skip(PendingRequest->GetBufferOffset()));
    } else {
        for (const auto& iovec: PendingRequest->GetIovecs()) {
            writer.Write(TStringBuf(
                reinterpret_cast<const char*>(iovec.GetBase()),
                iovec.GetLength()));
        }
    }

    Y_ABORT_UNLESS(
        writer.TargetBuffer.empty(),
        "Buffer is expected to be written completely");
}

void TWriteBackCache::TWriteDataEntry::MoveRequestBuffer(
    const void* allocationPtr,
    TQueuedOperations& pendingOperations,
    TImpl* impl)
{
    Y_ABORT_UNLESS(PendingRequest);
    Y_ABORT_UNLESS(CachedRequest == nullptr);

    CachedRequest =
        reinterpret_cast<const TCachedWriteDataRequest*>(allocationPtr);
    PendingRequest.reset();

    SetStatus(EWriteDataRequestStatus::Cached, impl);

    if (CachedPromise.Initialized()) {
        pendingOperations.WriteDataCompleted.push_back(
            std::move(CachedPromise));
    }
}

void TWriteBackCache::TWriteDataEntry::StartFlush(TImpl* impl)
{
    Y_ABORT_UNLESS(Status == EWriteDataRequestStatus::Cached);
    SetStatus(EWriteDataRequestStatus::Flushing, impl);
}

void TWriteBackCache::TWriteDataEntry::FinishFlush(
    TImpl* impl)
{
    Y_ABORT_UNLESS(Status == EWriteDataRequestStatus::Flushing);
    SetStatus(EWriteDataRequestStatus::Flushed, impl);
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
        entry->GetOffset(),
        entry->GetEnd(),
        [this, entry](auto it)
        {
            auto prev = it->second;
            TBase::Remove(it);

            if (prev.Begin < entry->GetOffset()) {
                TBase::Add(prev.Begin, entry->GetOffset(), prev.Value);
            }

            if (entry->GetEnd() < prev.End) {
                TBase::Add(entry->GetEnd(), prev.End, prev.Value);
            }
        });

    TBase::Add(entry->GetOffset(), entry->GetEnd(), entry);
}

void TWriteBackCache::TWriteDataEntryIntervalMap::Remove(TWriteDataEntry* entry)
{
    TBase::VisitOverlapping(
        entry->GetOffset(),
        entry->GetEnd(),
        [&](auto it)
        {
            if (it->second.Value == entry) {
                TBase::Remove(it);
            }
        });
}

}   // namespace NCloud::NFileStore::NFuse
