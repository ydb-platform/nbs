#include "write_back_cache.h"

#include "node_cache.h"
#include "node_state.h"
#include "node_state_holder.h"
#include "persistent_storage.h"
#include "sequence_id_generator.h"
#include "utils.h"
#include "write_data_request_builder.h"
#include "write_data_request.h"
#include "write_data_request_manager.h"

#include <cloud/filestore/libs/diagnostics/critical_events.h>
#include <cloud/filestore/libs/service/context.h>
#include <cloud/filestore/libs/storage/core/helpers.h>

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

#include <span>

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
};

////////////////////////////////////////////////////////////////////////////////

struct TFlushConfig
{
    TDuration AutomaticFlushPeriod;
    TDuration FlushRetryPeriod;
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

}   // namespace

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
    const IFileStorePtr Session;
    const ISchedulerPtr Scheduler;
    const ITimerPtr Timer;
    const IWriteBackCacheStatsPtr Stats;
    const IWriteDataRequestBuilderPtr RequestBuilder;
    const ISequenceIdGeneratorPtr SequenceIdGenerator;
    const TFlushConfig FlushConfig;
    const TLog Log;
    const TString LogTag;
    const TString FileSystemId;

    // All fields below should be protected by this lock
    TMutex Lock;

    TWriteDataRequestManager RequestManager;
    IPersistentStoragePtr PersistentStorage;

    // WriteData entries and Flush states grouped by nodeId
    TNodeStateHolder NodeStates;

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
        , SequenceIdGenerator(std::make_shared<TSequenceIdGenerator>())
        , FlushConfig(flushConfig)
        , Log(std::move(log))
        , LogTag(
              Sprintf("[f:%s][c:%s]", fileSystemId.c_str(), clientId.c_str()))
        , FileSystemId(fileSystemId)
        , NodeStates(Stats)
    {
        auto createPersistentStorageResult =
            CreateFileRingBufferPersistentStorage(
                Stats,
                {.FilePath = filePath, .DataCapacity = capacityBytes});

        if (HasError(createPersistentStorageResult)) {
            ReportWriteBackCacheCorruptionError(
                TStringBuilder()
                << LogTag
                << " WriteBackCache persistent storage initialization failed: "
                << createPersistentStorageResult.GetError()
                << ", FilePath: " << filePath.Quote());
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

        RequestManager = TWriteDataRequestManager(
            SequenceIdGenerator,
            PersistentStorage,
            Timer,
            Stats);

        auto initStatus = RequestManager.Init(
            [&](std::unique_ptr<TCachedWriteDataRequest> entry)
            {
                auto& nodeState =
                    NodeStates.GetOrCreateNodeState(entry->GetNodeId());
                NodesWithNewEntries.insert(entry->GetNodeId());
                AddCachedEntry(nodeState, std::move(entry));
            });

        if (!initStatus) {
            ReportWriteBackCacheCorruptionError(
                TStringBuilder()
                << LogTag
                << " WriteBackCache failed to deserialize requests from the "
                   "persistent storage, FilePath: "
                << filePath.Quote());
            return;
        }

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

    TFuture<NProto::TReadDataResponse> ReadData(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TReadDataRequest> request)
    {
        if (request->GetFileSystemId().empty()) {
            request->SetFileSystemId(callContext->FileSystemId);
        }

        auto error = TUtils::ValidateReadDataRequest(*request, FileSystemId);
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
                auto* nodeState = self->NodeStates.GetNodeState(nodeId);
                Y_ABORT_UNLESS(nodeState);
                nodeState->RangeLock.UnlockRead(offset, end);
                self->DeleteNodeStateIfNeeded(nodeState);
                self->ProcessQueuedOperations(guard);
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
                self->QueuedOperations.ReadData.push_back(
                    std::move(pendingRequest));
            }
        };

        auto guard = Guard(Lock);

        auto& nodeState = NodeStates.GetOrCreateNodeState(nodeId);
        nodeState.RangeLock.LockRead(offset, end, std::move(locker));

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

        auto error = TUtils::ValidateWriteDataRequest(*request, FileSystemId);
        if (HasError(error)) {
            Y_DEBUG_ABORT_UNLESS(false, "%s", error.GetMessage().c_str());
            NProto::TWriteDataResponse response;
            *response.MutableError() = error;
            return MakeFuture(std::move(response));
        }

        const auto nodeId = request->GetNodeId();
        const auto offset = request->GetOffset();
        const auto end = NStorage::CalculateByteCount(*request) -
                         request->GetBufferOffset() + offset;

        Y_ABORT_UNLESS(offset < end);

        auto unlocker =
            [ptr = weak_from_this(), nodeId, offset, end](const auto&)
        {
            if (auto self = ptr.lock()) {
                auto guard = Guard(self->Lock);
                auto* nodeState = self->NodeStates.GetNodeState(nodeId);
                Y_ABORT_UNLESS(nodeState);
                nodeState->RangeLock.UnlockWrite(offset, end);
                self->DeleteNodeStateIfNeeded(nodeState);
                self->ProcessQueuedOperations(guard);
            }
        };

        auto promise = NewPromise<NProto::TWriteDataResponse>();
        auto future = promise.GetFuture();

        future.Subscribe(std::move(unlocker));

        auto locker =
            [ptr = weak_from_this(), request = std::move(request), promise = std::move(promise)]() mutable
        {
            auto self = ptr.lock();
            // Lock action is invoked immediately or from
            // UnlockRead/UnlockWrite calls that can be made only when
            // TImpl is alive
            Y_ABORT_UNLESS(self);

            self->NodesWithNewEntries.insert(request->GetNodeId());

            auto res = self->RequestManager.AddRequest(std::move(request));
            std::visit([&](auto res)
            {
                self->DoAddRequest(std::move(res), std::move(promise));
            }, std::move(res));
        };

        auto guard = Guard(Lock);

        auto& nodeState = NodeStates.GetOrCreateNodeState(nodeId);
        nodeState.RangeLock.LockWrite(offset, end, std::move(locker));

        ProcessQueuedOperations(guard);

        return future;
    }

    TFuture<void> FlushNodeData(ui64 nodeId)
    {
        auto guard = Guard(Lock);

        auto* nodeState = NodeStates.GetNodeState(nodeId);
        if (nodeState == nullptr ||
            !nodeState->Cache.HasPendingOrUnflushedRequests())
        {
            return NThreading::MakeFuture();
        }

        const ui64 maxRequestId =
            nodeState->Cache.GetMaxPendingOrUnflushedSequenceId();

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

        if (!RequestManager.HasPendingOrUnflushedRequests()) {
            return NThreading::MakeFuture();
        }

        ui64 maxRequestId = RequestManager.GetMaxPendingOrUnflushedSequenceId();

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
            auto* nodeState = NodeStates.GetNodeState(nodeId);
            if (nodeState) {
                queuedOperationsUpdated |= EnqueueFlushIfNeeded(nodeState);
            }
        }
        NodesWithNewEntries.clear();

        if (queuedOperationsUpdated) {
            ProcessQueuedOperations(guard);
        }

        return future;
    }

    bool IsEmpty() const
    {
        with_lock (Lock) {
            return !RequestManager.HasPendingOrUnflushedRequests();
        }
    }

    ui64 AcquireNodeStateRef()
    {
        with_lock (Lock) {
            return NodeStates.Ref();
        }
    }

    void ReleaseNodeStateRef(ui64 refId)
    {
        with_lock (Lock) {
            NodeStates.Unref(refId);
        }
    }

    ui64 GetCachedNodeSize(ui64 nodeId) const
    {
        with_lock (Lock) {
            const auto* nodeState = NodeStates.GetPinnedNodeState(nodeId);
            return nodeState ? nodeState->CachedNodeSize : 0;
        }
    }

    void SetCachedNodeSize(ui64 nodeId, ui64 size)
    {
        with_lock (Lock) {
            if (auto* nodeState = NodeStates.GetPinnedNodeState(nodeId)) {
                nodeState->CachedNodeSize = size;
            }
        }
    }

private:
    void DoAddRequest(
        std::unique_ptr<TCachedWriteDataRequest> request,
        TPromise<NProto::TWriteDataResponse> promise)
    {
        auto& nodeState = NodeStates.GetOrCreateNodeState(request->GetNodeId());
        QueuedOperations.WriteDataCompleted.push_back(std::move(promise));
        AddCachedEntry(nodeState, std::move(request));
    }

    void DoAddRequest(
        std::unique_ptr<TPendingWriteDataRequest> request,
        TPromise<NProto::TWriteDataResponse> promise)
    {
        request->AccessPromise() = std::move(promise);
        auto& nodeState =
            NodeStates.GetOrCreateNodeState(request->GetRequest().GetNodeId());
        nodeState.Cache.EnqueuePendingRequest(std::move(request));
        RequestFlushAllCachedData();
    }

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
            auto* nodeState = NodeStates.GetNodeState(nodeId);
            if (nodeState && nodeState->Cache.HasUnflushedRequests()) {
                nodeState->AutomaticFlushRequestId =
                    nodeState->Cache.GetMaxUnflushedSequenceId();
                EnqueueFlushIfNeeded(nodeState);
            }
        }

        NodesWithNewCachedEntries.clear();
    }

    // should be protected by |Lock|
    void DeleteNodeStateIfNeeded(TNodeState* nodeState)
    {
        if (!nodeState->CanBeDeleted()) {
            return;
        }

        NodeStates.EraseNodeState(nodeState->NodeId);
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
        while (true) {
            auto cachedRequest = RequestManager.TryProcessPendingRequest();
            if (!cachedRequest) {
                break;
            }

            auto* nodeState =
                NodeStates.GetNodeState(cachedRequest->GetNodeId());

            Y_ABORT_UNLESS(nodeState);
            Y_ABORT_UNLESS(nodeState->Cache.HasPendingRequests());
            auto pendingRequest = nodeState->Cache.DequeuePendingRequest();

            Y_ABORT_UNLESS(
                pendingRequest->GetSequenceId() ==
                cachedRequest->GetSequenceId());

            QueuedOperations.WriteDataCompleted.push_back(
                std::move(pendingRequest->AccessPromise()));

            AddCachedEntry(*nodeState, std::move(cachedRequest));

            // The entry transitioned from Pending to Cached status may be
            // requested for flushing. Recheck Flush condition for the node.
            EnqueueFlushIfNeeded(nodeState);
        }
    }

    TVector<TCachedDataPart> CalculateDataPartsToReadAndFillBuffer(
        ui64 nodeId,
        ui64 offset,
        ui64 length,
        TString* buffer)
    {
        with_lock (Lock) {
            auto* nodeState = NodeStates.GetNodeState(nodeId);
            if (nodeState == nullptr) {
                return {};
            }
            auto data = nodeState->Cache.GetCachedData(offset, length);
            if (data.ReadDataByteCount == 0) {
                return {};
            }
            *buffer = TString(data.ReadDataByteCount, 0);
            for (const auto& part: data.Parts) {
                part.Data.copy(
                    buffer->begin() + part.RelativeOffset,
                    part.Data.size());
            }
            return data.Parts;
        }
    }

    static bool IsIntervalFullyCoveredByParts(
        const TVector<TCachedDataPart>& parts,
        ui64 length)
    {
        if (parts.empty() || parts.front().RelativeOffset != 0 ||
            parts.back().RelativeOffset + parts.back().Data.size() != length)
        {
            return false;
        }

        for (size_t i = 1; i < parts.size(); i++) {
            const ui64 prevEnd =
                parts[i - 1].RelativeOffset + parts[i - 1].Data.size();

            Y_DEBUG_ABORT_UNLESS(prevEnd <= parts[i].RelativeOffset);
            if (prevEnd != parts[i].RelativeOffset) {
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
        TVector<TCachedDataPart> Parts;
        TPromise<NProto::TReadDataResponse> Promise;
    };

    void StartPendingReadDataRequest(TPendingReadDataRequest request)
    {
        auto nodeId = request.Request->GetNodeId();

        TReadDataState state;
        state.StartingFromOffset = request.Request->GetOffset();
        state.Length = request.Request->GetLength();
        state.Promise = std::move(request.Promise);

        state.Parts = CalculateDataPartsToReadAndFillBuffer(
            nodeId,
            state.StartingFromOffset,
            state.Length,
            &state.Buffer);

        if (IsIntervalFullyCoveredByParts(
                state.Parts,
                state.Length))
        {
            // Serve request from cache
            Y_ABORT_UNLESS(state.Buffer.size() == state.Length);
            NProto::TReadDataResponse response;
            response.SetBuffer(std::move(state.Buffer));
            Stats->AddReadDataStats(EReadDataRequestCacheStatus::FullHit);
            state.Promise.SetValue(std::move(response));
            return;
        }

        auto cacheState = state.Parts.empty()
            ? EReadDataRequestCacheStatus::Miss
            : EReadDataRequestCacheStatus::PartialHit;

        Stats->AddReadDataStats(cacheState);

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

        if (responseBufferLength < state.Buffer.size()) {
            response.SetBuffer(response.GetBuffer()
                                   .substr(response.GetBufferOffset())
                                   .resize(state.Buffer.size(), 0));
            response.SetBufferOffset(0);
        }

        char* responseBufferData =
            response.MutableBuffer()->begin() + response.GetBufferOffset();

        // be careful and don't take data from parts here as it
        // may be already deleted
        for (const auto& part: state.Parts) {
            const char* from = state.Buffer.data() + part.RelativeOffset;
            char* to = responseBufferData + part.RelativeOffset;
            MemCopy(to, from, part.Data.size());
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

            nodeState->Cache.VisitUnflushedRequests(
                [&](const TCachedWriteDataRequest* entry)
                {
                    if (batchBuilder->AddRequest(
                            entry->GetHandle(),
                            entry->GetOffset(),
                            entry->GetBuffer()))
                    {
                        entryCount++;
                        return true;
                    }
                    return false;
                });
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
    void EnqueueFlushCompletions(
        TDeque<TFlushRequest>& flushRequests,
        ui64 minRequestId)
    {
        while (!flushRequests.empty() &&
               flushRequests.front().RequestId < minRequestId)
        {
            QueuedOperations.FlushCompleted.push_back(
                std::move(flushRequests.front().Promise));
            flushRequests.pop_front();
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

            Y_ABORT_UNLESS(nodeState->Cache.HasUnflushedRequests());

            auto* request =
                nodeState->Cache.MoveFrontUnflushedRequestToFlushed();

            RequestManager.SetFlushed(request);
        }

        EnqueueFlushCompletions(
            nodeState->FlushRequests,
            nodeState->Cache.GetMinPendingOrUnflushedSequenceId(Max<ui64>()));

        EnqueueFlushCompletions(
            FlushAllRequests,
            RequestManager.GetMinPendingOrUnflushedSequenceId());

        // Clear flushed entries from the persistent queue
        while (nodeState->Cache.HasFlushedRequests()) {
            auto request = nodeState->Cache.DequeueFlushedRequest();
            RequestManager.Evict(std::move(request));
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
        TNodeState& nodeState,
        std::unique_ptr<TCachedWriteDataRequest> entry)
    {
        Y_ABORT_UNLESS(nodeState.NodeId == entry->GetNodeId());

        nodeState.CachedNodeSize =
            Max(nodeState.CachedNodeSize, entry->GetEnd());

        nodeState.Cache.EnqueueUnflushedRequest(std::move(entry));

        NodesWithNewCachedEntries.insert(nodeState.NodeId);
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

}   // namespace NCloud::NFileStore::NFuse
