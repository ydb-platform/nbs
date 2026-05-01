#include "write_back_cache.h"

#include "node_flush_state.h"
#include "persistent_storage.h"
#include "read_response_builder.h"
#include "sequence_id_generator.h"
#include "utils.h"
#include "write_back_cache_state.h"
#include "write_back_cache_stats.h"
#include "write_data_request_builder.h"

#include <cloud/filestore/libs/diagnostics/critical_events.h>
#include <cloud/filestore/libs/diagnostics/metrics/metric.h>
#include <cloud/filestore/libs/diagnostics/module_stats.h>
#include <cloud/filestore/libs/service/context.h>
#include <cloud/filestore/libs/service/request.h>

#include <atomic>

namespace NCloud::NFileStore::NFuse {

using namespace NThreading;
using namespace NMetrics;
using namespace NWriteBackCache;

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TFlushConfig
{
    TDuration AutomaticFlushPeriod;
    TDuration FlushRetryPeriod;
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

template <class F, class TResponse>
concept TRequestExecutor =
    std::invocable<F> &&
    std::same_as<std::invoke_result_t<F>, TFuture<TResponse>>;

template <class F, class TResponse>
concept TResponseProcessor =
    std::invocable<F, const TResponse&>;

////////////////////////////////////////////////////////////////////////////////

class TWriteBackCache::TImpl final
    : public std::enable_shared_from_this<TImpl>
    , private IQueuedOperationsProcessor
{
private:
    const IFileStorePtr Session;
    const ISchedulerPtr Scheduler;
    const ITimerPtr Timer;
    const IWriteBackCacheInternalStatsPtr InternalStats;
    const IWriteBackCacheStatsPtr Stats;
    const IWriteDataRequestBuilderPtr RequestBuilder;
    const ISequenceIdGeneratorPtr SequenceIdGenerator;
    const TFlushConfig FlushConfig;
    const TLog Log;
    const TString LogTag;
    const TString FileSystemId;

    IPersistentStoragePtr PersistentStorage;
    TWriteBackCacheState State;

    std::atomic<bool> DrainRequested = false;
    std::atomic<bool> DrainCompleted = false;

public:
    explicit TImpl(TWriteBackCacheArgs args)
        : Session(std::move(args.Session))
        , Scheduler(std::move(args.Scheduler))
        , Timer(std::move(args.Timer))
        , InternalStats(args.Stats->GetWriteBackCacheInternalStats())
        , Stats(args.Stats)
        , RequestBuilder(CreateWriteDataRequestBuilder(
              {.FileSystemId = args.FileSystemId,
               .MaxWriteRequestSize = args.FlushMaxWriteRequestSize,
               .MaxWriteRequestsCount = args.FlushMaxWriteRequestsCount,
               .MaxSumWriteRequestsSize = args.FlushMaxSumWriteRequestsSize,
               .ZeroCopyWriteEnabled = args.ZeroCopyWriteEnabled}))
        , SequenceIdGenerator(std::make_shared<TSequenceIdGenerator>())
        , FlushConfig(
              {.AutomaticFlushPeriod = args.AutomaticFlushPeriod,
               .FlushRetryPeriod = args.FlushRetryPeriod})
        , Log(std::move(args.Log))
        , LogTag(Sprintf(
              "[f:%s][c:%s]",
              args.FileSystemId.c_str(),
              args.ClientId.c_str()))
        , FileSystemId(args.FileSystemId)
        , State(
              *this,
              Timer,
              args.Stats->GetWriteBackCacheStateStats(),
              args.Stats->GetWriteDataRequestManagerStats(),
              args.Stats->GetNodeStateHolderStats(),
              LogTag)
    {
        auto createPersistentStorageResult =
            CreateFileRingBufferPersistentStorage(
                args.Stats->GetPersistentStorageStats(),
                {.FilePath = args.FilePath, .DataCapacity = args.CapacityBytes},
                Log,
                LogTag);

        if (HasError(createPersistentStorageResult)) {
            ReportWriteBackCacheCorruptionError(
                TStringBuilder()
                << LogTag
                << " WriteBackCache persistent storage initialization failed: "
                << createPersistentStorageResult.GetError()
                << ", FilePath: " << args.FilePath.Quote());
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

        if (!State.Init(PersistentStorage)) {
            ReportWriteBackCacheCorruptionError(
                TStringBuilder()
                << LogTag
                << " WriteBackCache failed to deserialize requests from the "
                   "persistent storage due to corruption, FilePath: "
                << args.FilePath.Quote());
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
        State.TriggerPeriodicFlushAll();
        ScheduleAutomaticFlushIfNeeded();
    }

    // Only transition Normal -> Draining -> Drained is possible
    EWriteBackCacheMode GetMode()
    {
        // GetMode may lie in a hot path - we want to avoid unnecessary mutex
        // acquisition and memory synchronization
        //
        // DrainRequested and DrainCompleted don't participate
        // in data synchronization so relaxed memory ordering can be used
        //
        if (!DrainRequested.load(std::memory_order_relaxed)) {
            return EWriteBackCacheMode::Normal;
        }

        return IsDrained() ? EWriteBackCacheMode::Drained
                           : EWriteBackCacheMode::Draining;
    }

    NThreading::TFuture<NCloud::NProto::TError> Drain()
    {
        if (!DrainRequested.exchange(true)) {
            STORAGE_INFO(LogTag << " Start WriteBackCache draining");
        }

        State.SetDrainingMode();
        return State.AddFlushAllRequest();
    }

    bool IsDrained()
    {
        if (DrainCompleted.load(std::memory_order_relaxed)) {
            return true;
        }

        // This call acquires mutex
        if (!State.IsDrained()) {
            return false;
        }

        if (!DrainCompleted.exchange(true)) {
            STORAGE_INFO(LogTag << " Complete WriteBackCache draining");
        }

        return true;
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

        // Prevent cached data parts from being evicted from storage until
        // the response is completed
        const auto pinId = State.PinCachedData(request->GetNodeId());

        TReadResponseBuilder responseBuilder(*request);
        if (auto response = responseBuilder.TryFullyServeFromCache(State)) {
            State.UnpinCachedData(request->GetNodeId(), pinId);
            InternalStats->AddReadDataStats(
                EReadDataRequestCacheStatus::FullHit);
            return MakeFuture(std::move(*response));
        }

        auto callback = [ptr = weak_from_this(),
                         responseBuilder = std::move(responseBuilder),
                         pinId](TFuture<NProto::TReadDataResponse> future)
        {
            auto response = future.ExtractValue();

            if (auto self = ptr.lock()) {
                if (!HasError(response)) {
                    bool cachedDataApplied =
                        responseBuilder.AugmentResponseWithCachedData(
                            response,
                            self->State);

                    if (cachedDataApplied) {
                        self->InternalStats->AddReadDataStats(
                            EReadDataRequestCacheStatus::PartialHit);
                    } else {
                        self->InternalStats->AddReadDataStats(
                            EReadDataRequestCacheStatus::Miss);
                    }
                }
                self->State.UnpinCachedData(responseBuilder.GetNodeId(), pinId);
            }
            return response;
        };

        return Session->ReadData(std::move(callContext), std::move(request))
            .Apply(std::move(callback));
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

        return State.AddWriteDataRequest(std::move(request));
    }

    TFuture<NProto::TError> FlushNodeData(ui64 nodeId)
    {
        return State.AddFlushRequest(nodeId);
    }

    TFuture<NProto::TError> FlushAllData()
    {
        return State.AddFlushAllRequest();
    }

    TFuture<NProto::TError> ReleaseHandle(ui64 nodeId, ui64 handle)
    {
        return State.AddReleaseHandleRequest(nodeId, handle);
    }

    TFuture<NProto::TReadDataResponse> ReadDataDirect(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TReadDataRequest> request)
    {
        return ExecuteRequestUnderBarrier<NProto::TReadDataResponse>(
            request->GetNodeId(),
            [this,
             callContext = std::move(callContext),
             request = std::move(request)]() mutable
            {
                return Session->ReadData(
                    std::move(callContext),
                    std::move(request));
            });
    }

    TFuture<NProto::TWriteDataResponse> WriteDataDirect(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TWriteDataRequest> request)
    {
        return ExecuteRequestUnderBarrier<NProto::TWriteDataResponse>(
            request->GetNodeId(),
            [this,
             callContext = std::move(callContext),
             request = std::move(request)]() mutable
            {
                return Session->WriteData(
                    std::move(callContext),
                    std::move(request));
            });
    }

    TFuture<NProto::TSetNodeAttrResponse> SetNodeAttr(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TSetNodeAttrRequest> request)
    {
        const bool attrSizeNotAffected =
            (request->GetFlags() &
             ProtoFlag(NProto::TSetNodeAttrRequest::F_SET_ATTR_SIZE)) == 0;

        if (attrSizeNotAffected) {
            return Session->SetNodeAttr(
                std::move(callContext),
                std::move(request));
        }

        // MaxWrittenOffset is used to adjust node size in GetAttr/ReadDir
        // responses according to the cached data. After WriteData requests are
        // flushed and evicted, MaxWrittenOffset doesn't automatically shrink
        // back in order to prevent data race.
        //
        // SetNodeAttr request may interfere with the requests being flushed.
        // In order to prevent data race, we need to acquire a barrier and
        // execute SetNodeAttr under a barrier.

        const ui64 nodeId = request->GetNodeId();

        auto executor = [this,
                         callContext = std::move(callContext),
                         request = std::move(request)]() mutable
        {
            return Session->SetNodeAttr(
                std::move(callContext),
                std::move(request));
        };

        auto callback = [this, nodeId](const auto& response)
        {
            if (!HasError(response)) {
                State.ResetMaxWrittenOffset(nodeId);
            }
        };

        return ExecuteRequestUnderBarrier<NProto::TSetNodeAttrResponse>(
            nodeId,
            std::move(executor),
            std::move(callback));
    }

    ui64 AcquireNodeStateRef()
    {
        return State.PinNodeStates();
    }

    void ReleaseNodeStateRef(ui64 refId)
    {
        State.UnpinNodeStates(refId);
    }

    ui64 GetMaxWrittenOffset(ui64 nodeId) const
    {
        return State.GetMaxWrittenOffset(nodeId);
    }

    IModuleStatsPtr CreateModuleStats() const
    {
        return CreateWriteBackCacheModuleStats(
            Stats,
            [ptr = weak_from_this()](TInstant now)
            {
                Y_UNUSED(now);

                if (auto self = ptr.lock()) {
                    self->State.UpdateStats();
                }
            });
    }

private:
    // Implementation of IQueuedOperationsProcessor
    void ScheduleFlushNode(ui64 nodeId) override
    {
        auto batchBuilder =
            RequestBuilder->CreateWriteDataRequestBatchBuilder(nodeId);

        State.VisitUnflushedRequests(
            nodeId,
            [&batchBuilder](const TCachedWriteDataRequest* request)
            {
                return batchBuilder->AddRequest(
                    request->GetHandle(),
                    request->GetOffset(),
                    request->GetBuffer());
            });

        auto writeDataBatch = batchBuilder->Build();

        auto flushState = std::make_shared<TNodeFlushState>(
            nodeId,
            std::move(writeDataBatch.Requests),
            writeDataBatch.AffectedRequestCount);

        ExecuteFlush(flushState);
    }

    void ExecuteFlush(std::shared_ptr<TNodeFlushState> flushState)
    {
        auto requests = flushState->BeginFlush();

        for (size_t i = 0; i < requests.size(); ++i) {
            auto& request = requests[i];
            auto callContext = MakeIntrusive<TCallContext>(FileSystemId);

            callContext->RequestType = EFileStoreRequest::WriteData;
            callContext->RequestSize =
                NCloud::NFileStore::CalculateByteCount(*request) -
                request->GetBufferOffset();

            auto callback = [ptr = weak_from_this(), flushState, i](
                                const auto& future) mutable
            {
                auto action = flushState->OnWriteDataRequestCompleted(
                    i,
                    future.GetValue());

                switch (action) {
                    case EWriteDataRequestCompletedAction::CollectFlushResult:
                        if (auto self = ptr.lock()) {
                            self->CompleteFlush(std::move(flushState));
                        }
                        break;

                    case EWriteDataRequestCompletedAction::ContinueExecution:
                        break;
                }
            };

            Session->WriteData(std::move(callContext), std::move(request))
                .Subscribe(std::move(callback));
        }
    }

    void CompleteFlush(std::shared_ptr<TNodeFlushState> flushState)
    {
        auto error = flushState->CollectFlushResult();

        if (HasError(error)) {
            auto retryStatus =
                State.FlushFailed(flushState->GetNodeId(), error);

            if (retryStatus == EFlushRetryStatus::ShouldRetry) {
                ScheduleRetryFlush(std::move(flushState));
            }
        } else {
            State.FlushSucceeded(
                flushState->GetNodeId(),
                flushState->GetAffectedUnflushedRequestCount());
        }
    }

    void ScheduleRetryFlush(std::shared_ptr<TNodeFlushState> flushState)
    {
        // TODO(nasonov): better retry policy
        Scheduler->Schedule(
            Timer->Now() + FlushConfig.FlushRetryPeriod,
            [ptr = weak_from_this(),
             flushState = std::move(flushState)]() mutable
            {
                auto self = ptr.lock();
                if (self) {
                    self->ExecuteFlush(flushState);
                }
            });
    }

    // TRequestExecutorCallable: () -> TFuture<TResponse>, may capture [this]
    // TResponseProcessorCallable: (const TResponse&), may capture [this]
    template <class TResponse>
    TFuture<TResponse> ExecuteRequestUnderBarrier(
        ui64 nodeId,
        TRequestExecutor<TResponse> auto requestExecutor,
        TResponseProcessor<TResponse> auto responseProcessor)
    {
        auto promise = NewPromise<TResponse>();
        auto resultFuture = promise.GetFuture();

        auto acquireBarrierCallback =
            [ptr = weak_from_this(),
             requestExecutor = std::move(requestExecutor),
             responseProcessor = std::move(responseProcessor),
             promise = std::move(promise),
             nodeId](const auto& future) mutable
        {
            const TResultOrError<ui64>& acquireBarrierResult =
                future.GetValue();

            if (HasError(acquireBarrierResult.GetError())) {
                // Barrier acquisition failed - propagate the error
                TResponse response;
                *response.MutableError() = acquireBarrierResult.GetError();
                promise.SetValue(std::move(response));
                return;
            }

            auto self = ptr.lock();
            if (!self) {
                TResponse response;
                *response.MutableError() =
                    MakeError(E_REJECTED, "WriteBackCache is destroyed");
                promise.SetValue(std::move(response));
                return;
            }

            auto requestCallback =
                [ptr = std::move(ptr),
                 responseProcessor = std::move(responseProcessor),
                 barrierId = acquireBarrierResult.GetResult(),
                 promise = std::move(promise),
                 nodeId](const TFuture<TResponse>& future) mutable
            {
                const auto& response = future.GetValue();
                if (auto self = ptr.lock()) {
                    responseProcessor(response);
                    self->State.ReleaseBarrier(nodeId, barrierId);
                }
                promise.SetValue(response);
            };

            requestExecutor().Subscribe(std::move(requestCallback));
        };

        State.AcquireBarrier(nodeId).Subscribe(
            std::move(acquireBarrierCallback));

        return resultFuture;
    }

    template <class TResponse>
    TFuture<TResponse> ExecuteRequestUnderBarrier(
        ui64 nodeId,
        TRequestExecutor<TResponse> auto requestExecutor)
    {
        return ExecuteRequestUnderBarrier<TResponse>(
            nodeId,
            std::move(requestExecutor),
            [](const auto&) {});
    }
};

////////////////////////////////////////////////////////////////////////////////

TWriteBackCache::TWriteBackCache() = default;

TWriteBackCache::TWriteBackCache(TWriteBackCacheArgs args)
    : Impl(std::make_shared<TImpl>(std::move(args)))
{
    Impl->ScheduleAutomaticFlushIfNeeded();
}

TWriteBackCache::~TWriteBackCache() = default;

EWriteBackCacheMode TWriteBackCache::GetMode() const
{
    return Impl->GetMode();
}

NThreading::TFuture<NCloud::NProto::TError> TWriteBackCache::Drain()
{
    return Impl->Drain();
}

bool TWriteBackCache::IsDrained() const
{
    return Impl->IsDrained();
}

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

TFuture<NProto::TError> TWriteBackCache::FlushNodeData(ui64 nodeId)
{
    return Impl->FlushNodeData(nodeId);
}

TFuture<NProto::TError> TWriteBackCache::FlushAllData()
{
    return Impl->FlushAllData();
}

TFuture<NProto::TError> TWriteBackCache::ReleaseHandle(ui64 nodeId, ui64 handle)
{
    return Impl->ReleaseHandle(nodeId, handle);
}

TFuture<NProto::TReadDataResponse> TWriteBackCache::ReadDataDirect(
    TCallContextPtr callContext,
    std::shared_ptr<NProto::TReadDataRequest> request)
{
    return Impl->ReadDataDirect(std::move(callContext), std::move(request));
}

TFuture<NProto::TWriteDataResponse> TWriteBackCache::WriteDataDirect(
    TCallContextPtr callContext,
    std::shared_ptr<NProto::TWriteDataRequest> request)
{
    return Impl->WriteDataDirect(std::move(callContext), std::move(request));
}

TFuture<NProto::TSetNodeAttrResponse> TWriteBackCache::SetNodeAttr(
    TCallContextPtr callContext,
    std::shared_ptr<NProto::TSetNodeAttrRequest> request)
{
    return Impl->SetNodeAttr(std::move(callContext), std::move(request));
}

ui64 TWriteBackCache::AcquireNodeStateRef()
{
    return Impl->AcquireNodeStateRef();
}

void TWriteBackCache::ReleaseNodeStateRef(ui64 refId)
{
    Impl->ReleaseNodeStateRef(refId);
}

ui64 TWriteBackCache::GetMaxWrittenOffset(ui64 nodeId) const
{
    return Impl->GetMaxWrittenOffset(nodeId);
}

IModuleStatsPtr TWriteBackCache::CreateModuleStats() const
{
    return Impl->CreateModuleStats();
}

}   // namespace NCloud::NFileStore::NFuse
