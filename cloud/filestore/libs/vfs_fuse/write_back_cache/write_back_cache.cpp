#include "write_back_cache.h"

#include "node_flush_state.h"
#include "persistent_storage.h"
#include "sequence_id_generator.h"
#include "utils.h"
#include "write_back_cache_state.h"
#include "write_back_cache_stats.h"
#include "write_data_request_builder.h"

#include <cloud/filestore/libs/diagnostics/critical_events.h>
#include <cloud/filestore/libs/service/context.h>
#include <cloud/filestore/libs/storage/core/helpers.h>

namespace NCloud::NFileStore::NFuse {

using namespace NThreading;
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

class TWriteBackCache::TImpl final
    : public std::enable_shared_from_this<TImpl>
    , private IQueuedOperationsProcessor
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

    IPersistentStoragePtr PersistentStorage;
    TWriteBackCacheState State;

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
        , State(*this, Timer, Stats)
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

        if (!State.Init(PersistentStorage)) {
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
        State.TriggerPeriodicFlushAll();
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

        const auto nodeId = request->GetNodeId();
        const auto offset = request->GetOffset();
        const auto length = request->GetLength();

        // Prevent cached data parts from being evicted from storage until
        // the response is completed
        const auto pinId = State.PinCachedData(nodeId);
        const auto cachedData = State.GetCachedData(nodeId, offset, length);

        if (TUtils::IsFullyCoveredByParts(cachedData.Parts, length)) {
            auto response = TUtils::BuildReadDataResponse(cachedData.Parts);
            State.UnpinCachedData(nodeId, pinId);
            Stats->AddReadDataStats(EReadDataRequestCacheStatus::FullHit);
            return MakeFuture(std::move(response));
        }

        auto callback = [ptr = weak_from_this(), nodeId, offset, length, pinId](
                            TFuture<NProto::TReadDataResponse> future)
        {
            auto response = future.ExtractValue();

            if (auto self = ptr.lock()) {
                if (!HasError(response)) {
                    const auto cachedData =
                        self->State.GetCachedData(nodeId, offset, length);

                    if (cachedData.Parts.empty()) {
                        self->Stats->AddReadDataStats(
                            EReadDataRequestCacheStatus::Miss);
                    } else {
                        self->Stats->AddReadDataStats(
                            EReadDataRequestCacheStatus::PartialHit);
                    }

                    TUtils::AugmentReadDataResponse(response, cachedData);
                }
                self->State.UnpinCachedData(nodeId, pinId);
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

    bool IsEmpty() const
    {
        return !State.HasUnflushedRequests();
    }

    ui64 AcquireNodeStateRef()
    {
        return State.PinNodeStates();
    }

    void ReleaseNodeStateRef(ui64 refId)
    {
        State.UnpinNodeStates(refId);
    }

    ui64 GetCachedNodeSize(ui64 nodeId) const
    {
        return State.GetCachedNodeSize(nodeId);
    }

    void SetCachedNodeSize(ui64 nodeId, ui64 size)
    {
        State.SetCachedNodeSize(nodeId, size);
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

        Stats->FlushStarted();

        ExecuteFlush(flushState);
    }

    void ExecuteFlush(std::shared_ptr<TNodeFlushState> flushState)
    {
        auto requests = flushState->BeginFlush();

        for (size_t i = 0; i < requests.size(); ++i) {
            auto& request = requests[i];
            auto callContext = MakeIntrusive<TCallContext>(FileSystemId);

            callContext->RequestType = EFileStoreRequest::WriteData;
            callContext->RequestSize = NStorage::CalculateByteCount(*request) -
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
            Stats->FlushFailed();

            auto retryStatus =
                State.FlushFailed(flushState->GetNodeId(), error);

            if (retryStatus == EFlushRetryStatus::ShouldRetry) {
                ScheduleRetryFlush(std::move(flushState));
                return;
            }
        } else {
            State.FlushSucceeded(
                flushState->GetNodeId(),
                flushState->GetAffectedUnflushedRequestCount());
        }

        Stats->FlushCompleted();
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
