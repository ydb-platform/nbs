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

#include <util/system/mutex.h>

namespace NCloud::NFileStore::NFuse {

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

    // All fields below should be protected by this lock
    TMutex Lock;
    IPersistentStoragePtr PersistentStorage;
    TWriteBackCacheState State;

public:
    explicit TImpl(TWriteBackCacheArgs args)
        : Session(std::move(args.Session))
        , Scheduler(std::move(args.Scheduler))
        , Timer(std::move(args.Timer))
        , Stats(std::move(args.Stats))
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
        , State(*this, Timer, Stats)
    {
        auto createPersistentStorageResult =
            CreateFileRingBufferPersistentStorage(
                Stats,
                {.FilePath = args.FilePath,
                 .DataCapacity = args.CapacityBytes});

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

        Stats->ResetNonDerivativeCounters();

        if (!State.Init(PersistentStorage)) {
            ReportWriteBackCacheCorruptionError(
                TStringBuilder()
                << LogTag
                << " WriteBackCache failed to deserialize requests from the "
                   "persistent storage, FilePath: "
                << args.FilePath.Quote());
            return;
        }

        const auto persistentStorageStats = PersistentStorage->GetStats();

        STORAGE_INFO(
            LogTag << " WriteBackCache has been initialized "
            << "{\"FilePath\": " << args.FilePath.Quote()
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

        auto nodeId = request->GetNodeId();
        auto offset = request->GetOffset();
        auto end = request->GetOffset() + request->GetLength();

        auto unlocker =
            [ptr = weak_from_this(), nodeId, offset, end](const auto&)
        {
            if (auto self = ptr.lock()) {
                self->State.UnlockRead(nodeId, offset, end);
            }
        };

        TPendingReadDataRequest pendingRequest = {
            .CallContext = std::move(callContext),
            .Request = std::move(request),
            .Promise = NewPromise<NProto::TReadDataResponse>()};

        auto result = pendingRequest.Promise.GetFuture();
        result.Subscribe(std::move(unlocker));

        auto locker =
            [ptr = weak_from_this(),
             pendingRequest = std::move(pendingRequest)](const auto& f) mutable
        {
            f.GetValue();
            auto self = ptr.lock();
            // Lock action is invoked immediately or from
            // UnlockRead/UnlockWrite calls that can be made only when
            // TImpl is alive
            Y_DEBUG_ABORT_UNLESS(self);
            if (self) {
                self->StartPendingReadDataRequest(std::move(pendingRequest));
            }
        };

        State.LockRead(nodeId, offset, end).Apply(std::move(locker));

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
                self->State.UnlockWrite(nodeId, offset, end);
            }
        };

        auto future =
            State.LockWrite(nodeId, offset, end)
                .Apply(
                    [this, request = std::move(request)](const auto& f) mutable
                    {
                        f.GetValue();
                        return State.AddWriteDataRequest(std::move(request));
                    });

        future.Subscribe(std::move(unlocker));

        return future;
    }

    TFuture<void> FlushNodeData(ui64 nodeId)
    {
        return State.AddFlushRequest(nodeId);
    }

    TFuture<void> FlushAllData()
    {
        return State.AddFlushAllRequest();
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
    TVector<TCachedDataPart> CalculateDataPartsToReadAndFillBuffer(
        ui64 nodeId,
        ui64 offset,
        ui64 length,
        TString* buffer)
    {
        with_lock (Lock) {
            auto data = State.GetCachedData(nodeId, offset, length);
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

                    default:
                        Y_ABORT(
                            "Unexpected action - %d",
                            static_cast<int>(action));
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
            ScheduleRetryFlush(std::move(flushState));
            return;
        }

        Stats->FlushCompleted();

        with_lock (Lock) {
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
};

////////////////////////////////////////////////////////////////////////////////

TWriteBackCache::TWriteBackCache() = default;

TWriteBackCache::TWriteBackCache(TWriteBackCacheArgs args)
    : Impl(std::make_shared<TImpl>(std::move(args)))
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
