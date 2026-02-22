#include "write_back_cache.h"

#include "flusher.h"
#include "persistent_storage.h"
#include "sequence_id_generator.h"
#include "utils.h"
#include "write_back_cache_state.h"
#include "write_back_cache_stats.h"
#include "write_data_request_builder.h"

#include <cloud/filestore/libs/diagnostics/critical_events.h>
#include <cloud/filestore/libs/service/context.h>
#include <cloud/filestore/libs/storage/core/helpers.h>

#include <util/generic/scope.h>

namespace NCloud::NFileStore::NFuse {

using namespace NThreading;
using namespace NWriteBackCache;

////////////////////////////////////////////////////////////////////////////////

class TWriteBackCache::TImpl final
    : public std::enable_shared_from_this<TImpl>
    , private IWriteBackCacheStateListener
    , private IWriteDataRequestExecutor
{
private:
    const IFileStorePtr Session;
    const ISchedulerPtr Scheduler;
    const ITimerPtr Timer;
    const IWriteBackCacheStatsPtr Stats;
    const IWriteDataRequestBuilderPtr RequestBuilder;
    const ISequenceIdGeneratorPtr SequenceIdGenerator;
    const TDuration AutomaticFlushPeriod;
    const TDuration FlushRetryPeriod;
    const TLog Log;
    const TString LogTag;
    const TString FileSystemId;

    IPersistentStoragePtr Storage;
    TWriteBackCacheState State;
    TFlusher Flusher;
    bool IsFailed = false;

public:
    explicit TImpl(TWriteBackCacheArgs args)
        : Session(std::move(args.Session))
        , Scheduler(std::move(args.Scheduler))
        , Timer(std::move(args.Timer))
        , Stats(
              args.Stats ? std::move(args.Stats)
                         : CreateDummyWriteBackCacheStats())
        , RequestBuilder(CreateWriteDataRequestBuilder(
              {.FileSystemId = args.FileSystemId,
               .MaxWriteRequestSize = args.FlushMaxWriteRequestSize,
               .MaxWriteRequestsCount = args.FlushMaxWriteRequestsCount,
               .MaxSumWriteRequestsSize = args.FlushMaxSumWriteRequestsSize,
               .ZeroCopyWriteEnabled = args.ZeroCopyWriteEnabled}))
        , SequenceIdGenerator(std::make_shared<TSequenceIdGenerator>())
        , AutomaticFlushPeriod(args.AutomaticFlushPeriod)
        , FlushRetryPeriod(args.FlushRetryPeriod)
        , Log(std::move(args.Log))
        , LogTag(Sprintf(
              "[f:%s][c:%s]",
              args.FileSystemId.c_str(),
              args.ClientId.c_str()))
        , FileSystemId(args.FileSystemId)
    {
        auto createPersistentStorageResult =
            CreateFileRingBufferPersistentStorage(
                Stats,
                {.FilePath = args.FilePath,
                 .DataCapacity = args.CapacityBytes,
                 .MetadataCapacity = 0,
                 .EnableChecksumValidation = false});

        if (HasError(createPersistentStorageResult)) {
            ReportWriteBackCacheCorruptionError(
                TStringBuilder()
                << LogTag
                << " WriteBackCache persistent storage initialization failed: "
                << createPersistentStorageResult.GetError()
                << ", FilePath: " << args.FilePath.Quote());

            IsFailed = true;
            return;
        }

        Storage = createPersistentStorageResult.ExtractResult();

        State = TWriteBackCacheState(
            Storage,
            *this,
            Timer,
            Stats,
            {.EnableFlushFailure = false});

        Flusher = TFlusher(State, RequestBuilder, *this, Stats);

        // File ring buffer should be able to store any valid TWriteDataRequest.
        // Inability to store it will cause this and future requests to remain
        // in the pending queue forever (including requests with smaller size).
        // Should fit 1 MiB of data plus some headers (assume 1 KiB is enough).
        Y_ABORT_UNLESS(
            Storage->GetMaxSupportedAllocationByteCount() >=
            1024 * 1024 + 1016);

        Stats->ResetNonDerivativeCounters();

        if (!State.Init()) {
            ReportWriteBackCacheCorruptionError(
                TStringBuilder()
                << LogTag
                << " WriteBackCache failed to deserialize requests from the "
                   "persistent storage, FilePath: "
                << args.FilePath.Quote());

            IsFailed = true;
            return;
        }

        auto storageStats = Storage->GetStats();

        STORAGE_INFO(
            LogTag << " WriteBackCache has been initialized "
                   << "{\"FilePath\": " << args.FilePath.Quote()
                   << ", \"RawCapacityBytes\": "
                   << storageStats.RawCapacityByteCount
                   << ", \"RawUsedBytesCount\": "
                   << storageStats.RawUsedByteCount
                   << ", \"WriteDataRequestCount\": " << storageStats.EntryCount
                   << "}");
    }

    void ScheduleAutomaticFlushIfNeeded()
    {
        if (!AutomaticFlushPeriod) {
            return;
        }

        Scheduler->Schedule(
            Timer->Now() + AutomaticFlushPeriod,
            [ptr = weak_from_this()]()
            {
                if (auto self = ptr.lock()) {
                    self->State.TriggerPeriodicFlushAll();
                    self->ScheduleAutomaticFlushIfNeeded();
                }
            });
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

    TFuture<void> FlushNodeData(ui64 nodeId)
    {
        return State.AddFlushRequest(nodeId).IgnoreResult();
    }

    TFuture<void> FlushAllData()
    {
        return State.AddFlushAllRequest().IgnoreResult();
    }

    bool IsEmpty() const
    {
        return !State.HasUnflushedRequests();
    }

    ui64 AcquireNodeStateRef()
    {
        return State.PinMetadata();
    }

    void ReleaseNodeStateRef(ui64 refId)
    {
        State.UnpinMetadata(refId);
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
    // Implementation of IWriteDataRequestExecutor
    void ExecuteWriteDataRequest(
        std::shared_ptr<NProto::TWriteDataRequest> request,
        std::function<void(const NProto::TWriteDataResponse&)> callback)
        override
    {
        auto callContext = MakeIntrusive<TCallContext>(FileSystemId);

        callContext->RequestType = EFileStoreRequest::WriteData;
        callContext->RequestSize =
            NStorage::CalculateByteCount(*request) - request->GetBufferOffset();

        Session->WriteData(std::move(callContext), std::move(request))
            .Subscribe(
                [ptr = weak_from_this(), callback = std::move(callback)](
                    const TFuture<NProto::TWriteDataResponse>& future)
                {
                    if (auto self = ptr.lock()) {
                        callback(future.GetValue());
                    }
                });
    }

    // Implementation of IWriteBackCacheStateListener
    void ScheduleFlushNode(ui64 nodeId) override
    {
        Flusher.ScheduleFlushNode(nodeId);
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
