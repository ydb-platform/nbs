#include "write_back_cache.h"

#include "flusher.h"
#include "persistent_storage_impl.h"
#include "sequence_id_generator_impl.h"
#include "utils.h"
#include "write_back_cache_state.h"
#include "write_back_cache_stats.h"
#include "write_data_request_builder_impl.h"

#include <cloud/filestore/libs/diagnostics/critical_events.h>
#include <cloud/filestore/libs/service/context.h>

#include <cloud/storage/core/libs/common/file_ring_buffer.h>

#include <library/cpp/digest/crc32c/crc32c.h>
#include <library/cpp/threading/future/subscription/wait_all.h>

#include <util/generic/hash_set.h>
#include <util/generic/intrlist.h>
#include <util/generic/mem_copy.h>
#include <util/generic/set.h>
#include <util/generic/strbuf.h>
#include <util/generic/vector.h>
#include <util/stream/mem.h>
#include <util/system/mutex.h>

namespace NCloud::NFileStore::NFuse {

using namespace NThreading;
using namespace NWriteBackCache;

namespace {

////////////////////////////////////////////////////////////////////////////////

bool IsIntervalFullyCoveredByParts(
    const TVector<TCachedDataPart>& parts,
    ui64 offset,
    ui64 length)
{
    for (const auto& part: parts) {
        Y_ABORT_UNLESS(offset <= part.Offset);
        Y_ABORT_UNLESS(part.Offset + part.Data.size() <= offset + length);

        if (part.Offset != offset) {
            return false;
        }
        offset += part.Data.size();
        length -= part.Data.size();
    }

    return length == 0;
}

NProto::TReadDataResponse BuildReadDataResponse(
    NProto::TReadDataResponse response,
    ui64 offset,
    ui64 requestedLength,
    const TVector<TCachedDataPart>& parts,
    ui64 end)
{
    // ToDo: optimize this later
    const auto responseData =
        TStringBuf(response.GetBuffer()).Skip(response.GetBufferOffset());

    Y_ABORT_UNLESS(responseData.size() <= requestedLength);

    const auto availableLength = end > offset ? end - offset : 0;

    const auto expectedLength =
        Max(responseData.size(), Min(requestedLength, availableLength));

    if (responseData.size() == expectedLength && parts.empty()) {
        return response;
    }

    auto result = TString(expectedLength, 0);
    responseData.copy(result.begin(), responseData.size());

    for (const auto& part: parts) {
        part.Data.copy(
            result.begin() + (part.Offset - offset),
            part.Data.size());
    }

    response.SetBuffer(std::move(result));
    response.SetBufferOffset(0);

    return response;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

class TWriteBackCache::TImpl final: public std::enable_shared_from_this<TImpl>
{
private:
    const IFileStorePtr Session;
    const ISchedulerPtr Scheduler;
    const ITimerPtr Timer;
    const IWriteBackCacheStatsPtr Stats;
    TWriteDataRequestBuilder RequestBuilder;
    const TDuration AutomaticFlushPeriod;
    const TDuration FlushRetryPeriod;
    const TLog Log;
    const TString LogTag;
    const TString FileSystemId;

    TFileRingBufferStorage Storage;
    TSequenceIdGenerator SequenceGenerator;
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
        , RequestBuilder(
              {.MaxWriteRequestSize = args.FlushMaxWriteRequestSize,
               .MaxWriteRequestsCount = args.FlushMaxWriteRequestsCount,
               .MaxSumWriteRequestsSize = args.FlushMaxSumWriteRequestsSize,
               .ZeroCopyWriteEnabled = args.ZeroCopyWriteEnabled})
        , AutomaticFlushPeriod(args.AutomaticFlushPeriod)
        , FlushRetryPeriod(args.FlushRetryPeriod)
        , Log(std::move(args.Log))
        , LogTag(Sprintf(
              "[f:%s][c:%s]",
              args.FileSystemId.c_str(),
              args.ClientId.c_str()))
        , FileSystemId(args.FileSystemId)
        , State(Storage, Flusher)
        , Flusher(State, RequestBuilder, Session, args.FileSystemId)
    {
        auto error = Storage.Init(
            {.FilePath = args.FilePath,
             .DataCapacity = args.CapacityBytes,
             .MetadataCapacity = 0,
             .EnableChecksumCalculation = false,
             .EnableChecksumValidation = false});

        if (HasError(error)) {
            ReportWriteBackCacheCreatingOrDeletingError(
                TStringBuilder()
                << LogTag
                << " WriteBackCache storage initialization failed with error "
                << error);
            IsFailed = true;
            return;
        }

        // File ring buffer should be able to store any valid TWriteDataRequest.
        // Inability to store it will cause this and future requests to remain
        // in the pending queue forever (including requests with smaller size).
        // Should fit 1 MiB of data plus some headers (assume 1 KiB is enough).
        Y_ABORT_UNLESS(
            Storage.GetMaxSupportedAllocationByteCount() >= 1024 * 1024 + 1016);

        Stats->ResetNonDerivativeCounters();

        if (!State.Init()) {
            ReportWriteBackCacheCorruptionError(
                LogTag + " WriteBackCache persistent queue is corrupted");
            IsFailed = true;
            return;
        }

        auto storageStats = Storage.GetStats();

        STORAGE_INFO(
            LogTag << " WriteBackCache has been initialized "
                   << "{\"FilePath\": " << args.FilePath.Quote()
                   << ", \"RawCapacityBytes\": "
                   << storageStats.RawCapacityByteCount
                   << ", \"RawUsedBytesCount\": "
                   << storageStats.RawUsedByteCount
                   << ", \"WriteDataRequestCount\": " << storageStats.EntryCount
                   << "}");

        UpdatePersistentQueueStats();
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

        auto parts = GetCachedDataParts(nodeId, offset, length);

        if (IsIntervalFullyCoveredByParts(parts, offset, length)) {
            auto result = TString::Uninitialized(length);
            for (const auto& part: parts) {
                part.Data.copy(
                    result.begin() + (part.Offset - offset),
                    part.Data.size());
            }

            NProto::TReadDataResponse response;
            response.SetBuffer(std::move(result));

            return MakeFuture(std::move(response));
        }

        const auto pinId = State.PinCachedData(nodeId);

        auto callback = [ptr = weak_from_this(), nodeId, offset, length, pinId](
                            TFuture<NProto::TReadDataResponse> future)
        {
            auto response = future.ExtractValue();

            auto self = ptr.lock();
            if (!self) {
                return response;
            }

            if (HasError(response)) {
                self->State.UnpinCachedData(nodeId, pinId);
                return response;
            }

            auto end = self->GetCachedNodeSize(nodeId);
            auto parts = self->GetCachedDataParts(nodeId, offset, length);

            self->State.UnpinCachedData(nodeId, pinId);

            if (!parts.empty()) {
                end = Max(end, parts.back().Offset + parts.back().Data.size());
            }

            return BuildReadDataResponse(response, offset, length, parts, end);
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
        return State.PinAllCachedData();
    }

    void ReleaseNodeStateRef(ui64 refId)
    {
        State.UnpinAllCachedData(refId);
    }

    ui64 GetCachedNodeSize(ui64 nodeId) const
    {
        return State.GetCachedDataEndOffset(nodeId);
    }

    void SetCachedNodeSize(ui64 nodeId, ui64 size)
    {
        Y_UNUSED(nodeId);
        Y_UNUSED(size);
    }

private:
    auto GetCachedDataParts(ui64 nodeId, ui64 offset, ui64 length) const
        -> TVector<TCachedDataPart>
    {
        TVector<TCachedDataPart> parts;

        State.VisitCachedData(
            nodeId,
            offset,
            length,
            [&parts](TCachedDataPart part)
            {
                parts.push_back(part);
                return true;
            });

        return parts;
    }

    void UpdatePersistentQueueStats()
    {
        auto stats = Storage.GetStats();

        Stats->UpdatePersistentQueueStats(
            {.RawCapacity = stats.RawCapacityByteCount,
             .RawUsedBytesCount = stats.RawUsedByteCount,
             .MaxAllocationBytesCount = 0,
             .IsCorrupted = IsFailed});
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
