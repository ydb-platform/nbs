#include "flusher.h"

#include <cloud/storage/core/libs/common/error.h>

#include <util/generic/algorithm.h>
#include <util/generic/hash.h>
#include <util/system/spinlock.h>

namespace NCloud::NFileStore::NFuse::NWriteBackCache {

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TWriteDataRequestState
{
    std::shared_ptr<NProto::TWriteDataRequest> Request;
    NCloud::NProto::TError Error;

    explicit TWriteDataRequestState(
        std::shared_ptr<NProto::TWriteDataRequest> request)
        : Request(std::move(request))
    {}
};

////////////////////////////////////////////////////////////////////////////////

struct TFlushState
{
    const ui64 NodeId = 0;
    TVector<TWriteDataRequestState> WriteDataRequests;
    size_t AffectedCachedRequestsCount = 0;
    std::atomic<size_t> InFlightWriteDataRequestsCount = 0;

    explicit TFlushState(ui64 nodeId)
        : NodeId(nodeId)
    {}
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

class TFlusher::TImpl: public std::enable_shared_from_this<TImpl>
{
private:
    TWriteBackCacheState& State;
    const IWriteDataRequestBuilderPtr RequestBuilder;
    IWriteDataRequestExecutor& Executor;
    const IWriteBackCacheStatsPtr Stats;

    TAdaptiveLock Lock;
    THashMap<ui64, std::unique_ptr<TFlushState>> Nodes;

public:
    TImpl(
        TWriteBackCacheState& state,
        IWriteDataRequestBuilderPtr requestBuilder,
        IWriteDataRequestExecutor& executor,
        IWriteBackCacheStatsPtr stats)
        : State(state)
        , RequestBuilder(std::move(requestBuilder))
        , Executor(executor)
        , Stats(std::move(stats))
    {}

    void ScheduleFlushNode(ui64 nodeId)
    {
        auto& flushState = CreateFlushState(nodeId);

        auto batchBuilder =
            RequestBuilder->CreateWriteDataRequestBatchBuilder(nodeId);

        State.VisitUnflushedCachedRequests(
            nodeId,
            [&batchBuilder](const TCachedWriteDataRequest* request)
            {
                return batchBuilder->AddRequest(
                    request->GetHandle(),
                    request->GetOffset(),
                    request->GetBuffer());
            });

        auto writeDataBatch = batchBuilder->Build();

        Y_ABORT_UNLESS(writeDataBatch.AffectedRequestCount > 0);

        flushState.AffectedCachedRequestsCount =
            writeDataBatch.AffectedRequestCount;

        for (auto& request: writeDataBatch.Requests) {
            flushState.WriteDataRequests.emplace_back(std::move(request));
        }

        ExecuteFlush(flushState);
    }

private:
    TFlushState& CreateFlushState(ui64 nodeId)
    {
        auto guard = Guard(Lock);

        auto& ptr = Nodes[nodeId];
        Y_ABORT_UNLESS(!ptr, "Flush has already started for node %lu", nodeId);

        ptr = std::make_unique<TFlushState>(nodeId);
        return *ptr;
    }

    TFlushState& GetFlushState(ui64 nodeId)
    {
        auto guard = Guard(Lock);

        auto& ptr = Nodes[nodeId];
        Y_ABORT_UNLESS(ptr, "Flush state not found for node %lu", nodeId);

        return *ptr;
    }

    void DeleteFlushState(TFlushState& flushState)
    {
        auto guard = Guard(Lock);

        Y_ABORT_UNLESS(
            Nodes.erase(flushState.NodeId),
            "Flush state does not exist for node %lu",
            flushState.NodeId);
    }

    void ExecuteFlush(TFlushState& flushState)
    {
        Stats->FlushStarted();

        flushState.InFlightWriteDataRequestsCount =
            flushState.WriteDataRequests.size();

        // flushState may become unusable after last Session->WriteData call
        const auto size = flushState.WriteDataRequests.size();

        for (size_t i = 0; i < size; ++i) {
            Executor.ExecuteWriteDataRequest(
                flushState.WriteDataRequests[i].Request,
                [this, nodeId = flushState.NodeId, i](const auto& response)
                { WriteDataRequestCompleted(nodeId, i, response); });
        }
    }

    void WriteDataRequestCompleted(
        ui64 nodeId,
        size_t i,
        const NProto::TWriteDataResponse& response)
    {
        auto& flushState = GetFlushState(nodeId);

        auto prev = flushState.InFlightWriteDataRequestsCount--;
        Y_ABORT_UNLESS(prev > 0);

        if (HasError(response)) {
            flushState.WriteDataRequests[i].Error = response.GetError();
        }

        if (prev == 1) {
            FlushCompleted(flushState);
        }
    }

    void FlushCompleted(TFlushState& flushState)
    {
        Y_ABORT_UNLESS(flushState.InFlightWriteDataRequestsCount == 0);

        EraseIf(
            flushState.WriteDataRequests,
            [](const auto& it) { return !HasError(it.Error); });

        if (flushState.WriteDataRequests.empty()) {
            FlushSucceeded(flushState);
        } else {
            FlushFailed(flushState);
        }
    }

    void FlushSucceeded(TFlushState& flushState)
    {
        Stats->FlushCompleted();

        const ui64 nodeId = flushState.NodeId;
        const size_t requestCount = flushState.AffectedCachedRequestsCount;

        DeleteFlushState(flushState);

        State.FlushSucceeded(nodeId, requestCount);
    }

    void FlushFailed(TFlushState& flushState)
    {
        Stats->FlushFailed();

        const ui64 nodeId = flushState.NodeId;
        const auto error = flushState.WriteDataRequests.front().Error;

        // ToDo(#1751): error handling
        // https://github.com/ydb-platform/nbs/pull/4793
        DeleteFlushState(flushState);

        State.FlushFailed(nodeId, error);
    }
};

////////////////////////////////////////////////////////////////////////////////

TFlusher::TFlusher() = default;
TFlusher::TFlusher(TFlusher&&) noexcept = default;
TFlusher& TFlusher::operator=(TFlusher&&) noexcept = default;
TFlusher::~TFlusher() = default;

TFlusher::TFlusher(
    TWriteBackCacheState& state,
    IWriteDataRequestBuilderPtr requestBuilder,
    IWriteDataRequestExecutor& executor,
    IWriteBackCacheStatsPtr stats)
    : Impl(
          std::make_unique<TImpl>(
              state,
              std::move(requestBuilder),
              executor,
              std::move(stats)))
{}

void TFlusher::ScheduleFlushNode(ui64 nodeId)
{
    Impl->ScheduleFlushNode(nodeId);
}

}   // namespace NCloud::NFileStore::NFuse::NWriteBackCache
