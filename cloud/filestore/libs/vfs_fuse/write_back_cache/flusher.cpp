#include "flusher.h"

#include <cloud/filestore/libs/service/context.h>
#include <cloud/filestore/libs/service/filestore.h>
#include <cloud/filestore/libs/storage/core/helpers.h>

#include <util/generic/hash_set.h>
#include <util/system/spinlock.h>

namespace NCloud::NFileStore::NFuse::NWriteBackCache {

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TWriteDataRequestState
{
    std::shared_ptr<NProto::TWriteDataRequest> Request;
    NProto::TError Error;

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
    IWriteDataRequestBuilder& RequestBuilder;
    IFileStorePtr Session;
    TString FileSystemId;

    TAdaptiveLock Lock;
    THashMap<ui64, std::unique_ptr<TFlushState>> Nodes;

public:
    TImpl(
        TWriteBackCacheState& state,
        IWriteDataRequestBuilder& requestBuilder,
        IFileStorePtr session,
        const TString& fileSystemId)
        : State(state)
        , RequestBuilder(requestBuilder)
        , Session(std::move(session))
        , FileSystemId(fileSystemId)
    {
        Y_UNUSED(State);
        Y_UNUSED(RequestBuilder);
    }

    void ShouldFlushNode(ui64 nodeId)
    {
        auto& flushState = CreateFlushState(nodeId);

        auto writeDataBatch = RequestBuilder.BuildWriteDataRequests(
            FileSystemId,
            nodeId,
            [this, nodeId](const auto& visitor)
            {
                State.VisitUnflushedCachedRequests(
                    nodeId,
                    [&visitor](const TCachedWriteDataRequest* request)
                    {
                        return visitor(
                            request->GetHandle(),
                            request->GetOffset(),
                            request->GetCachedData());
                    });
            });

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

        Y_ABORT_UNLESS(Nodes.erase(flushState.NodeId));
    }

    void ExecuteFlush(TFlushState& flushState)
    {
        flushState.InFlightWriteDataRequestsCount =
            flushState.WriteDataRequests.size();

        // flushState may become unusable after last Session->WriteData call
        const auto size = flushState.WriteDataRequests.size();

        for (size_t i = 0; i < size; ++i) {
            auto callContext = MakeIntrusive<TCallContext>(FileSystemId);
            auto request = flushState.WriteDataRequests[i].Request;

            callContext->RequestType = EFileStoreRequest::WriteData;
            callContext->RequestSize = NStorage::CalculateByteCount(*request) -
                                       request->GetBufferOffset();

            Session->WriteData(std::move(callContext), std::move(request))
                .Subscribe(
                    [ptr = weak_from_this(), nodeId = flushState.NodeId, i](
                        const auto& future)
                    {
                        if (auto self = ptr.lock()) {
                            self->WriteDataRequestCompleted(
                                nodeId,
                                i,
                                future.GetValue());
                        }
                    });
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
        const ui64 nodeId = flushState.NodeId;
        const size_t requestCount = flushState.AffectedCachedRequestsCount;

        DeleteFlushState(flushState);

        State.FlushSucceeded(nodeId, requestCount);
    }

    void FlushFailed(TFlushState& flushState)
    {
        const ui64 nodeId = flushState.NodeId;
        const auto error = flushState.WriteDataRequests.front().Error;

        // ToDo: error handling
        DeleteFlushState(flushState);

        State.FlushFailed(nodeId, error);
    }
};

////////////////////////////////////////////////////////////////////////////////

TFlusher::TFlusher(
    TWriteBackCacheState& state,
    IWriteDataRequestBuilder& requestBuilder,
    IFileStorePtr session,
    const TString& fileSystemId)
    : Impl(
          std::make_shared<TImpl>(
              state,
              requestBuilder,
              std::move(session),
              fileSystemId))
{}

void TFlusher::ShouldFlushNode(ui64 nodeId)
{
    Impl->ShouldFlushNode(nodeId);
}

}   // namespace NCloud::NFileStore::NFuse::NWriteBackCache
