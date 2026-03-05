#include "node_flush_state.h"

#include <cloud/storage/core/libs/common/error.h>

#include <util/generic/algorithm.h>

namespace NCloud::NFileStore::NFuse::NWriteBackCache {

////////////////////////////////////////////////////////////////////////////////

TNodeFlushState::TNodeFlushState(
    ui64 nodeId,
    TVector<std::shared_ptr<NProto::TWriteDataRequest>> requests,
    ui64 affectedUnflushedRequestCount)
    : NodeId(nodeId)
    , AffectedUnflushedRequestCount(affectedUnflushedRequestCount)
{
    Y_ABORT_UNLESS(!requests.empty());
    Y_ABORT_UNLESS(affectedUnflushedRequestCount > 0);

    for (auto& request: requests) {
        WriteDataRequests.emplace_back(std::move(request));
    }
}

TVector<std::shared_ptr<NProto::TWriteDataRequest>>
TNodeFlushState::BeginFlush()
{
    Y_ABORT_UNLESS(InFlightWriteDataRequestCount.load() == 0);
    Y_ABORT_UNLESS(!WriteDataRequests.empty(), "Nothing to flush");

    TVector<std::shared_ptr<NProto::TWriteDataRequest>> res(
        Reserve(WriteDataRequests.size()));

    for (const auto& it: WriteDataRequests) {
        res.push_back(it.Request);
    }

    InFlightWriteDataRequestCount.store(WriteDataRequests.size());

    return res;
}

EWriteDataRequestCompletedAction TNodeFlushState::OnWriteDataRequestCompleted(
    size_t index,
    const NProto::TWriteDataResponse& response)
{
    Y_ABORT_UNLESS(index < WriteDataRequests.size());

    WriteDataRequests[index].Error = response.GetError();

    auto prev =
        InFlightWriteDataRequestCount.fetch_sub(1, std::memory_order_release);
    Y_ABORT_UNLESS(prev > 0);

    if (prev > 1) {
        return EWriteDataRequestCompletedAction::ContinueExecution;
    }

    // The thread that has made the final decrement should process the results
    // of other threads
    auto cur = InFlightWriteDataRequestCount.load(std::memory_order_acquire);
    Y_ABORT_UNLESS(cur == 0);

    return EWriteDataRequestCompletedAction::CollectFlushResult;
}

NCloud::NProto::TError TNodeFlushState::CollectFlushResult()
{
    Y_ABORT_UNLESS(InFlightWriteDataRequestCount.load() == 0);

    EraseIf(
        WriteDataRequests,
        [](const auto& it) { return !HasError(it.Error); });

    return WriteDataRequests.empty() ? NCloud::NProto::TError()
                                     : WriteDataRequests.front().Error;
}

}   // namespace NCloud::NFileStore::NFuse::NWriteBackCache
