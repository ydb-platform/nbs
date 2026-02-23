#include "node_flush_state.h"

#include <cloud/storage/core/libs/common/error.h>

#include <util/generic/algorithm.h>

namespace NCloud::NFileStore::NFuse::NWriteBackCache {

////////////////////////////////////////////////////////////////////////////////

TNodeFlushState::TNodeFlushState(
    ui64 nodeId,
    TVector<std::shared_ptr<NProto::TWriteDataRequest>> requests,
    size_t affectedUnflushedRequestCount)
    : NodeId(nodeId)
    , AffectedUnflushedRequestCount(affectedUnflushedRequestCount)
{
    Y_ABORT_UNLESS(!requests.empty());
    Y_ABORT_UNLESS(affectedUnflushedRequestCount > 0);

    for (auto& request: requests) {
        WriteDataRequests.emplace_back(std::move(request));
    }
}

size_t TNodeFlushState::CheckedDecrementInFlight()
{
    auto prev = InFlightWriteDataRequestCount.fetch_sub(1);
    Y_ABORT_UNLESS(prev > 0);
    return prev - 1;
}

void TNodeFlushState::EraseCompletedRequests()
{
    EraseIf(
        WriteDataRequests,
        [](const auto& it) { return !HasError(it.Error); });
}

}   // namespace NCloud::NFileStore::NFuse::NWriteBackCache
