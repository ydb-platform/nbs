#include "requests_inflight.h"

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

TRequestsInFlight::TAddResult TRequestsInFlight::TryAddRequest(
    ui64 requestId,
    TBlockRange64 blockRange)
{
    auto existRange = Requests.Overlaps(blockRange);
    if (existRange) {
        return TAddResult{
            .Added = false,
            .DuplicateRequestId = existRange->Range.Contains(blockRange)
                                      ? existRange->Key
                                      : InvalidRequestId};
    }

    const bool inserted = Requests.AddRange(requestId, blockRange);
    Y_ABORT_UNLESS(inserted);

    return TAddResult{.Added = true, .DuplicateRequestId = InvalidRequestId};
}

void TRequestsInFlight::RemoveRequest(ui64 requestId)
{
    Requests.RemoveRange(requestId);
}

size_t TRequestsInFlight::Size() const
{
    return Requests.Size();
}

}   // namespace NCloud::NBlockStore::NStorage
