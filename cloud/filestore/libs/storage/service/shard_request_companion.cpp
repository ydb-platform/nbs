#include "shard_request_companion.h"

#include <util/generic/utility.h>
#include <util/system/yassert.h>

namespace NCloud::NFileStore::NStorage {

using namespace NActors;

////////////////////////////////////////////////////////////////////////////////

TShardRequestCompanion::TShardRequestCompanion(
        TSendRequest sendRequest,
        ui32 maxRequestsInFlight)
    : SendRequest(std::move(sendRequest))
    , MaxRequestsInFlight(maxRequestsInFlight)
{}

void TShardRequestCompanion::StartRequests(
    const TActorContext& ctx,
    ui32 beginShardIndex,
    ui32 endShardIndex)
{
    NextShard = beginShardIndex;
    EndShard = endShardIndex;
    ResponsesLeft = endShardIndex - beginShardIndex;

    ui32 requestCount = ResponsesLeft;
    if (MaxRequestsInFlight) {
        requestCount = Min(requestCount, MaxRequestsInFlight);
    }

    for (ui32 i = 0; i < requestCount; ++i) {
        SendRequest(ctx, NextShard++);
    }
}

void TShardRequestCompanion::OnResponse(const TActorContext& ctx)
{
    Y_ABORT_UNLESS(ResponsesLeft);
    --ResponsesLeft;

    if (NextShard < EndShard) {
        SendRequest(ctx, NextShard++);
    }
}

bool TShardRequestCompanion::HasShardsLeft() const
{
    return ResponsesLeft != 0;
}

}   // namespace NCloud::NFileStore::NStorage
