#pragma once

#include <cloud/storage/core/libs/actors/public.h>

#include <util/system/types.h>

#include <functional>

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

// Schedules the requests that one step of a create/resize operation sends to
// the shards. The owner sends a request and reports its response.
class TShardRequestCompanion
{
public:
    using TSendRequest =
        std::function<void(const NActors::TActorContext& ctx, ui32 shardIndex)>;

private:
    const TSendRequest SendRequest;
    const ui32 MaxRequestsInFlight;

    ui32 NextShard = 0;
    ui32 EndShard = 0;
    ui32 ResponsesLeft = 0;

public:
    // Zero maxRequestsInFlight means no limit.
    TShardRequestCompanion(TSendRequest sendRequest, ui32 maxRequestsInFlight);

    // Sends the requests for the shards [beginShardIndex, endShardIndex).
    void StartRequests(
        const NActors::TActorContext& ctx,
        ui32 beginShardIndex,
        ui32 endShardIndex);
    // Accounts a response and sends the next request.
    void OnResponse(const NActors::TActorContext& ctx);
    // Whether any responses are still expected.
    bool HasShardsLeft() const;
};

}   // namespace NCloud::NFileStore::NStorage
