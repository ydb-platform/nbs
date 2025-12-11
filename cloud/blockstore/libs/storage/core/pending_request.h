#pragma once

#include <cloud/blockstore/libs/kikimr/public.h>
#include <cloud/blockstore/libs/storage/core/request_info.h>

#include <utility>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

struct TPendingRequest
{
    NActors::IEventHandlePtr Event;
    TRequestInfoPtr RequestInfo;

    TPendingRequest() = default;

    TPendingRequest(NActors::IEventHandlePtr event, TRequestInfoPtr requestInfo)
        : Event(std::move(event))
        , RequestInfo(std::move(requestInfo))
    {}
};

void SendPendingRequests(
    const NActors::TActorContext& ctx,
    TDeque<TPendingRequest>& pendingRequests);
void CancelPendingRequests(
    const NActors::TActorContext& ctx,
    TDeque<TPendingRequest>& pendingRequests);

}   // namespace NCloud::NBlockStore::NStorage
