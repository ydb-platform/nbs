#pragma once

#include "public.h"

#include "probes.h"
#include "request_info.h"

#include <ydb/library/actors/core/actor.h>

namespace NCloud::NBlockStore::NStorage {

LWTRACE_USING(BLOCKSTORE_STORAGE_PROVIDER);

////////////////////////////////////////////////////////////////////////////////

template <typename TMethod>
void RejectUnimplementedRequest(
    const typename TMethod::TRequest::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    auto* msg = ev->Get();

    auto requestInfo = CreateRequestInfo<TMethod>(
        ev->Sender,
        ev->Cookie,
        msg->CallContext);

    TRequestScope timer(*requestInfo);

    LWTRACK(
        RequestReceived_Partition,
        requestInfo->CallContext->LWOrbit,
        TMethod::Name,
        requestInfo->CallContext->RequestId);

    LWTRACK(
        ResponseSent_Partition,
        requestInfo->CallContext->LWOrbit,
        TMethod::Name,
        requestInfo->CallContext->RequestId);

    NCloud::Reply(
        ctx,
        *requestInfo,
        std::make_unique<typename TMethod::TResponse>(MakeError(
            E_NOT_IMPLEMENTED,
            TStringBuilder() << "Disk does not support " << TMethod::Name
        ))
    );
}

}   // namespace NCloud::NBlockStore::NStorage
