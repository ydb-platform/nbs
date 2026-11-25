#pragma once

#include "address_resolver.h"

#include <cloud/storage/core/libs/actors/public.h>

namespace NCloud::NIcV2::NPrivate {

////////////////////////////////////////////////////////////////////////////////

NActors::IActorPtr CreateAddressResolverActor(
    NCloud::ITaskQueuePtr completions);

NActors::IEventBasePtr CreateResolveRequest(
    TString host,
    ui16 port,
    TMonotonic deadline,
    NThreading::TPromise<TResolveResult> promise);

}   // namespace NCloud::NIcV2::NPrivate
