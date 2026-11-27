#pragma once

#include <cloud/storage/core/libs/actors/public.h>
#include <cloud/storage/core/libs/common/public.h>
#include <cloud/storage/core/libs/common/startable.h>
#include <cloud/storage/core/libs/icv2/tcp/address_resolver.h>

namespace NCloud::NIcV2 {

////////////////////////////////////////////////////////////////////////////////

// Start once after the actor system and completion queue. Stop cancels pending
// lookups and rejects new ones. Keep dependencies running until results arrive.
struct IActorAddressResolver
    : public IAddressResolver
    , public IStartable
{
};

using IActorAddressResolverPtr = std::shared_ptr<IActorAddressResolver>;

IActorAddressResolverPtr CreateAddressResolver(
    NCloud::IActorSystemPtr actorSystem,
    NCloud::ITaskQueuePtr completions);

}   // namespace NCloud::NIcV2
