#pragma once

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/public.h>

#include <contrib/ydb/library/actors/interconnect/address/interconnect_address.h>

#include <library/cpp/time_provider/monotonic.h>

#include <util/generic/vector.h>

namespace NActors {
class TActorSystem;
}   // namespace NActors

namespace NCloud::NIcV2 {

////////////////////////////////////////////////////////////////////////////////

using TResolveResult = TResultOrError<TVector<NInterconnect::TAddress>>;
using TResolveFuture = NThreading::TFuture<TResolveResult>;

// Resolve through the interconnect DNS service without waiting on the calling
// thread. Destroying the resolver cancels pending futures.
struct IAddressResolver
{
    virtual ~IAddressResolver();

    // Pass the deadline of the operation that needs these addresses.
    virtual TResolveFuture
    Resolve(const TString& host, ui16 port, TMonotonic deadline) = 0;
};

using IAddressResolverPtr = std::shared_ptr<IAddressResolver>;

////////////////////////////////////////////////////////////////////////////////

// Supply a running actor system with its DNS service and an asynchronous
// completion executor. Keep both running until all results and continuations
// finish, including after destroying the resolver. Continuations must not
// throw. Finish concurrent Resolve calls before destroying the resolver.
IAddressResolverPtr CreateAddressResolver(
    NActors::TActorSystem* actorSystem,
    ITaskQueuePtr completions);

}   // namespace NCloud::NIcV2
