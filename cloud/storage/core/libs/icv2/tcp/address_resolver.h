#pragma once

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/public.h>

#include <contrib/ydb/library/actors/interconnect/address/interconnect_address.h>

#include <util/generic/vector.h>

namespace NCloud::NIcV2 {

////////////////////////////////////////////////////////////////////////////////

using TResolveResult = TResultOrError<TVector<NInterconnect::TAddress>>;
using TResolveFuture = NThreading::TFuture<TResolveResult>;

// Resolve without waiting for DNS on the calling thread.
struct IAddressResolver
{
    virtual ~IAddressResolver();

    virtual TResolveFuture Resolve(const TString& host, ui16 port) = 0;
};

using IAddressResolverPtr = std::shared_ptr<IAddressResolver>;

// Use a running asynchronous executor with workers dedicated to blocking DNS
// lookups. Do not use I/O or request completion workers. Keep the executor
// running until all lookup futures are ready; DNS has no timeout or
// cancellation at this interface.
IAddressResolverPtr CreateAddressResolver(ITaskQueuePtr executor);

}   // namespace NCloud::NIcV2
