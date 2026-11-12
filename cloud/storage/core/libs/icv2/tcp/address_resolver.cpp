#include "address_resolver.h"

#include <cloud/storage/core/libs/common/task_queue.h>

#include <util/network/address.h>

namespace NCloud::NIcV2 {

////////////////////////////////////////////////////////////////////////////////

namespace {

////////////////////////////////////////////////////////////////////////////////

TResolveResult ResolveHost(const TString& host, ui16 port)
{
    try {
        const TNetworkAddress resolved(host, port);
        TVector<NInterconnect::TAddress> addresses;
        for (auto it = resolved.Begin(); it != resolved.End(); ++it) {
            if (it->ai_family == AF_INET || it->ai_family == AF_INET6) {
                NAddr::TAddrInfo address(&*it);
                addresses.emplace_back(address);
            }
        }
        if (addresses.empty()) {
            return MakeError(E_UNAVAILABLE, "No IP addresses for " + host);
        }
        return addresses;
    } catch (const yexception& e) {
        return MakeError(
            E_UNAVAILABLE,
            "Cannot resolve " + host + ": " + e.what());
    }
}

////////////////////////////////////////////////////////////////////////////////

class TAddressResolver final: public IAddressResolver
{
    const ITaskQueuePtr Executor;

public:
    explicit TAddressResolver(ITaskQueuePtr executor)
        : Executor(std::move(executor))
    {}

    TResolveFuture Resolve(const TString& host, ui16 port) override
    {
        return Executor->Execute([host, port]
                                 { return ResolveHost(host, port); });
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IAddressResolver::~IAddressResolver() = default;

IAddressResolverPtr CreateAddressResolver(ITaskQueuePtr executor)
{
    Y_DEBUG_ABORT_UNLESS(executor);

    return std::make_shared<TAddressResolver>(std::move(executor));
}

}   // namespace NCloud::NIcV2
