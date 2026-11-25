#pragma once

#include "public.h"

#include <cloud/blockstore/libs/cells/iface/public.h>
#include <cloud/blockstore/libs/icv2/iface/data_endpoint_factory.h>
#include <cloud/blockstore/libs/service/public.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/startable.h>
#include <cloud/storage/core/libs/diagnostics/public.h>
#include <cloud/storage/core/libs/icv2/tcp/address_resolver.h>

#include <contrib/ydb/library/actors/interconnect/address/interconnect_address.h>

#include <util/datetime/base.h>
#include <util/generic/string.h>

namespace NCloud::NBlockStore::NIcV2 {

////////////////////////////////////////////////////////////////////////////////

// Start once after initialization. Stop is idempotent; call it outside
// callbacks before releasing dependencies.
struct IService
    : public IStartable
    , public IDataEndpointFactory
{
    [[nodiscard]] virtual const NInterconnect::TAddress& GetAddress() const = 0;

    // A terminal error closes all connections. Create a new service to recover.
    [[nodiscard]] virtual NProto::TError GetError() const = 0;

    // Safe in callbacks. Call Stop outside callbacks to finish shutdown.
    virtual void RequestStop() = 0;
};

// Requires CellsEnabled and IcV2.Enabled. Keep the resolver and its
// dependencies available until Stop returns.
TResultOrError<IServicePtr> CreateService(
    NCells::TCellsConfigPtr config,
    IBlockStorePtr dataService,
    NCloud::NIcV2::IAddressResolverPtr resolver,
    ITimerPtr timer,
    ISchedulerPtr scheduler,
    ILoggingServicePtr logging,
    NMonitoring::TDynamicCountersPtr counters);

}   // namespace NCloud::NBlockStore::NIcV2
