#pragma once

#include "public.h"

#include <cloud/blockstore/libs/rdma/iface/client.h>

namespace NCloud::NBlockStore::NRdma {

////////////////////////////////////////////////////////////////////////////////

IClientPtr CreateClient(
    NVerbs::IVerbsPtr verbs,
    ILoggingServicePtr logging,
    IMonitoringServicePtr monitoring,
    TClientConfigPtr config);

}   // namespace NCloud::NBlockStore::NRdma
