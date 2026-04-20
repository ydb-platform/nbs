#pragma once

#include "public.h"

#include <cloud/storage/core/libs/rdma/iface/client.h>

namespace NCloud::NBlockStore::NRdma {

////////////////////////////////////////////////////////////////////////////////

IClientPtr CreateClient(
    NVerbs::IVerbsPtr verbs,
    ILoggingServicePtr logging,
    IMonitoringServicePtr monitoring,
    TClientConfigPtr config);

}   // namespace NCloud::NBlockStore::NRdma
