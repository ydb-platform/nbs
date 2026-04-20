#pragma once

#include "public.h"

#include <cloud/storage/core/libs/rdma/iface/server.h>

namespace NCloud::NBlockStore::NRdma {

////////////////////////////////////////////////////////////////////////////////

IServerPtr CreateServer(
    NVerbs::IVerbsPtr verbs,
    ILoggingServicePtr logging,
    IMonitoringServicePtr monitoring,
    TServerConfigPtr config);

}   // namespace NCloud::NBlockStore::NRdma
