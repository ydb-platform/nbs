#pragma once

#include "observability.h"
#include "public.h"

#include <cloud/storage/core/libs/rdma/iface/server.h>

namespace NCloud::NStorage::NRdma {

////////////////////////////////////////////////////////////////////////////////

IServerPtr CreateServer(
    NVerbs::IVerbsPtr verbs,
    TObservabilityProvider observabilityProvider,
    TServerConfigPtr config);

}   // namespace NCloud::NStorage::NRdma
