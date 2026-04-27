#pragma once

#include "public.h"

#include "observability.h"

#include <cloud/storage/core/libs/rdma/iface/client.h>

namespace NCloud::NStorage::NRdma {

////////////////////////////////////////////////////////////////////////////////

IClientPtr CreateClient(
    NVerbs::IVerbsPtr verbs,
    TRdmaObservabilityProvider observabilityProvider,
    TClientConfigPtr config);

}   // namespace NCloud::NStorage::NRdma
