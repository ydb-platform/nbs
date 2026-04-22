#pragma once

#include "public.h"

#include <cloud/storage/core/libs/rdma/iface/client.h>

namespace NCloud::NStorage::NRdma {

////////////////////////////////////////////////////////////////////////////////

IClientPtr CreateClient(
    NVerbs::IVerbsPtr verbs,
    TLog log,
    NMonitoring::TDynamicCountersPtr counters,
    TClientConfigPtr config);

}   // namespace NCloud::NStorage::NRdma
