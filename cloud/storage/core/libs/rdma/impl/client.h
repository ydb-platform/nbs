#pragma once

#include "public.h"

#include <cloud/storage/core/libs/rdma/iface/client.h>

namespace NCloud::NStorage::NRdma {

////////////////////////////////////////////////////////////////////////////////

IClientPtr CreateClient(
    NVerbs::IVerbsPtr verbs,
    ILoggingServicePtr logging,
    IMonitoringServicePtr monitoring,
    TString logComponent,
    TString countersGroupName,
    TString countersComponentName,
    TClientConfigPtr config);

}   // namespace NCloud::NStorage::NRdma
