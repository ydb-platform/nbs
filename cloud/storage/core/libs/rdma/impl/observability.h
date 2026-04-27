#pragma once

#include "public.h"

#include <cloud/storage/core/libs/diagnostics/logging.h>
#include <cloud/storage/core/libs/diagnostics/monitoring.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>

namespace NCloud::NStorage::NRdma {

////////////////////////////////////////////////////////////////////////////////

class TObservabilityProvider
{
private:
    ILoggingServicePtr Logging;
    IMonitoringServicePtr Monitoring;
    TString LogComponent;
    TString CountersGroupName;
    TString CountersComponentName;

public:
    TObservabilityProvider(
        ILoggingServicePtr logging,
        IMonitoringServicePtr monitoring,
        TString logComponent,
        TString countersGroupName,
        TString countersComponentName);

    TLog CreateLog() const;

    NMonitoring::TDynamicCountersPtr CreateCounters() const;
};

}   // namespace NCloud::NStorage::NRdma
