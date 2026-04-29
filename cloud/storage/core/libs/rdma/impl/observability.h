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
    TString LogComponent;
    NMonitoring::TDynamicCountersPtr CountersRoot;
    TString CountersGroupTag;
    TString ComponentTag;

public:
    TObservabilityProvider(
        ILoggingServicePtr logging,
        TString logComponent,
        NMonitoring::TDynamicCountersPtr countersRoot,
        TString countersGroupTag,
        TString componentTag);

    TLog CreateLog() const;

    NMonitoring::TDynamicCountersPtr CreateCounters() const;
};

}   // namespace NCloud::NStorage::NRdma
