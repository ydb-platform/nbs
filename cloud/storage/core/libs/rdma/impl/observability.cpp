#include "observability.h"

namespace NCloud::NStorage::NRdma {

////////////////////////////////////////////////////////////////////////////////

TRdmaObservabilityProvider::TRdmaObservabilityProvider(
    ILoggingServicePtr logging,
    IMonitoringServicePtr monitoring,
    TString logComponent,
    TString countersGroupName,
    TString countersComponentName)
    : Logging(std::move(logging))
    , Monitoring(std::move(monitoring))
    , LogComponent(std::move(logComponent))
    , CountersGroupName(std::move(countersGroupName))
    , CountersComponentName(std::move(countersComponentName))
{}

TLog TRdmaObservabilityProvider::CreateLog() const
{
    return Logging->CreateLog(LogComponent);
}

NMonitoring::TDynamicCountersPtr
TRdmaObservabilityProvider::CreateCounters() const
{
    return Monitoring->GetCounters()
        ->GetSubgroup("counters", CountersGroupName)
        ->GetSubgroup("component", CountersComponentName);
}

}   // namespace NCloud::NStorage::NRdma
