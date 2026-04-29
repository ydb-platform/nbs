#include "observability.h"

namespace NCloud::NStorage::NRdma {

////////////////////////////////////////////////////////////////////////////////

TObservabilityProvider::TObservabilityProvider(
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

TLog TObservabilityProvider::CreateLog() const
{
    return Logging->CreateLog(LogComponent);
}

NMonitoring::TDynamicCountersPtr TObservabilityProvider::CreateCounters() const
{
    return Monitoring->GetCounters()
        ->GetSubgroup("counters", CountersGroupName)
        ->GetSubgroup("component", CountersComponentName);
}

}   // namespace NCloud::NStorage::NRdma
