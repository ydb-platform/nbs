#include "observability.h"

namespace NCloud::NStorage::NRdma {

////////////////////////////////////////////////////////////////////////////////

TObservabilityProvider::TObservabilityProvider(
    ILoggingServicePtr logging,
    TString logComponent,
    NMonitoring::TDynamicCountersPtr counters)
    : Logging(std::move(logging))
    , LogComponent(std::move(logComponent))
    , Counters(std::move(counters))
{}

TLog TObservabilityProvider::CreateLog() const
{
    return Logging->CreateLog(LogComponent);
}

NMonitoring::TDynamicCountersPtr TObservabilityProvider::CreateCounters() const
{
    return Counters;
}

}   // namespace NCloud::NStorage::NRdma
