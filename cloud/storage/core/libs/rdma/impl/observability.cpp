#include "observability.h"

namespace NCloud::NStorage::NRdma {

////////////////////////////////////////////////////////////////////////////////

TObservabilityProvider::TObservabilityProvider(
    ILoggingServicePtr logging,
    TString logComponent,
    NMonitoring::TDynamicCountersPtr countersRoot,
    TString countersGroupTag,
    TString componentTag)
    : Logging(std::move(logging))
    , LogComponent(std::move(logComponent))
    , CountersRoot(std::move(countersRoot))
    , CountersGroupTag(std::move(countersGroupTag))
    , ComponentTag(std::move(componentTag))
{}

TLog TObservabilityProvider::CreateLog() const
{
    return Logging->CreateLog(LogComponent);
}

NMonitoring::TDynamicCountersPtr TObservabilityProvider::CreateCounters() const
{
    return CountersRoot
        ->GetSubgroup("counters", CountersGroupTag)
        ->GetSubgroup("component", ComponentTag);
}

}   // namespace NCloud::NStorage::NRdma
