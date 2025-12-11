#include "auth_counters.h"

namespace NCloud::NStorage {

using namespace NMonitoring;

////////////////////////////////////////////////////////////////////////////////

TAuthCounters::TAuthCounters(TDynamicCounterPtr counters, TString counterId)
{
    auto group = counters->GetSubgroup("counters", counterId)
                     ->GetSubgroup("component", "auth");

    for (int i = 0; i < (int)EAuthorizationStatus::MAX; ++i) {
        const EAuthorizationStatus status = (EAuthorizationStatus)i;
        AuthorizationStatusCounters[i] = group->GetCounter(ToString(status));
    }
}

void TAuthCounters::ReportAuthorizationStatus(EAuthorizationStatus status)
{
    AuthorizationStatusCounters[(int)status]->Inc();
}

}   // namespace NCloud::NStorage
