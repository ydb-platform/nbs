#pragma once

#include <library/cpp/monlib/dynamic_counters/counters.h>

namespace NCloud {

TIntrusivePtr<NMonitoring::TDynamicCounters> MakeVisibilitySubgroup(
    NMonitoring::TDynamicCounters& counters,
    const TString& name,
    const TString& value,
    NMonitoring::TCountableBase::EVisibility visibility);

}   // namespace NCloud
