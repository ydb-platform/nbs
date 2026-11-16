#pragma once

#include <library/cpp/monlib/dynamic_counters/counters.h>

#include <memory>

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

struct TRdmaForwarderCounters
{
    using TCounterPtr = NMonitoring::TDynamicCounters::TCounterPtr;

    TCounterPtr Errors;

    explicit TRdmaForwarderCounters(
        TIntrusivePtr<NMonitoring::TDynamicCounters> group)
    {
        Errors = group->GetCounter("Errors", true);
    }
};

using TRdmaForwarderCountersPtr = std::shared_ptr<TRdmaForwarderCounters>;

}   // namespace NCloud::NFileStore::NStorage
