#pragma once

#include <library/cpp/monlib/dynamic_counters/counters.h>

#include <memory>

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

struct TRdmaForwarderCounters
{
    using TCounterPtr = NMonitoring::TDynamicCounters::TCounterPtr;

    TCounterPtr Requests;
    TCounterPtr Errors;
    TCounterPtr InFlightRequests;

    explicit TRdmaForwarderCounters(
        TIntrusivePtr<NMonitoring::TDynamicCounters> group)
    {
        Requests = group->GetCounter("Requests", true);
        Errors = group->GetCounter("Errors", true);
        InFlightRequests = group->GetCounter("InFlightRequests");
    }
};

using TRdmaForwarderCountersPtr = std::shared_ptr<TRdmaForwarderCounters>;

}   // namespace NCloud::NFileStore::NStorage
