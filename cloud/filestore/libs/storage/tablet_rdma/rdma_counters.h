#pragma once

#include <library/cpp/monlib/dynamic_counters/counters.h>

#include <memory>

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

struct TRdmaForwarderCounters
{
    using TCounterPtr = NMonitoring::TDynamicCounters::TCounterPtr;

    TCounterPtr Requests;
    TCounterPtr OkReplies;
    TCounterPtr ErrorReplies;
    TCounterPtr Timeouts;
    TCounterPtr PipeFailures;
    TCounterPtr InvalidRequests;
    TCounterPtr ParseErrors;
    TCounterPtr UnsupportedMethods;
    TCounterPtr DroppedFileSystemEvents;
    TCounterPtr InFlightRequests;

    explicit TRdmaForwarderCounters(
        TIntrusivePtr<NMonitoring::TDynamicCounters> group)
    {
        Requests = group->GetCounter("Requests", true);
        OkReplies = group->GetCounter("OkReplies", true);
        ErrorReplies = group->GetCounter("ErrorReplies", true);
        Timeouts = group->GetCounter("Timeouts", true);
        PipeFailures = group->GetCounter("PipeFailures", true);
        InvalidRequests = group->GetCounter("InvalidRequests", true);
        ParseErrors = group->GetCounter("ParseErrors", true);
        UnsupportedMethods = group->GetCounter("UnsupportedMethods", true);
        DroppedFileSystemEvents =
            group->GetCounter("DroppedFileSystemEvents", true);
        InFlightRequests = group->GetCounter("InFlightRequests");
    }
};

using TRdmaForwarderCountersPtr = std::shared_ptr<TRdmaForwarderCounters>;

}   // namespace NCloud::NFileStore::NStorage
