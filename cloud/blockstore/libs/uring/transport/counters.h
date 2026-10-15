#pragma once

#include "public.h"

#include <cloud/storage/core/libs/diagnostics/monitoring.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>

#include <memory>

namespace NCloud::NBlockStore::NUring {

////////////////////////////////////////////////////////////////////////////////

struct TCounters
{
    using TCounterPtr = NMonitoring::TDynamicCounters::TCounterPtr;

    TCounterPtr Requests;
    TCounterPtr Errors;
    TCounterPtr Rejected;
    TCounterPtr Timeouts;
    TCounterPtr Reconnects;
    TCounterPtr InlineBytes;
    TCounterPtr OobBytes;
    TCounterPtr DirectSendBytes;
    TCounterPtr DirectRecvBytes;
    TCounterPtr StagingCopyBytes;
    TCounterPtr SendZcRequests;
    TCounterPtr SendZcNotifications;
    TCounterPtr SendZcCopiedNotifications;
    TCounterPtr SendZeroCopySupported;
    TCounterPtr ActiveConnections;
};

using TCountersPtr = std::shared_ptr<TCounters>;

inline TCountersPtr MakeCounters(
    const IMonitoringServicePtr& monitoring,
    const TString& component)
{
    auto group = monitoring->GetCounters()
                     ->GetSubgroup("counters", "blockstore")
                     ->GetSubgroup("component", component);

    auto counters = std::make_shared<TCounters>();
    counters->Requests = group->GetCounter("Requests", true);
    counters->Errors = group->GetCounter("Errors", true);
    counters->Rejected = group->GetCounter("Rejected", true);
    counters->Timeouts = group->GetCounter("Timeouts", true);
    counters->Reconnects = group->GetCounter("Reconnects", true);
    counters->InlineBytes = group->GetCounter("InlineBytes", true);
    counters->OobBytes = group->GetCounter("OobBytes", true);
    counters->DirectSendBytes = group->GetCounter("DirectSendBytes", true);
    counters->DirectRecvBytes = group->GetCounter("DirectRecvBytes", true);
    counters->StagingCopyBytes = group->GetCounter("StagingCopyBytes", true);
    counters->SendZcRequests = group->GetCounter("SendZcRequests", true);
    counters->SendZcNotifications =
        group->GetCounter("SendZcNotifications", true);
    counters->SendZcCopiedNotifications =
        group->GetCounter("SendZcCopiedNotifications", true);
    counters->SendZeroCopySupported =
        group->GetCounter("SendZeroCopySupported");
    counters->ActiveConnections = group->GetCounter("ActiveConnections");
    return counters;
}

}   // namespace NCloud::NBlockStore::NUring
