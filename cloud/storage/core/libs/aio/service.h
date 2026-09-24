#pragma once

#include <cloud/storage/core/libs/common/public.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>

namespace NCloud {

////////////////////////////////////////////////////////////////////////////////

struct TAioServiceParams
{
    static constexpr ui32 DefaultMaxEvents = 1024;

    ui32 MaxEvents = DefaultMaxEvents;

    TString CompletionThreadName = "AIO";

    // Defaults to an orphan group: it is never registered with monitoring, so
    // a caller that does not pass counters collects into nothing.
    TIntrusivePtr<NMonitoring::TDynamicCounters> Counters =
        MakeIntrusive<NMonitoring::TDynamicCounters>();
};

IFileIOServicePtr CreateAIOService(TAioServiceParams params = {});
IFileIOServiceFactoryPtr CreateAIOServiceFactory(TAioServiceParams params = {});

IFileIOServicePtr CreateThreadedAIOService(
    ui32 threadCount,
    TAioServiceParams params = {});

}   // namespace NCloud
