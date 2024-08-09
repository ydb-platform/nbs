#include "critical_events.h"
#include "public.h"

#include <library/cpp/logger/log.h>
#include <library/cpp/monlib/dynamic_counters/counters.h>

#include <util/string/builder.h>

namespace NCloud {

using namespace NMonitoring;

namespace {

NMonitoring::TDynamicCountersPtr CriticalEvents;
TLog Log;

}  // namespace

////////////////////////////////////////////////////////////////////////////////

void SetCriticalEventsLog(TLog log)
{
    Log = std::move(log);
}

void InitCriticalEventsCounter(NMonitoring::TDynamicCountersPtr counters)
{
    CriticalEvents = std::move(counters);

#define STORAGE_INIT_CRITICAL_EVENT_COUNTER(name)                              \
    *CriticalEvents->GetCounter(GetCriticalEventFor##name(), true) = 0;        \
// STORAGE_INIT_CRITICAL_EVENT_COUNTER

    STORAGE_CRITICAL_EVENTS(STORAGE_INIT_CRITICAL_EVENT_COUNTER)
#undef STORAGE_INIT_CRITICAL_EVENT_COUNTER
}

TString GetCriticalEventFullName(const TString& name)
{
    return "AppCriticalEvents/" + name;
}

TString ReportCriticalEvent(
    const TString& sensorName,
    const TString& message,
    bool verifyDebug)
{
    if (verifyDebug) {
        Y_DEBUG_ABORT_UNLESS(0);
    }

    if (CriticalEvents) {
        auto counter = CriticalEvents->GetCounter(
            sensorName,
            true);
        counter->Inc();
    }

    TStringBuilder fullMessage;
    fullMessage << "CRITICAL_EVENT:" << sensorName;
    if (message) {
        fullMessage << ":" << message;
    }

    if (Log.IsNotNullLog()) {
        Log.AddLog("%s", fullMessage.c_str());
    } else {
        // Write message and \n in one call. This will reduce the chance of
        // shuffling with writings of other threads.
        Cerr << fullMessage + '\n';
        Cerr.Flush();
    }

    return fullMessage;
}

#define STORAGE_DEFINE_CRITICAL_EVENT_ROUTINE(name)                            \
    TString Report##name(const TString& message)                               \
    {                                                                          \
        return ReportCriticalEvent(                                            \
            GetCriticalEventFor##name(),                                       \
            message,                                                           \
            false);                                                            \
    }                                                                          \
                                                                               \
    const TString GetCriticalEventFor##name()                                  \
    {                                                                          \
        return "AppCriticalEvents/"#name;                                      \
    }                                                                          \
// STORAGE_DEFINE_CRITICAL_EVENT_ROUTINE

    STORAGE_CRITICAL_EVENTS(STORAGE_DEFINE_CRITICAL_EVENT_ROUTINE)
#undef STORAGE_DEFINE_CRITICAL_EVENT_ROUTINE

////////////////////////////////////////////////////////////////////////////////

void ReportPreconditionFailed(
    TStringBuf file,
    int line,
    TStringBuf func,
    TStringBuf expr,
    TStringBuf message)
{
    ReportCriticalEvent(
        "PreconditionFailed",
        TStringBuilder()
            << file << ":" << line
            << " " << func << "(): requirement " << expr
            << " failed. " << message,
        true    // verifyDebug
    );
}

}   // namespace NCloud
