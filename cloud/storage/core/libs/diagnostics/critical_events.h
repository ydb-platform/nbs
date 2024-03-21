#pragma once

#include "public.h"

namespace NCloud {

////////////////////////////////////////////////////////////////////////////////

#define STORAGE_CRITICAL_EVENTS(xxx)                                           \
    xxx(CpuWaitCounterReadError)                                               \
    xxx(HiveProxyConcurrentLockError)                                          \
    xxx(BackupTabletBootInfosFailure)                                          \
    xxx(MlockFailed)                                                           \
// STORAGE_CRITICAL_EVENTS

////////////////////////////////////////////////////////////////////////////////

void InitCriticalEventsCounter(NMonitoring::TDynamicCountersPtr counters);

TString ReportCriticalEvent(
    const TString& sensorName,
    const TString& message,
    bool verifyDebug);

#define STORAGE_DECLARE_CRITICAL_EVENT_ROUTINE(name)                           \
    TString Report##name(const TString& message = "");                         \
    const TString GetCriticalEventFor##name();                                 \
// STORAGE_DECLARE_CRITICAL_EVENT_ROUTINE

    STORAGE_CRITICAL_EVENTS(STORAGE_DECLARE_CRITICAL_EVENT_ROUTINE)
#undef STORAGE_DECLARE_CRITICAL_EVENT_ROUTINE

void ReportPreconditionFailed(
    TStringBuf file,
    int line,
    TStringBuf func,
    TStringBuf expr);

////////////////////////////////////////////////////////////////////////////////

#define STORAGE_CHECK_PRECONDITION(expr)                                             \
    if (!(expr)) {                                                             \
        ReportPreconditionFailed(                                              \
            __SOURCE_FILE_IMPL__,                                              \
            __LINE__,                                                          \
            __FUNCTION__,                                                      \
            #expr);                                                            \
    }                                                                          \

}   // namespace NCloud
