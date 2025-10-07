#pragma once

#include "public.h"

namespace NCloud {

////////////////////////////////////////////////////////////////////////////////

#define STORAGE_CRITICAL_EVENTS(xxx)                                           \
    xxx(CpuWaitCounterReadError)                                               \
    xxx(HiveProxyConcurrentLockError)                                          \
    xxx(BackupTabletBootInfosFailure)                                          \
    xxx(LoadTabletBootInfoBackupFailure)                                       \
    xxx(BackupPathDescriptionsFailure)                                         \
    xxx(LoadPathDescriptionBackupFailure)                                      \
    xxx(MlockFailed)                                                           \
    xxx(ConfigDispatcherItemParseError)                                        \
    xxx(GetConfigsFromCmsYamlParseError)                                       \
// STORAGE_CRITICAL_EVENTS

////////////////////////////////////////////////////////////////////////////////

void SetCriticalEventsLog(TLog log);
void InitCriticalEventsCounter(NMonitoring::TDynamicCountersPtr counters);

TString GetCriticalEventFullName(const TString& name);

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
    TStringBuf expr,
    TStringBuf message);

////////////////////////////////////////////////////////////////////////////////

#define STORAGE_CHECK_PRECONDITION_C(expr, message)                            \
    if (Y_UNLIKELY(!(expr))) {                                                 \
        ReportPreconditionFailed(                                              \
            __SOURCE_FILE_IMPL__,                                              \
            __LINE__,                                                          \
            __FUNCTION__,                                                      \
            #expr,                                                             \
            message);                                                          \
    }                                                                          \

#define STORAGE_CHECK_PRECONDITION(expr) STORAGE_CHECK_PRECONDITION_C(expr, "")

}   // namespace NCloud
