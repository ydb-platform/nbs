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
    xxx(FailedToParseRdmaError)                                                \
    xxx(FailedToSerializeRdmaError)                                            \
    xxx(SchemeCacheError)                                                      \
    xxx(FileRingBufferCorruptionDetectedError)                                 \
    xxx(FileRingBufferInitializationError)                                     \
    xxx(AccessToCorruptedFileRingBufferError)                                  \
    xxx(JournalStrandedRecordDetectedError)                                    \
    xxx(JournalledDeviceCreationError)                                         \
// STORAGE_CRITICAL_EVENTS

#define STORAGE_IMPOSSIBLE_EVENTS(xxx)                                         \
    xxx(UnexpectedEvent)                                                       \
// STORAGE_IMPOSSIBLE_EVENTS

////////////////////////////////////////////////////////////////////////////////

void SetCriticalEventsLog(TLog log);
void InitCriticalEventsCounter(NMonitoring::TDynamicCountersPtr counters);

// Test-only guard for tests that intentionally trigger impossible events.
// Suppresses aborts while logging and counters remain active.
// Restores the previous abort setting when it leaves scope.
// The override affects the whole process; tests must run sequentially within
// it.
class TImpossibleEventsAbortGuard
{
private:
    const bool WasAbortDisabled;

    explicit TImpossibleEventsAbortGuard(bool wasAbortDisabled);

    friend TImpossibleEventsAbortGuard
    DisableImpossibleEventsAbortForCurrentTest();

public:
    TImpossibleEventsAbortGuard(const TImpossibleEventsAbortGuard&) = delete;
    TImpossibleEventsAbortGuard& operator=(
        const TImpossibleEventsAbortGuard&) = delete;

    ~TImpossibleEventsAbortGuard();
};

[[nodiscard]] TImpossibleEventsAbortGuard
DisableImpossibleEventsAbortForCurrentTest();

TString GetCriticalEventFullName(const TString& name);
TString GetImpossibleEventFullName(const TString& name);

TString ReportCriticalEvent(
    const TString& sensorName,
    const TString& message,
    bool verifyDebug);

void ReportCriticalEventWithoutLogging(const TString& sensorName);

TString LogCriticalEvent(const TString& sensorName, const TString& message);

#define STORAGE_DECLARE_CRITICAL_EVENT_ROUTINE(name)                           \
    TString Report##name(const TString& message = "");                         \
    const TString GetCriticalEventFor##name();                                 \
// STORAGE_DECLARE_CRITICAL_EVENT_ROUTINE

    STORAGE_CRITICAL_EVENTS(STORAGE_DECLARE_CRITICAL_EVENT_ROUTINE)
#undef STORAGE_DECLARE_CRITICAL_EVENT_ROUTINE

#define STORAGE_DECLARE_IMPOSSIBLE_EVENT_ROUTINE(name)                         \
    TString Report##name(const TString& message = "");                         \
    const TString GetImpossibleEventFor##name();                               \
// STORAGE_DECLARE_CRITICAL_EVENT_ROUTINE

    STORAGE_IMPOSSIBLE_EVENTS(STORAGE_DECLARE_IMPOSSIBLE_EVENT_ROUTINE)
#undef STORAGE_DECLARE_IMPOSSIBLE_EVENT_ROUTINE

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
