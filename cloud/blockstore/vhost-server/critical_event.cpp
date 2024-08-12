#include "critical_event.h"

#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <util/string/builder.h>
#include <util/system/spinlock.h>

namespace NCloud::NBlockStore::NVHostServer {

////////////////////////////////////////////////////////////////////////////////

namespace {

constexpr size_t MaxStoredCriticalEventCount = 1024;

TLog Log;
TCriticalEvents CriticalEventsAccumulator;
TAdaptiveLock Guard;

}   // namespace

void SetCriticalEventsLog(TLog log)
{
    Log = std::move(log);
}

void ReportCriticalEvent(TString sensorName, TString message)
{
    if (Log.IsOpen()) {
        TStringBuilder fullMessage;
        fullMessage << "CRITICAL_EVENT:" << sensorName;
        if (message) {
            fullMessage << ":" << message;
        }
        STORAGE_ERROR(fullMessage);
    }

    with_lock (Guard) {
        if (CriticalEventsAccumulator.size() >= MaxStoredCriticalEventCount) {
            return;
        }
        CriticalEventsAccumulator.push_back(
            TCriticalEvent{std::move(sensorName), std::move(message)});
    }
}

TCriticalEvents TakeAccumulatedCriticalEvents()
{
    TCriticalEvents result;
    with_lock (Guard) {
        CriticalEventsAccumulator.swap(result);
    }
    return result;
}

}   // namespace NCloud::NBlockStore::NVHostServer
