#include "critical_event.h"

#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <util/string/builder.h>
#include <util/system/spinlock.h>

namespace NCloud::NBlockStore::NVHostServer {

////////////////////////////////////////////////////////////////////////////////

TLog Log;
TCriticalEvents CriticalEventsAccumulator;
TAdaptiveLock Guard;

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
        CriticalEventsAccumulator.emplace_back(
            std::move(sensorName),
            std::move(message));
    }
}

TCriticalEvents TakeAccumulatedCriticalEvents()
{
    with_lock (Guard) {
        TCriticalEvents result;
        CriticalEventsAccumulator.swap(result);
        return result;
    }
}

}   // namespace NCloud::NBlockStore::NVHostServer
