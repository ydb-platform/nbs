#pragma once

#include <library/cpp/logger/log.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/system/types.h>

namespace NCloud::NBlockStore::NVHostServer {

////////////////////////////////////////////////////////////////////////////////

struct TCriticalEvent
{
    TString Name;
    TString Message;
};
using TCriticalEvents = TVector<TCriticalEvent>;

void SetCriticalEventsLog(TLog log);
void ReportCriticalEvent(TString name, TString message);

TCriticalEvents TakeAccumulatedCriticalEvents();

}   // namespace NCloud::NBlockStore::NVHostServer
