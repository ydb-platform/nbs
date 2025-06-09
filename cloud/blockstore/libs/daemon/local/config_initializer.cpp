#include "config_initializer.h"
#include "options.h"

#include <cloud/storage/core/libs/diagnostics/logging.h>

namespace NCloud::NBlockStore::NServer {

using namespace NCloud::NBlockStore::NDiscovery;

////////////////////////////////////////////////////////////////////////////////

TConfigInitializerLocal::TConfigInitializerLocal(TOptionsLocalPtr options)
    : TConfigInitializerCommon(options)
    , Options(options)
{}

ui32 TConfigInitializerLocal::GetLogDefaultLevel() const
{
    ui32 defaultLevel = 5;

    if (Options->VerboseLevel) {
        auto level = GetLogLevel(Options->VerboseLevel);
        Y_ENSURE(level, "unknown log level: " << Options->VerboseLevel.Quote());
        defaultLevel = *level;
    }

    return defaultLevel;
}

ui32 TConfigInitializerLocal::GetMonitoringPort() const
{
    return Options->MonitoringPort;
}

TString TConfigInitializerLocal::GetMonitoringAddress() const
{
    return Options->MonitoringAddress;
}

ui32 TConfigInitializerLocal::GetMonitoringThreads() const
{
    return Options->MonitoringThreads ? Options->MonitoringThreads : 1;
}

bool TConfigInitializerLocal::GetUseNonreplicatedRdmaActor() const
{
    return false;
}

TDuration TConfigInitializerLocal::GetInactiveClientsTimeout() const
{
    return TDuration::Max();
}

TString TConfigInitializerLocal::GetLogBackendFileName() const
{
    return "";
}


}   // namespace NCloud::NBlockStore::NServer
