#include "server.h"

#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <cloud/contrib/vhost/include/vhost/server.h>

#include <library/cpp/logger/log.h>

namespace NCloud::NFileStore::NVhost {

namespace {

////////////////////////////////////////////////////////////////////////////////

TLog VhostLog;

ELogPriority GetLogPriority(LogLevel level)
{
    switch (level) {
        case LOG_ERROR:
            return TLOG_ERR;
        case LOG_WARNING:
            return TLOG_WARNING;
        case LOG_INFO:
            return TLOG_INFO;
        case LOG_DEBUG:
            return TLOG_DEBUG;
    }
}

void vhd_log(LogLevel level, const char* format, ...)
{
    va_list params;
    va_start(params, format);

    ELogPriority priority = GetLogPriority(level);
    if (priority <= VhostLog.FiltrationLevel()) {
        Printf(VhostLog << priority << ": ", format, params);
    }

    va_end(params);
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void InitLog(ILoggingServicePtr logging)
{
    VhostLog = logging->CreateLog("NFS_VHOST");
}

void StartServer()
{
    int res = vhd_start_vhost_server(vhd_log);
    Y_ABORT_UNLESS(res == 0, "Error starting vhost server");
}

void StopServer()
{
    vhd_stop_vhost_server();
}

}   // namespace NCloud::NFileStore::NVhost
