#include "log.h"

#include "fuse.h"

#include <cloud/storage/core/libs/diagnostics/logging.h>

namespace NCloud::NFileStore::NFuse {

namespace {

////////////////////////////////////////////////////////////////////////////////

TLog FuseLog;

#if defined(FUSE_VIRTIO)

ELogPriority GetLogPriority(fuse_log_level level)
{
    switch (level) {
        case FUSE_LOG_EMERG:
            return TLOG_EMERG;
        case FUSE_LOG_ALERT:
            return TLOG_ALERT;
        case FUSE_LOG_CRIT:
            return TLOG_CRIT;
        case FUSE_LOG_ERR:
            return TLOG_ERR;
        case FUSE_LOG_WARNING:
            return TLOG_WARNING;
        case FUSE_LOG_NOTICE:
            return TLOG_NOTICE;
        case FUSE_LOG_INFO:
            return TLOG_INFO;
        case FUSE_LOG_DEBUG:
            return TLOG_DEBUG;
    }
}

void fuse_log(fuse_log_level level, const char* format, va_list params)
{
    ELogPriority priority = GetLogPriority(level);
    if (priority <= FuseLog.FiltrationLevel()) {
        Printf(FuseLog << priority << ": ", format, params);
    }
}

#endif

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void InitLog(ILoggingServicePtr logging)
{
    FuseLog = logging->CreateLog("NFS_FUSE");

#if defined(FUSE_VIRTIO)
    fuse_set_log_func(fuse_log);
#endif
}

}   // namespace NCloud::NFileStore::NFuse
