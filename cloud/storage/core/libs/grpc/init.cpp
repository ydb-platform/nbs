#include "init.h"

#include <contrib/libs/grpc/include/grpc/grpc.h>
#include <contrib/libs/grpc/include/grpc/support/log.h>

#include <library/cpp/deprecated/atomic/atomic.h>
#include <library/cpp/logger/log.h>

#include <util/system/mutex.h>
#include <util/system/src_location.h>

#include <atomic>

namespace NCloud {

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TGrpcState
{
    TAtomic Counter = 0;
    TLog Log;
};

TGrpcState* GrpcState = nullptr;
TMutex GrpcStateMutex;

////////////////////////////////////////////////////////////////////////////////

gpr_log_severity LogPriorityToSeverity(ELogPriority priority)
{
    if (priority <= TLOG_ERR) {
        return GPR_LOG_SEVERITY_ERROR;
    } else if (priority <= TLOG_INFO) {
        return GPR_LOG_SEVERITY_INFO;
    } else {
        return GPR_LOG_SEVERITY_DEBUG;
    }
}

ELogPriority LogSeverityToPriority(gpr_log_severity severity)
{
    switch (severity) {
        default:
        case GPR_LOG_SEVERITY_DEBUG:
            return TLOG_DEBUG;
        case GPR_LOG_SEVERITY_INFO:
            return TLOG_INFO;
        case GPR_LOG_SEVERITY_ERROR:
            return TLOG_ERR;
    }
}

void AddLog(gpr_log_func_args* args)
{
    auto file = ::NPrivate::StripRoot({
        args->file,
        static_cast<ui32>(strlen(args->file))});

    Y_ABORT_UNLESS(GrpcState);

    GrpcState->Log << LogSeverityToPriority(args->severity)
        << TSourceLocation(file.As<TStringBuf>(), args->line)
        << ": " << args->message;
}

void EnableGRpcTracing()
{
    static const char* tracers[] = {
        "api",
        "channel",
        "client_channel_call",
        "client_channel_routing",
        "connectivity_state",
        "handshaker",
        "http",
        "http2_stream_state",
        "op_failure",
        "tcp",
        "timer",
    };

    for (size_t i = 0; i < Y_ARRAY_SIZE(tracers); ++i) {
        grpc_tracer_set_enabled(tracers[i], 1);
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TGrpcInitializer::TGrpcInitializer()
{
    with_lock (GrpcStateMutex) {
        if (!GrpcState) {
            GrpcState = new TGrpcState;
        }

        if (AtomicGetAndIncrement(GrpcState->Counter) == 0) {
            grpc_init();
        }
    }
}

TGrpcInitializer::~TGrpcInitializer()
{
    with_lock (GrpcStateMutex) {
        Y_ABORT_UNLESS(GrpcState);

        if (AtomicDecrement(GrpcState->Counter) == 0) {
            grpc_shutdown_blocking();

            delete GrpcState;
            GrpcState = nullptr;
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

void GrpcLoggerInit(const TLog& log, bool enableTracing)
{
    with_lock (GrpcStateMutex) {
        Y_ABORT_UNLESS(GrpcState);

        GrpcState->Log = log;
    }

    gpr_set_log_verbosity(LogPriorityToSeverity(log.FiltrationLevel()));
    gpr_set_log_function(AddLog);

    if (enableTracing) {
        EnableGRpcTracing();
    }
}

}   // namespace NCloud
