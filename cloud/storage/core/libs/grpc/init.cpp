#include "init.h"

#include <contrib/libs/grpc/include/grpc/grpc.h>
#include <contrib/libs/grpc/include/grpc/support/log.h>

#include <library/cpp/deprecated/atomic/atomic.h>
#include <library/cpp/logger/log.h>

#include <util/system/sanitizers.h>
#include <util/system/src_location.h>

#include <atomic>

namespace NCloud {

namespace {

////////////////////////////////////////////////////////////////////////////////
// We have to manage the lifetime of the GRPC custom logger to make sure it is
// alive as long as the GRPC is running.
TLog* GrpcLog;

////////////////////////////////////////////////////////////////////////////////

gpr_log_severity LogPriorityToSeverity(ELogPriority priority)
{
    if (priority <= TLOG_ERR) {
        return GPR_LOG_SEVERITY_ERROR;
    }

    if (priority <= TLOG_INFO) {
        return GPR_LOG_SEVERITY_INFO;
    }

    return GPR_LOG_SEVERITY_DEBUG;
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

    // TSAN does not understand |std::atomic_thread_fence|, so teach it
#if defined(_tsan_enabled_)
    __tsan_acquire(GrpcLog);
#endif
    *GrpcLog << LogSeverityToPriority(args->severity)
        << TSourceLocation(file.As<TStringBuf>(), args->line)
        << ": " << args->message;
}

void EnableGrpcTracing()
{
    const char* tracers[] = {
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

    for (const char* tracer: tracers) {
        grpc_tracer_set_enabled(tracer, 1);
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TGrpcInitializer::TGrpcInitializer()
{
    grpc_init();
}

TGrpcInitializer::~TGrpcInitializer()
{
    grpc_shutdown_blocking();

    if (!grpc_is_initialized()) {
        // Restore the default log function. Just in case
        gpr_set_log_function(nullptr);

        std::atomic_thread_fence(std::memory_order_seq_cst);
    // TSAN does not understand |std::atomic_thread_fence|, so teach it
#if defined(_tsan_enabled_)
    __tsan_release(GrpcLog);
#endif
        // Now we can safely destroy the custom logger
        delete GrpcLog;
        GrpcLog = nullptr;
    }
}

////////////////////////////////////////////////////////////////////////////////

void GrpcLoggerInit(const TLog& log, bool enableTracing)
{
    // Logger should only be initialized once
    Y_ABORT_UNLESS(!GrpcLog);

    GrpcLog = new TLog(log);
    // |gpr_set_log_verbosity| and |gpr_set_log_function| do not imply any
    // memory barrier, so we need a full barrier here
    std::atomic_thread_fence(std::memory_order_seq_cst);
    // TSAN does not understand |std::atomic_thread_fence|, so teach it
#if defined(_tsan_enabled_)
    __tsan_release(GrpcLog);
#endif

    gpr_set_log_verbosity(LogPriorityToSeverity(log.FiltrationLevel()));
    gpr_set_log_function(AddLog);

    if (enableTracing) {
        EnableGrpcTracing();
    }
}

}   // namespace NCloud
