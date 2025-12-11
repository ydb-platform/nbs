#pragma once

#include <cloud/storage/core/libs/diagnostics/logging.h>

namespace NCloud::NBlockStore::NRdma {

////////////////////////////////////////////////////////////////////////////////

#define RDMA_LOG_S(priority, log, stream)                     \
    if (log.IsOpen() && priority <= log.FiltrationLevel()) {  \
        log << static_cast<ELogPriority>(priority) << stream; \
    }

#define RDMA_LOG_T(priority, throttler, ...) \
    if ((throttler).Kick()) {                \
        RDMA_LOG_S(priority, __VA_ARGS__);   \
    }

////////////////////////////////////////////////////////////////////////////////

#define RDMA_LOG2(priority, stream) RDMA_LOG_S(priority, Log, stream)
#define RDMA_LOG3 RDMA_LOG_S
#define RDMA_LOG4 RDMA_LOG_T

#define RDMA_LOG(...) Y_VA_MACRO(RDMA_LOG, __VA_ARGS__)

#define RDMA_ERROR(...) RDMA_LOG(TLOG_ERR, __VA_ARGS__)
#define RDMA_WARN(...) RDMA_LOG(TLOG_WARNING, __VA_ARGS__)
#define RDMA_INFO(...) RDMA_LOG(TLOG_INFO, __VA_ARGS__)
#define RDMA_DEBUG(...) RDMA_LOG(TLOG_DEBUG, __VA_ARGS__)
#define RDMA_TRACE(...) RDMA_LOG(TLOG_RESOURCES, __VA_ARGS__)

}   // namespace NCloud::NBlockStore::NRdma
