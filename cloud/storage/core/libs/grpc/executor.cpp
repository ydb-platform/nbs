#include "executor.h"

#include <contrib/libs/grpc/src/core/lib/iomgr/executor.h>

namespace NCloud {

////////////////////////////////////////////////////////////////////////////////

size_t SetExecutorThreadsLimit(size_t count)
{
    return grpc_core::Executor::SetThreadsLimit(count);
}

}   // namespace NCloud
