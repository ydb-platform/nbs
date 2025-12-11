#include "threadpool.h"

#include <contrib/libs/grpc/include/grpc/support/cpu.h>
#include <contrib/libs/grpc/src/core/lib/event_engine/thread_pool.h>
#include <contrib/libs/grpc/src/core/lib/gpr/useful.h>

namespace NCloud {

////////////////////////////////////////////////////////////////////////////////

size_t SetDefaultThreadPoolLimit(size_t count)
{
    return grpc_event_engine::experimental::ThreadPool::SetThreadsLimit(count);
}

}   // namespace NCloud
