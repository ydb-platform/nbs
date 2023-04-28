#include "threadpool.h"

#include <contrib/libs/grpc/include/grpc/support/cpu.h>
#include <contrib/libs/grpc/src/core/lib/gpr/useful.h>
#include <contrib/libs/grpc/src/cpp/server/dynamic_thread_pool.h>
#include <contrib/libs/grpc/src/cpp/server/thread_pool_interface.h>

namespace NCloud {

namespace {

////////////////////////////////////////////////////////////////////////////////

size_t ThreadsLimit = 0;

grpc::ThreadPoolInterface* CreateDefaultThreadPoolImpl()
{
    size_t threadsCount = std::max(4u, gpr_cpu_num_cores());
    if (ThreadsLimit) {
        threadsCount = std::min(threadsCount, ThreadsLimit);
    }
    return new grpc::DynamicThreadPool(threadsCount);
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

size_t SetDefaultThreadPoolLimit(size_t count)
{
    grpc::SetCreateThreadPool(CreateDefaultThreadPoolImpl);

    size_t prev = ThreadsLimit;
    ThreadsLimit = count;
    return prev;
}

}   // namespace NCloud
