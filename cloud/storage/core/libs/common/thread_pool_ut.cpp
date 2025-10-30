#include "thread_pool.h"

#include "task_queue.h"

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/scope.h>

#include <thread>

namespace NCloud {

////////////////////////////////////////////////////////////////////////////////

static constexpr auto WaitTimeout = TDuration::Seconds(5);

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TThreadPoolTest)
{
    Y_UNIT_TEST(ShouldExecuteTask)
    {
        auto threadPool = CreateThreadPool("thread", 1);
        threadPool->Start();
        Y_DEFER {
            threadPool->Stop();
        };

        auto future = threadPool->Execute([] {
            return 42;
        });

        UNIT_ASSERT_EQUAL(future.GetValue(WaitTimeout), 42);
    }

    Y_UNIT_TEST(ShouldExecuteTaskEnqueuedBeforeStart)
    {
        auto threadPool = CreateThreadPool("thread", 1);

        auto promise = NThreading::NewPromise();

        auto future = promise.GetFuture();

        std::thread thread(
            [threadPool, promise = std::move(promise)]() mutable
            {
                promise.SetValue();
                auto future = threadPool->Execute([] { return 42; });

                UNIT_ASSERT_EQUAL(future.GetValue(WaitTimeout), 42);
            });

        future.GetValueSync();

        // Sleep to be sure that task will be enqueued before start.
        Sleep(TDuration::Seconds(1));

        threadPool->Start();
        Y_DEFER
        {
            threadPool->Stop();
        };

        thread.join();
    }
}

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TLongRunningTaskExecutorTest)
{
    Y_UNIT_TEST(ShouldExecuteTask)
    {
        auto executor = CreateLongRunningTaskExecutor("thread");

        auto future = executor->Execute([] {
            return 42;
        });

        UNIT_ASSERT_EQUAL(future.GetValue(WaitTimeout), 42);
    }
}

}   // namespace NCloud
