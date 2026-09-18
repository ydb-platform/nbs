#include "thread_pool.h"

#include "task_queue.h"

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/scope.h>

#include <latch>
#include <thread>

namespace NCloud {

////////////////////////////////////////////////////////////////////////////////

static constexpr auto WaitTimeout = TDuration::Seconds(5);

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TThreadPoolTest)
{
    Y_UNIT_TEST(ShouldMeasureQueuedTasks)
    {
        auto counters = MakeIntrusive<NMonitoring::TDynamicCounters>();
        auto pool = CreateThreadPool("IO.SQ", 1, counters);
        auto group = counters->GetSubgroup("thread", "IO.SQ");
        auto pending = group->GetCounter("PendingTasks");

        // Before Start, both tasks must remain queued.
        auto first = pool->Execute([] { return 1; });
        auto second = pool->Execute([] { return 2; });
        UNIT_ASSERT_VALUES_EQUAL(pending->Val(), 2);
        UNIT_ASSERT_VALUES_EQUAL(group->GetCounter("QueueCount")->Val(), 0);

        pool->Start();
        UNIT_ASSERT_VALUES_EQUAL(first.GetValue(WaitTimeout), 1);
        UNIT_ASSERT_VALUES_EQUAL(second.GetValue(WaitTimeout), 2);
        pool->Stop();

        UNIT_ASSERT_VALUES_EQUAL(pending->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(group->GetCounter("QueueCount")->Val(), 2);
        UNIT_ASSERT_VALUES_EQUAL(group->GetCounter("ExecutionCount")->Val(), 2);
        auto snapshot = group->FindHistogram("QueueLatencyUs")->Snapshot();
        ui64 samples = 0;
        for (ui32 i = 0; i < snapshot->Count(); ++i) {
            samples += snapshot->Value(i);
        }
        UNIT_ASSERT_VALUES_EQUAL(samples, 2);
    }

    Y_UNIT_TEST(ShouldReleasePendingCountersOnDestruction)
    {
        auto counters = MakeIntrusive<NMonitoring::TDynamicCounters>();
        auto pending = counters->GetSubgroup("thread", "IO.SQ")
                           ->GetCounter("PendingTasks");
        {
            auto pool = CreateThreadPool("IO.SQ", 1, counters);
            pool->ExecuteSimple([] {});
            UNIT_ASSERT_VALUES_EQUAL(pending->Val(), 1);
        }
        UNIT_ASSERT_VALUES_EQUAL(pending->Val(), 0);
    }

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

        std::latch enqueued{1};

        std::thread thread(
            [&]() mutable
            {
                enqueued.count_down();
                auto future = threadPool->Execute([] { return 42; });

                UNIT_ASSERT_EQUAL(future.GetValue(WaitTimeout), 42);
            });

        enqueued.wait();

        // Sleep to be sure that the thread will call the AllocateWorker
        // function before the thread pool starts.
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
