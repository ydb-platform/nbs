#include "relaxed_counters.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NFileStore::NFuse::NWriteBackCache {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TRelaxedCountersTest)
{
    Y_UNIT_TEST(RelaxedCounter)
    {
        TRelaxedCounter counter;

        UNIT_ASSERT_VALUES_EQUAL(0, counter.Get());

        counter.Inc();
        UNIT_ASSERT_VALUES_EQUAL(1, counter.Get());

        counter.Add(4);
        UNIT_ASSERT_VALUES_EQUAL(5, counter.Get());

        counter.Dec();
        UNIT_ASSERT_VALUES_EQUAL(4, counter.Get());

        counter.Set(10);
        UNIT_ASSERT_VALUES_EQUAL(10, counter.Get());
    }

    Y_UNIT_TEST(RelaxedMaxCounter)
    {
        // 3 buckets for max calculator
        TRelaxedMaxCounter<3> maxCounter;

        maxCounter.Put(5);
        UNIT_ASSERT_VALUES_EQUAL(5, maxCounter.Get());

        maxCounter.Put(3);
        UNIT_ASSERT_VALUES_EQUAL(5, maxCounter.Get());

        maxCounter.Put(7);
        UNIT_ASSERT_VALUES_EQUAL(7, maxCounter.Get());

        maxCounter.Update();
        UNIT_ASSERT_VALUES_EQUAL(7, maxCounter.Get());

        maxCounter.Put(6);
        maxCounter.Update();
        UNIT_ASSERT_VALUES_EQUAL(7, maxCounter.Get());

        maxCounter.Put(4);
        maxCounter.Update();
        UNIT_ASSERT_VALUES_EQUAL(7, maxCounter.Get());

        maxCounter.Put(3);
        maxCounter.Update();
        UNIT_ASSERT_VALUES_EQUAL(6, maxCounter.Get());

        maxCounter.Update();
        UNIT_ASSERT_VALUES_EQUAL(4, maxCounter.Get());

        maxCounter.Update();
        UNIT_ASSERT_VALUES_EQUAL(3, maxCounter.Get());

        maxCounter.Update();
        UNIT_ASSERT_VALUES_EQUAL(0, maxCounter.Get());
    }

    Y_UNIT_TEST(RelaxedCombinedMaxCounter)
    {
        TRelaxedCombinedMaxCounter<3> combinedCounter;

        UNIT_ASSERT_VALUES_EQUAL(0, combinedCounter.GetCurrent());
        UNIT_ASSERT_VALUES_EQUAL(0, combinedCounter.GetMax());

        combinedCounter.Set(5);
        UNIT_ASSERT_VALUES_EQUAL(5, combinedCounter.GetCurrent());
        UNIT_ASSERT_VALUES_EQUAL(5, combinedCounter.GetMax());

        combinedCounter.Inc();
        UNIT_ASSERT_VALUES_EQUAL(6, combinedCounter.GetCurrent());
        UNIT_ASSERT_VALUES_EQUAL(6, combinedCounter.GetMax());

        combinedCounter.Dec();
        UNIT_ASSERT_VALUES_EQUAL(5, combinedCounter.GetCurrent());
        UNIT_ASSERT_VALUES_EQUAL(6, combinedCounter.GetMax());

        // Buckets: [<6>, 0, 0] -> [6, <5>, 0]
        combinedCounter.Update();
        UNIT_ASSERT_VALUES_EQUAL(5, combinedCounter.GetCurrent());
        UNIT_ASSERT_VALUES_EQUAL(6, combinedCounter.GetMax());

        combinedCounter.Set(3);
        UNIT_ASSERT_VALUES_EQUAL(3, combinedCounter.GetCurrent());
        UNIT_ASSERT_VALUES_EQUAL(6, combinedCounter.GetMax());

        // Buckets: [6, <5>, 0] -> [6, 5, <3>]
        combinedCounter.Update();
        UNIT_ASSERT_VALUES_EQUAL(3, combinedCounter.GetCurrent());
        UNIT_ASSERT_VALUES_EQUAL(6, combinedCounter.GetMax());

        // Buckets: [6, 5, <3>] -> [<3>, 5, 3]
        combinedCounter.Update();
        UNIT_ASSERT_VALUES_EQUAL(3, combinedCounter.GetCurrent());
        UNIT_ASSERT_VALUES_EQUAL(6, combinedCounter.GetMax());

        // Buckets: [<3>, 5, 3] -> [3, <3>, 3]
        combinedCounter.Update();
        UNIT_ASSERT_VALUES_EQUAL(3, combinedCounter.GetCurrent());
        UNIT_ASSERT_VALUES_EQUAL(5, combinedCounter.GetMax());

        combinedCounter.Update();
        UNIT_ASSERT_VALUES_EQUAL(3, combinedCounter.GetCurrent());
        UNIT_ASSERT_VALUES_EQUAL(3, combinedCounter.GetMax());
    }

    Y_UNIT_TEST(TRelaxedEventCounter)
    {
        TRelaxedEventCounter<2> eventCounter;

        UNIT_ASSERT_VALUES_EQUAL(0, eventCounter.GetActiveCount());
        UNIT_ASSERT_VALUES_EQUAL(0, eventCounter.GetActiveMaxCount());
        UNIT_ASSERT_VALUES_EQUAL(0, eventCounter.GetCompletedCount());

        eventCounter.Started();
        UNIT_ASSERT_VALUES_EQUAL(1, eventCounter.GetActiveCount());
        UNIT_ASSERT_VALUES_EQUAL(1, eventCounter.GetActiveMaxCount());
        UNIT_ASSERT_VALUES_EQUAL(0, eventCounter.GetCompletedCount());

        eventCounter.Started();
        UNIT_ASSERT_VALUES_EQUAL(2, eventCounter.GetActiveCount());
        UNIT_ASSERT_VALUES_EQUAL(2, eventCounter.GetActiveMaxCount());
        UNIT_ASSERT_VALUES_EQUAL(0, eventCounter.GetCompletedCount());

        eventCounter.Completed();
        UNIT_ASSERT_VALUES_EQUAL(1, eventCounter.GetActiveCount());
        UNIT_ASSERT_VALUES_EQUAL(2, eventCounter.GetActiveMaxCount());
        UNIT_ASSERT_VALUES_EQUAL(1, eventCounter.GetCompletedCount());

        eventCounter.Update();
        UNIT_ASSERT_VALUES_EQUAL(1, eventCounter.GetActiveCount());
        UNIT_ASSERT_VALUES_EQUAL(2, eventCounter.GetActiveMaxCount());
        UNIT_ASSERT_VALUES_EQUAL(1, eventCounter.GetCompletedCount());

        eventCounter.Completed();
        UNIT_ASSERT_VALUES_EQUAL(0, eventCounter.GetActiveCount());
        UNIT_ASSERT_VALUES_EQUAL(2, eventCounter.GetActiveMaxCount());
        UNIT_ASSERT_VALUES_EQUAL(2, eventCounter.GetCompletedCount());

        eventCounter.Update();
        UNIT_ASSERT_VALUES_EQUAL(0, eventCounter.GetActiveCount());
        UNIT_ASSERT_VALUES_EQUAL(2, eventCounter.GetActiveMaxCount());
        UNIT_ASSERT_VALUES_EQUAL(2, eventCounter.GetCompletedCount());

        eventCounter.Update();
        UNIT_ASSERT_VALUES_EQUAL(0, eventCounter.GetActiveCount());
        UNIT_ASSERT_VALUES_EQUAL(1, eventCounter.GetActiveMaxCount());
        UNIT_ASSERT_VALUES_EQUAL(2, eventCounter.GetCompletedCount());

        eventCounter.Started();
        eventCounter.Started();
        eventCounter.Reset();
        UNIT_ASSERT_VALUES_EQUAL(0, eventCounter.GetActiveCount());
        UNIT_ASSERT_VALUES_EQUAL(2, eventCounter.GetActiveMaxCount());
        UNIT_ASSERT_VALUES_EQUAL(2, eventCounter.GetCompletedCount());
    }

    Y_UNIT_TEST(TRelaxedEventCounterWithTimeStats)
    {
        TRelaxedEventCounterWithTimeStats<2> eventCounter;

        UNIT_ASSERT_VALUES_EQUAL(0, eventCounter.GetActiveCount());
        UNIT_ASSERT_VALUES_EQUAL(0, eventCounter.GetActiveMaxCount());
        UNIT_ASSERT_VALUES_EQUAL(0, eventCounter.GetCompletedCount());
        UNIT_ASSERT_VALUES_EQUAL(0, eventCounter.GetCompletedTime());
        UNIT_ASSERT_VALUES_EQUAL(0, eventCounter.GetMaxTime());

        eventCounter.Started();
        UNIT_ASSERT_VALUES_EQUAL(1, eventCounter.GetActiveCount());
        UNIT_ASSERT_VALUES_EQUAL(1, eventCounter.GetActiveMaxCount());
        UNIT_ASSERT_VALUES_EQUAL(0, eventCounter.GetCompletedCount());
        UNIT_ASSERT_VALUES_EQUAL(0, eventCounter.GetCompletedTime());
        UNIT_ASSERT_VALUES_EQUAL(0, eventCounter.GetMaxTime());

        eventCounter.Started();
        UNIT_ASSERT_VALUES_EQUAL(2, eventCounter.GetActiveCount());
        UNIT_ASSERT_VALUES_EQUAL(2, eventCounter.GetActiveMaxCount());
        UNIT_ASSERT_VALUES_EQUAL(0, eventCounter.GetCompletedCount());
        UNIT_ASSERT_VALUES_EQUAL(0, eventCounter.GetCompletedTime());
        UNIT_ASSERT_VALUES_EQUAL(0, eventCounter.GetMaxTime());

        eventCounter.Completed(TDuration::MicroSeconds(10));
        UNIT_ASSERT_VALUES_EQUAL(1, eventCounter.GetActiveCount());
        UNIT_ASSERT_VALUES_EQUAL(2, eventCounter.GetActiveMaxCount());
        UNIT_ASSERT_VALUES_EQUAL(1, eventCounter.GetCompletedCount());
        UNIT_ASSERT_VALUES_EQUAL(10, eventCounter.GetCompletedTime());
        UNIT_ASSERT_VALUES_EQUAL(10, eventCounter.GetMaxTime());

        eventCounter.Update(TDuration::MicroSeconds(15));
        UNIT_ASSERT_VALUES_EQUAL(1, eventCounter.GetActiveCount());
        UNIT_ASSERT_VALUES_EQUAL(2, eventCounter.GetActiveMaxCount());
        UNIT_ASSERT_VALUES_EQUAL(1, eventCounter.GetCompletedCount());
        UNIT_ASSERT_VALUES_EQUAL(10, eventCounter.GetCompletedTime());
        UNIT_ASSERT_VALUES_EQUAL(15, eventCounter.GetMaxTime());

        eventCounter.Completed(TDuration::MicroSeconds(20));
        UNIT_ASSERT_VALUES_EQUAL(0, eventCounter.GetActiveCount());
        UNIT_ASSERT_VALUES_EQUAL(2, eventCounter.GetActiveMaxCount());
        UNIT_ASSERT_VALUES_EQUAL(2, eventCounter.GetCompletedCount());
        UNIT_ASSERT_VALUES_EQUAL(30, eventCounter.GetCompletedTime());
        UNIT_ASSERT_VALUES_EQUAL(20, eventCounter.GetMaxTime());

        eventCounter.Update(TDuration::MicroSeconds(15));
        UNIT_ASSERT_VALUES_EQUAL(0, eventCounter.GetActiveCount());
        UNIT_ASSERT_VALUES_EQUAL(2, eventCounter.GetActiveMaxCount());
        UNIT_ASSERT_VALUES_EQUAL(2, eventCounter.GetCompletedCount());
        UNIT_ASSERT_VALUES_EQUAL(30, eventCounter.GetCompletedTime());
        UNIT_ASSERT_VALUES_EQUAL(20, eventCounter.GetMaxTime());

        eventCounter.Update(TDuration::Zero());
        UNIT_ASSERT_VALUES_EQUAL(0, eventCounter.GetActiveCount());
        UNIT_ASSERT_VALUES_EQUAL(1, eventCounter.GetActiveMaxCount());
        UNIT_ASSERT_VALUES_EQUAL(2, eventCounter.GetCompletedCount());
        UNIT_ASSERT_VALUES_EQUAL(30, eventCounter.GetCompletedTime());
        UNIT_ASSERT_VALUES_EQUAL(20, eventCounter.GetMaxTime());

        eventCounter.Started();
        eventCounter.Started();
        eventCounter.Reset();
        UNIT_ASSERT_VALUES_EQUAL(0, eventCounter.GetActiveCount());
        UNIT_ASSERT_VALUES_EQUAL(2, eventCounter.GetActiveMaxCount());
        UNIT_ASSERT_VALUES_EQUAL(2, eventCounter.GetCompletedCount());
        UNIT_ASSERT_VALUES_EQUAL(30, eventCounter.GetCompletedTime());
        UNIT_ASSERT_VALUES_EQUAL(20, eventCounter.GetMaxTime());
    }
}

}   // namespace NCloud::NFileStore::NFuse::NWriteBackCache
