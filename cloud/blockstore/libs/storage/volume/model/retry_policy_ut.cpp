#include "retry_policy.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

constexpr TDuration TIME_INCREMENT = TDuration::Seconds(1);
constexpr TDuration TIME_MAX = TDuration::Seconds(5);

#define CHECK_TIMEOUT_AND_DEADLINE(timeout, deadline, policy)        \
    UNIT_ASSERT_VALUES_EQUAL(timeout, policy.GetCurrentTimeout());   \
    UNIT_ASSERT_VALUES_EQUAL(deadline, policy.GetCurrentDeadline()); \
    // CHECK_TIMEOUT_AND_DEADLINE

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TRetryPolicy)
{
    Y_UNIT_TEST(ShouldCorrectlyInitialize)
    {
        TRetryPolicy retryPolicy(TIME_INCREMENT, TIME_MAX);
        CHECK_TIMEOUT_AND_DEADLINE(
            TDuration::Zero(),
            TInstant::Zero(),
            retryPolicy);
    }

    Y_UNIT_TEST(ShouldCorrectlyUpdate)
    {
        TRetryPolicy retryPolicy(TIME_INCREMENT, TIME_MAX);

        auto now = TInstant::Zero();
        for (auto timeout = TDuration::Zero(); timeout < TIME_MAX;
             timeout += TIME_INCREMENT)
        {
            CHECK_TIMEOUT_AND_DEADLINE(timeout, now, retryPolicy);

            retryPolicy.Update(now);
            now += timeout + TIME_INCREMENT;

            CHECK_TIMEOUT_AND_DEADLINE(
                timeout + TIME_INCREMENT,
                now,
                retryPolicy);
        }

        CHECK_TIMEOUT_AND_DEADLINE(TIME_MAX, now, retryPolicy);

        retryPolicy.Update(now + 2 * TIME_MAX);

        CHECK_TIMEOUT_AND_DEADLINE(TIME_MAX, now + 3 * TIME_MAX, retryPolicy);
    }

    Y_UNIT_TEST(ShouldCorrectlyUpdateDeadline)
    {
        TRetryPolicy retryPolicy(TIME_INCREMENT, TIME_MAX);

        auto now = TInstant::Now();
        retryPolicy.Update(now);

        CHECK_TIMEOUT_AND_DEADLINE(
            TIME_INCREMENT,
            now + TIME_INCREMENT,
            retryPolicy);

        retryPolicy.Update(now + TIME_INCREMENT / 2);

        // now < deadline => newDeadline = dealine + currentTimeout.
        CHECK_TIMEOUT_AND_DEADLINE(
            2 * TIME_INCREMENT,
            now + 3 * TIME_INCREMENT,
            retryPolicy);

        retryPolicy.Update(now + 10 * TIME_INCREMENT);

        // now > deadline => newDeadline = now + currentTimeout.
        CHECK_TIMEOUT_AND_DEADLINE(
            3 * TIME_INCREMENT,
            now + 13 * TIME_INCREMENT,
            retryPolicy);
    }
}

#undef CHECK_TIMEOUT_AND_DEADLINE

}   // namespace NCloud::NBlockStore::NStorage
