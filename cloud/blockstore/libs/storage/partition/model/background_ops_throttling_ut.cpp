#include "background_ops_throttling.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NBlockStore::NStorage::NPartition {

namespace {

////////////////////////////////////////////////////////////////////////////////

void CheckBudgetInvariant(
    TDuration lastOperationExecTime,
    TDuration delay,
    TDuration maxExecTimePerSecond)
{
    const double execRatio = static_cast<double>(lastOperationExecTime.GetValue())
        / (lastOperationExecTime.GetValue() + delay.GetValue());
    const double budgetRatio = static_cast<double>(maxExecTimePerSecond.GetValue())
        / TDuration::Seconds(1).GetValue();

    static constexpr double Epsilon = 1e-6;
    UNIT_ASSERT_DOUBLES_EQUAL(budgetRatio, execRatio, Epsilon);
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TBackgroundOpsThrottlingTest)
{
    Y_UNIT_TEST(ShouldNotDelayWhenMaxDelayIsZero)
    {
        const auto delay = CalculateBackgroundOpThrottleDelay(
            TDuration::MilliSeconds(50),
            TDuration::MilliSeconds(100),
            TDuration::MilliSeconds(10),
            TDuration::Zero());

        UNIT_ASSERT(!delay);
    }

    Y_UNIT_TEST(ShouldNotDelayWhenMaxExecTimePerSecondIsZero)
    {
        const auto minDelay = TDuration::MilliSeconds(10);
        const auto delay = CalculateBackgroundOpThrottleDelay(
            TDuration::MilliSeconds(100),
            TDuration::Zero(),
            minDelay,
            TDuration::Seconds(1));

        UNIT_ASSERT_VALUES_EQUAL(minDelay, delay);
    }

    Y_UNIT_TEST(ShouldNotDelayWhenMaxExecTimeForLastSecondIsZero)
    {
        const auto minDelay = TDuration::MilliSeconds(10);
        const auto delay = CalculateBackgroundOpThrottleDelay(
            TDuration::Zero(),
            TDuration::MilliSeconds(100),
            minDelay,
            TDuration::Seconds(1));

        UNIT_ASSERT_VALUES_EQUAL(minDelay, delay);
    }

    Y_UNIT_TEST(ShouldNotDelayWhenMaxExecTimePerSecondIsSecondOrGreater)
    {
        {
            const auto execTime = TDuration::MilliSeconds(500);
            const auto maxExec = TDuration::MilliSeconds(1000);
            const auto delay = CalculateBackgroundOpThrottleDelay(
                execTime,
                maxExec,
                TDuration::Zero(),
                TDuration::Seconds(1));

            UNIT_ASSERT_VALUES_EQUAL(TDuration::Zero(), delay);
        }

        {
            const auto execTime = TDuration::MilliSeconds(500);
            const auto maxExec = TDuration::MilliSeconds(1500);
            const auto delay = CalculateBackgroundOpThrottleDelay(
                execTime,
                maxExec,
                TDuration::Zero(),
                TDuration::Seconds(1));

            UNIT_ASSERT_VALUES_EQUAL(TDuration::Zero(), delay);
        }
    }

    Y_UNIT_TEST(ShouldScaleDelayCorrectly)
    {
        {
            const auto execTime = TDuration::MilliSeconds(25);
            const auto maxExec = TDuration::MilliSeconds(50);
            const auto delay = CalculateBackgroundOpThrottleDelay(
                execTime,
                maxExec,
                TDuration::Zero(),
                TDuration::Hours(1));

            UNIT_ASSERT_VALUES_EQUAL(TDuration::MilliSeconds(475), delay);
            CheckBudgetInvariant(execTime, delay, maxExec);
        }

        {
            const auto execTime = TDuration::MilliSeconds(100);
            const auto maxExec = TDuration::MilliSeconds(50);
            const auto delay = CalculateBackgroundOpThrottleDelay(
                execTime,
                maxExec,
                TDuration::Zero(),
                TDuration::Hours(1));

            UNIT_ASSERT_VALUES_EQUAL(TDuration::MilliSeconds(1900), delay);
            CheckBudgetInvariant(execTime, delay, maxExec);
        }

        {
            const auto execTime = TDuration::MilliSeconds(50);
            const auto maxExec = TDuration::MilliSeconds(50);
            const auto delay = CalculateBackgroundOpThrottleDelay(
                execTime,
                maxExec,
                TDuration::Zero(),
                TDuration::Hours(1));

            UNIT_ASSERT_VALUES_EQUAL(TDuration::MilliSeconds(950), delay);
            CheckBudgetInvariant(execTime, delay, maxExec);
        }

        {
            const auto execTime = TDuration::MilliSeconds(25);
            const auto maxExec = TDuration::MilliSeconds(100);
            const auto delay = CalculateBackgroundOpThrottleDelay(
                execTime,
                maxExec,
                TDuration::Zero(),
                TDuration::Hours(1));

            UNIT_ASSERT_VALUES_EQUAL(TDuration::MilliSeconds(225), delay);
            CheckBudgetInvariant(execTime, delay, maxExec);
        }
    }

    Y_UNIT_TEST(ShouldScaleDelayCorrectlyWhenExecTimeIsSecondOrGreater)
    {
        {
            const auto execTime = TDuration::MilliSeconds(1000);
            const auto maxExec = TDuration::MilliSeconds(400);
            const auto delay = CalculateBackgroundOpThrottleDelay(
                execTime,
                maxExec,
                TDuration::Zero(),
                TDuration::Hours(1));

            UNIT_ASSERT_VALUES_EQUAL(TDuration::MilliSeconds(1500), delay);
            CheckBudgetInvariant(execTime, delay, maxExec);
        }

        {
            const auto execTime = TDuration::MilliSeconds(1500);
            const auto maxExec = TDuration::MilliSeconds(400);
            const auto delay = CalculateBackgroundOpThrottleDelay(
                execTime,
                maxExec,
                TDuration::Zero(),
                TDuration::Hours(1));

            UNIT_ASSERT_VALUES_EQUAL(TDuration::MilliSeconds(2250), delay);
            CheckBudgetInvariant(execTime, delay, maxExec);
        }

        {
            const auto execTime = TDuration::MilliSeconds(1500);
            const auto maxExec = TDuration::MilliSeconds(1500);
            const auto delay = CalculateBackgroundOpThrottleDelay(
                execTime,
                maxExec,
                TDuration::Zero(),
                TDuration::Hours(1));

            UNIT_ASSERT_VALUES_EQUAL(TDuration::MilliSeconds(0), delay);
        }
    }

    Y_UNIT_TEST(ShouldBoundDelayByMinDelay)
    {
        const auto execTime = TDuration::MilliSeconds(10);
        const auto maxExec = TDuration::MilliSeconds(50);
        const auto minDelay = TDuration::MilliSeconds(300);

        const auto delay = CalculateBackgroundOpThrottleDelay(
            execTime,
            maxExec,
            minDelay,
            TDuration::Hours(1));
        const auto delayWithoutMinDelay = CalculateBackgroundOpThrottleDelay(
            execTime,
            maxExec,
            TDuration::Zero(),
            TDuration::Hours(1));

        UNIT_ASSERT_VALUES_EQUAL(minDelay, delay);
        UNIT_ASSERT(delay > delayWithoutMinDelay);
    }

    Y_UNIT_TEST(ShouldBoundDelayByMaxDelay)
    {
        const auto execTime = TDuration::MilliSeconds(50);
        const auto maxExec = TDuration::MilliSeconds(100);
        const auto maxDelay = TDuration::MilliSeconds(300);

        const auto delay = CalculateBackgroundOpThrottleDelay(
            execTime,
            maxExec,
            TDuration::Zero(),
            maxDelay);
        const auto delayWithoutMaxDelay = CalculateBackgroundOpThrottleDelay(
            execTime,
            maxExec,
            TDuration::Zero(),
            TDuration::Hours(1));

        UNIT_ASSERT_VALUES_EQUAL(maxDelay, delay);
        UNIT_ASSERT(delay < delayWithoutMaxDelay);
    }
}

}   // namespace NCloud::NBlockStore::NStorage::NPartition
