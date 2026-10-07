#include <cloud/blockstore/libs/storage/partition/model/small_blobs_removal_state.h>

#include <library/cpp/testing/unittest/registar.h>

#include <array>

namespace NCloud::NBlockStore::NStorage::NPartition {

Y_UNIT_TEST_SUITE(TSmallBlobsRemovalStateTest)
{
    Y_UNIT_TEST(ShouldWaitForEveryBackgroundOperation)
    {
        const auto start = TInstant::Seconds(100);
        TSmallBlobsRemovalState state(true, start + TDuration::Minutes(5));
        std::array<EOperationStatus, 6> operations;
        operations.fill(EOperationStatus::Idle);
        const auto ready = [&]
        {
            return state.IsReady(start,
                                 {operations[0], operations[1], operations[2],
                                  operations[3], operations[4], operations[5]});
        };
        UNIT_ASSERT(ready());
        for (auto& operation: operations) {
            operation = EOperationStatus::Enqueued;
            UNIT_ASSERT(!ready());
            operation = EOperationStatus::Started;
            UNIT_ASSERT(!ready());
            operation = EOperationStatus::Idle;
        }
        UNIT_ASSERT(ready());
    }

    Y_UNIT_TEST(ShouldStopAtTimeoutAndAlwaysReportReady)
    {
        const auto start = TInstant::Seconds(100);
        const auto deadline = start + TDuration::Minutes(5);
        TSmallBlobsRemovalState state(true, deadline);
        UNIT_ASSERT(state.IsActive(deadline - TDuration::MicroSeconds(1)));
        UNIT_ASSERT(!state.IsReady(deadline - TDuration::MicroSeconds(1),
                                   {EOperationStatus::Started}));
        UNIT_ASSERT(!state.IsActive(deadline));
        UNIT_ASSERT(state.IsStopped(deadline));
        UNIT_ASSERT(state.IsReady(deadline, {EOperationStatus::Started}));
        UNIT_ASSERT(state.IsReady(deadline + TDuration::Minutes(1),
                                  {EOperationStatus::Enqueued}));
    }

    Y_UNIT_TEST(ShouldPreserveDeadlineAcrossBootAttempts)
    {
        const auto start = TInstant::Seconds(100);
        const auto timeout = TDuration::Minutes(5);
        TSmallBlobsRemovalState state(true, start + timeout);
        state.Activate(start + TDuration::Minutes(4), timeout);
        UNIT_ASSERT(state.IsStopped(start + timeout));
    }

    Y_UNIT_TEST(ShouldResumeNormalSchedulingWhenDisabled)
    {
        const auto start = TInstant::Seconds(100);
        TSmallBlobsRemovalState state(true, start + TDuration::Minutes(5));
        // This snapshot belongs to an operation that started in the mode.
        const bool operationStartedInMode = state.IsActive(start);
        state.Finish();
        UNIT_ASSERT(state.IsEnabled());
        UNIT_ASSERT(state.IsStopped(start));
        state.Disable();
        UNIT_ASSERT(!state.IsEnabled());
        UNIT_ASSERT(!state.IsActive(start));
        UNIT_ASSERT(!state.IsStopped(start + TDuration::Hours(1)));
        UNIT_ASSERT(!state.IsReady(start, {EOperationStatus::Started}));
        UNIT_ASSERT(operationStartedInMode);
    }

    Y_UNIT_TEST(ShouldResetTimeoutForNewCycle)
    {
        const auto start = TInstant::Seconds(100);
        const auto timeout = TDuration::Minutes(5);
        TSmallBlobsRemovalState state(true, {});
        state.Activate(start, timeout);
        UNIT_ASSERT(state.IsStopped(start + timeout));
        state = TSmallBlobsRemovalState(true, {});
        state.Activate(start + timeout, timeout);
        UNIT_ASSERT(state.IsActive(start + timeout));
        UNIT_ASSERT(state.IsStopped(start + timeout + timeout));
    }

    Y_UNIT_TEST(ShouldKeepModeDisabledByDefault)
    {
        TSmallBlobsRemovalState state;
        const auto now = TInstant::Seconds(100);
        state.Activate(now, TDuration::Minutes(5));
        UNIT_ASSERT(!state.IsActive(now));
        UNIT_ASSERT(!state.IsStopped(now + TDuration::Hours(1)));
    }
}

}   // namespace NCloud::NBlockStore::NStorage::NPartition
