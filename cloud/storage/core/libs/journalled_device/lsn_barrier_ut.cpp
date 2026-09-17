#include "lsn_barrier.h"

#include <library/cpp/testing/unittest/registar.h>

#include <atomic>
#include <thread>

namespace NCloud::NJournalled {

namespace {

////////////////////////////////////////////////////////////////////////////////

ui64 GetCurrentLsn(TLsnBarrier& barrier)
{
    return barrier.Acquire().GetLsn();
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TLsnBarrierTest)
{
    Y_UNIT_TEST(ShouldStartWithZeroLsn)
    {
        TLsnBarrier barrier;

        UNIT_ASSERT_VALUES_EQUAL(0, GetCurrentLsn(barrier));
        UNIT_ASSERT_VALUES_EQUAL(0, barrier.GetBarrierLsn());
    }

    Y_UNIT_TEST(ShouldAdvanceLsnMonotonically)
    {
        TLsnBarrier barrier;

        barrier.Advance(10);
        UNIT_ASSERT_VALUES_EQUAL(10, GetCurrentLsn(barrier));

        // stale lsns should be ignored
        barrier.Advance(5);
        UNIT_ASSERT_VALUES_EQUAL(10, GetCurrentLsn(barrier));

        barrier.Advance(10);
        UNIT_ASSERT_VALUES_EQUAL(10, GetCurrentLsn(barrier));

        barrier.Advance(20);
        UNIT_ASSERT_VALUES_EQUAL(20, GetCurrentLsn(barrier));
    }

    Y_UNIT_TEST(ShouldReportCurrentLsnWhenNothingAcquired)
    {
        TLsnBarrier barrier;

        barrier.Advance(10);
        UNIT_ASSERT_VALUES_EQUAL(10, barrier.GetBarrierLsn());

        barrier.Acquire().Release();

        barrier.Advance(20);
        UNIT_ASSERT_VALUES_EQUAL(20, barrier.GetBarrierLsn());
    }

    Y_UNIT_TEST(ShouldHoldTheBarrierWhileLsnAdvances)
    {
        TLsnBarrier barrier;

        barrier.Advance(10);

        auto guard = barrier.Acquire();
        UNIT_ASSERT_VALUES_EQUAL(10, guard.GetLsn());

        barrier.Advance(20);

        // the barrier holds back reclamation, the current lsn moves on
        UNIT_ASSERT_VALUES_EQUAL(20, GetCurrentLsn(barrier));
        UNIT_ASSERT_VALUES_EQUAL(10, barrier.GetBarrierLsn());

        guard.Release();
        UNIT_ASSERT_VALUES_EQUAL(20, barrier.GetBarrierLsn());
    }

    Y_UNIT_TEST(ShouldReportTheOldestOfSeveralBarriers)
    {
        TLsnBarrier barrier;

        barrier.Advance(10);
        auto first = barrier.Acquire();

        barrier.Advance(20);
        auto second = barrier.Acquire();

        barrier.Advance(30);
        auto third = barrier.Acquire();

        UNIT_ASSERT_VALUES_EQUAL(10, first.GetLsn());
        UNIT_ASSERT_VALUES_EQUAL(20, second.GetLsn());
        UNIT_ASSERT_VALUES_EQUAL(30, third.GetLsn());

        UNIT_ASSERT_VALUES_EQUAL(10, barrier.GetBarrierLsn());

        first.Release();
        UNIT_ASSERT_VALUES_EQUAL(20, barrier.GetBarrierLsn());

        second.Release();
        UNIT_ASSERT_VALUES_EQUAL(30, barrier.GetBarrierLsn());

        third.Release();
        UNIT_ASSERT_VALUES_EQUAL(30, barrier.GetBarrierLsn());
    }

    Y_UNIT_TEST(ShouldRefcountRepeatedBarriersAtTheSameLsn)
    {
        TLsnBarrier barrier;

        barrier.Advance(10);

        auto first = barrier.Acquire();
        auto second = barrier.Acquire();
        auto third = barrier.Acquire();

        UNIT_ASSERT_VALUES_EQUAL(10, first.GetLsn());
        UNIT_ASSERT_VALUES_EQUAL(10, second.GetLsn());
        UNIT_ASSERT_VALUES_EQUAL(10, third.GetLsn());

        barrier.Advance(20);

        first.Release();
        UNIT_ASSERT_VALUES_EQUAL(10, barrier.GetBarrierLsn());

        second.Release();
        UNIT_ASSERT_VALUES_EQUAL(10, barrier.GetBarrierLsn());

        // only the last release lets the barrier move
        third.Release();
        UNIT_ASSERT_VALUES_EQUAL(20, barrier.GetBarrierLsn());
    }

    Y_UNIT_TEST(ShouldAllowOutOfOrderRelease)
    {
        TLsnBarrier barrier;

        barrier.Advance(10);
        auto first = barrier.Acquire();

        barrier.Advance(20);
        auto second = barrier.Acquire();

        second.Release();
        UNIT_ASSERT_VALUES_EQUAL(10, barrier.GetBarrierLsn());

        first.Release();
        UNIT_ASSERT_VALUES_EQUAL(20, barrier.GetBarrierLsn());
    }

    Y_UNIT_TEST(ShouldReleaseTheBarrierOnGuardDestruction)
    {
        TLsnBarrier barrier;

        barrier.Advance(10);

        {
            const auto guard = barrier.Acquire();

            barrier.Advance(20);
            UNIT_ASSERT_VALUES_EQUAL(10, barrier.GetBarrierLsn());
        }

        UNIT_ASSERT_VALUES_EQUAL(20, barrier.GetBarrierLsn());
    }

    Y_UNIT_TEST(ShouldMoveTheGuardWithoutReleasingTheBarrier)
    {
        TLsnBarrier barrier;

        barrier.Advance(10);

        {
            auto guard = barrier.Acquire();
            auto movedGuard = std::move(guard);

            barrier.Advance(20);
            UNIT_ASSERT_VALUES_EQUAL(10, movedGuard.GetLsn());
            UNIT_ASSERT_VALUES_EQUAL(10, barrier.GetBarrierLsn());
        }

        UNIT_ASSERT_VALUES_EQUAL(20, barrier.GetBarrierLsn());
    }

    Y_UNIT_TEST(ShouldAcquireZeroLsnBeforeTheFirstAdvance)
    {
        TLsnBarrier barrier;

        auto guard = barrier.Acquire();
        UNIT_ASSERT_VALUES_EQUAL(0, guard.GetLsn());

        barrier.Advance(10);
        UNIT_ASSERT_VALUES_EQUAL(0, barrier.GetBarrierLsn());

        guard.Release();
        UNIT_ASSERT_VALUES_EQUAL(10, barrier.GetBarrierLsn());
    }

    Y_UNIT_TEST(ShouldAcquireAheadOfTheCurrentLsn)
    {
        TLsnBarrier barrier;

        barrier.Advance(10);

        auto guard = barrier.AcquireAtLeast(20);
        UNIT_ASSERT_VALUES_EQUAL(20, guard.GetLsn());

        // a barrier placed ahead of the current lsn holds nothing back yet
        UNIT_ASSERT_VALUES_EQUAL(10, barrier.GetBarrierLsn());

        barrier.Advance(20);
        UNIT_ASSERT_VALUES_EQUAL(20, barrier.GetBarrierLsn());

        // ... and starts holding once the current lsn has reached it
        barrier.Advance(30);
        UNIT_ASSERT_VALUES_EQUAL(20, barrier.GetBarrierLsn());

        guard.Release();
        UNIT_ASSERT_VALUES_EQUAL(30, barrier.GetBarrierLsn());
    }

    Y_UNIT_TEST(ShouldNotAcquireBehindTheCurrentLsn)
    {
        TLsnBarrier barrier;

        barrier.Advance(10);

        // a position that has already been passed cannot be acquired, the
        // caller learns that from the lsn the guard reports
        auto guard = barrier.AcquireAtLeast(5);
        UNIT_ASSERT_VALUES_EQUAL(10, guard.GetLsn());

        barrier.Advance(20);
        UNIT_ASSERT_VALUES_EQUAL(10, barrier.GetBarrierLsn());

        guard.Release();
        UNIT_ASSERT_VALUES_EQUAL(20, barrier.GetBarrierLsn());
    }

    Y_UNIT_TEST(ShouldNotReclaimPastTheBarrierUnderConcurrency)
    {
        // The contract readers rely on: while a barrier is held,
        // GetBarrierLsn() never moves past it, so records with a greater lsn
        // stay alive.
        //
        // As in the device, the lsn advances rarely (once per log record
        // written) while barriers are acquired on every read. The writer is
        // paced by the readers' progress, so it can neither hog the lock nor
        // outlive them.
        constexpr ui64 ReaderCount = 4;
        constexpr ui64 IterationCount = 2000;
        constexpr ui64 AdvanceCount = 100;
        constexpr ui64 TotalIterations = ReaderCount * IterationCount;

        TLsnBarrier barrier;

        std::atomic<ui64> iterationsDone = 0;
        std::atomic<ui64> contractViolations = 0;
        std::atomic<ui64> orderViolations = 0;

        TVector<std::thread> readers;
        readers.reserve(ReaderCount);

        for (ui64 i = 0; i < ReaderCount; ++i) {
            readers.emplace_back(
                [&]
                {
                    for (ui64 j = 0; j < IterationCount; ++j) {
                        const auto guard = barrier.Acquire();

                        if (barrier.GetBarrierLsn() > guard.GetLsn()) {
                            ++contractViolations;
                        }

                        // read the barrier lsn first - the current one only
                        // grows, so this ordering keeps the comparison
                        // meaningful
                        const ui64 barrierLsn = barrier.GetBarrierLsn();
                        if (barrierLsn > GetCurrentLsn(barrier)) {
                            ++orderViolations;
                        }

                        ++iterationsDone;
                    }
                });
        }

        std::thread writer(
            [&]
            {
                for (ui64 lsn = 1; lsn <= AdvanceCount; ++lsn) {
                    // wait for the readers to get through the next slice of
                    // their work before moving the lsn on
                    const ui64 due = lsn * TotalIterations / AdvanceCount;
                    while (iterationsDone.load() < due) {
                        std::this_thread::yield();
                    }

                    barrier.Advance(lsn);
                }
            });

        for (auto& reader: readers) {
            reader.join();
        }
        writer.join();

        UNIT_ASSERT_VALUES_EQUAL(0, contractViolations.load());
        UNIT_ASSERT_VALUES_EQUAL(0, orderViolations.load());

        // every barrier was released, so nothing is held any more
        UNIT_ASSERT_VALUES_EQUAL(AdvanceCount, GetCurrentLsn(barrier));
        UNIT_ASSERT_VALUES_EQUAL(AdvanceCount, barrier.GetBarrierLsn());
    }
}

}   // namespace NCloud::NJournalled
