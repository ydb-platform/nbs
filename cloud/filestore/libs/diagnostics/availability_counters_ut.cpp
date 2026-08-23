#include "availability_counters.h"

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/ptr.h>

#include <atomic>
#include <cerrno>
#include <thread>
#include <utility>

namespace NCloud::NFileStore {

namespace {

////////////////////////////////////////////////////////////////////////////////

using ERequestType = EFileStoreAvailabilityRequestType;

constexpr TDuration IntervalDuration = TDuration::Minutes(5);

struct TEnv
{
    NMonitoring::TDynamicCountersPtr CounterGroup =
        MakeIntrusive<NMonitoring::TDynamicCounters>();
    TAvailabilityCounters Counters;
    TInstant Now;

    NMonitoring::TDynamicCounters::TCounterPtr TotalIntervals;
    NMonitoring::TDynamicCounters::TCounterPtr AvailableIntervals;
    NMonitoring::TDynamicCounters::TCounterPtr UnavailableIntervals;
    NMonitoring::TDynamicCounters::TCounterPtr LastIntervalAvailable;
    NMonitoring::TDynamicCounters::TCounterPtr MissingIntervals;

    TEnv()
        // start from an interval-aligned instant to make the test arithmetic
        // obvious
        : Now(TInstant::Hours(100))
    {
        Counters.EnableAndRegister(IntervalDuration, *CounterGroup);
        // the first call only initializes the interval boundary
        Counters.UpdateStats(Now);

        TotalIntervals =
            CounterGroup->GetCounter("Availability_TotalIntervals", true);
        AvailableIntervals =
            CounterGroup->GetCounter("Availability_AvailableIntervals", true);
        UnavailableIntervals =
            CounterGroup->GetCounter("Availability_UnavailableIntervals", true);
        LastIntervalAvailable =
            CounterGroup->GetCounter("Availability_LastIntervalAvailable");
        MissingIntervals =
            CounterGroup->GetCounter("Availability_MissingIntervals", true);
    }

    // Advances time to the end of the current interval and rolls it over.
    void FinishInterval()
    {
        Now += IntervalDuration;
        Counters.UpdateStats(Now);
    }

    void AdvanceWithinInterval(TDuration duration)
    {
        Now += duration;
        UNIT_ASSERT(duration < IntervalDuration);
        Counters.UpdateStats(Now);
    }

    // Maps the published label back to the request type through the
    // production forward mapping.
    static ERequestType RequestTypeByName(const TString& requestName)
    {
        // index 0 is ERequestType::None, which has no published name
        for (size_t i = 1; i < FileStoreAvailabilityRequestTypeCount; ++i) {
            const auto requestType = static_cast<ERequestType>(i);
            if (requestName == GetAvailabilityRequestTypeName(requestType)) {
                return requestType;
            }
        }
        UNIT_FAIL("unknown availability request name: " << requestName);
        return ERequestType::None;
    }

    TCallContextPtr Start(const TString& requestName)
    {
        auto callContext = MakeIntrusive<TCallContext>();
        callContext->AvailabilityRequestType = RequestTypeByName(requestName);
        Counters.RequestStarted(*callContext, Now);
        return callContext;
    }

    void CompleteOk(const TCallContextPtr& callContext)
    {
        // production success replies do not write GuestReplyErrno: they rely
        // on it being 0 for the current attempt
        Counters.RequestCompleted(*callContext, Now);
    }

    void CompleteWithErrno(
        const TCallContextPtr& callContext,
        int guestReplyErrno)
    {
        callContext->GuestReplyErrno = guestReplyErrno;
        Counters.RequestCompleted(*callContext, Now);
    }

    // Asserts against the per-request-type published counters on the
    // "request=<type>" subgroup.
    // The name is passed as the literal expected label so that the test
    // does not use the production mapping function as its own oracle.
    void AssertRequestIntervals(
        const TString& requestName,
        i64 available,
        i64 unavailable,
        bool lastAvailable)
    {
        auto group = CounterGroup->FindSubgroup("request", requestName);
        UNIT_ASSERT(group);
        // the total is aggregated only - per-request it would be identical
        // for every request type
        UNIT_ASSERT(!group->FindCounter("Availability_TotalIntervals"));
        UNIT_ASSERT_VALUES_EQUAL(
            available,
            group->GetCounter("Availability_AvailableIntervals", true)->Val());
        UNIT_ASSERT_VALUES_EQUAL(
            unavailable,
            group->GetCounter(
                "Availability_UnavailableIntervals", true)->Val());
        UNIT_ASSERT_VALUES_EQUAL(
            lastAvailable,
            group->GetCounter("Availability_LastIntervalAvailable")->Val()
                != 0);
    }

    // Asserts against the aggregated published counters - the logical AND
    // over the request types.
    void AssertIntervals(
        i64 total,
        i64 available,
        i64 unavailable,
        bool lastAvailable)
    {
        UNIT_ASSERT_VALUES_EQUAL(total, TotalIntervals->Val());
        UNIT_ASSERT_VALUES_EQUAL(available, AvailableIntervals->Val());
        UNIT_ASSERT_VALUES_EQUAL(unavailable, UnavailableIntervals->Val());
        UNIT_ASSERT_VALUES_EQUAL(
            lastAvailable,
            LastIntervalAvailable->Val() != 0);
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TAvailabilityCountersTest)
{
    Y_UNIT_TEST(ShouldReportEmptyIntervalAsAvailable)
    {
        TEnv env;

        env.FinishInterval();
        env.AssertIntervals(
            1,       // total
            1,       // available
            0,       // unavailable
            true);   // last interval available

        env.FinishInterval();
        env.AssertIntervals(
            2,       // total
            2,       // available
            0,       // unavailable
            true);   // last interval available
    }

    Y_UNIT_TEST(ShouldReportSuccessfulRequestsAsAvailable)
    {
        TEnv env;

        auto callContext = env.Start("read");
        env.CompleteOk(callContext);

        env.FinishInterval();
        env.AssertIntervals(
            1,       // total
            1,       // available
            0,       // unavailable
            true);   // last interval available
    }

    Y_UNIT_TEST(ShouldReportNonEioErrorsAsAvailable)
    {
        TEnv env;

        // completion with an error response other than EIO is a normal
        // terminal outcome
        auto callContext = env.Start("getattr");
        env.CompleteWithErrno(callContext, ENOENT);

        env.FinishInterval();
        env.AssertIntervals(
            1,       // total
            1,       // available
            0,       // unavailable
            true);   // last interval available
    }

    Y_UNIT_TEST(ShouldReportEioOnlyIntervalAsUnavailable)
    {
        TEnv env;

        for (int i = 0; i < 3; ++i) {
            auto callContext = env.Start("write");
            env.CompleteWithErrno(callContext, EIO);
        }

        env.FinishInterval();
        env.AssertIntervals(
            1,       // total
            0,       // available
            1,       // unavailable
            false);  // last interval available

        // the next interval has no requests => available again
        env.FinishInterval();
        env.AssertIntervals(
            2,       // total
            1,       // available
            1,       // unavailable
            true);   // last interval available
    }

    Y_UNIT_TEST(ShouldReportMixedEioAndSuccessAsAvailable)
    {
        TEnv env;

        auto bad = env.Start("write");
        env.CompleteWithErrno(bad, EIO);

        auto good = env.Start("write");
        env.CompleteOk(good);

        env.FinishInterval();
        env.AssertIntervals(
            1,       // total
            1,       // available
            0,       // unavailable
            true);   // last interval available
    }

    Y_UNIT_TEST(ShouldReportHungRequestAsUnavailable)
    {
        TEnv env;

        // the request is started in interval 1...
        auto callContext = env.Start("fsync");

        // ...so it is not hung during interval 1 (it has not been
        // outstanding throughout the entire interval)
        env.FinishInterval();
        env.AssertIntervals(
            1,       // total
            1,       // available
            0,       // unavailable
            true);   // last interval available

        // it remains outstanding throughout intervals 2 and 3 => hung
        env.FinishInterval();
        env.AssertIntervals(
            2,       // total
            1,       // available
            1,       // unavailable
            false);  // last interval available
        env.FinishInterval();
        env.AssertIntervals(
            3,       // total
            1,       // available
            2,       // unavailable
            false);  // last interval available

        // once it completes successfully, the interval becomes available
        env.CompleteOk(callContext);
        env.FinishInterval();
        env.AssertIntervals(
            4,       // total
            2,       // available
            2,       // unavailable
            true);   // last interval available
    }

    Y_UNIT_TEST(ShouldTreatLateNonEioCompletionOfOldRequestAsAvailable)
    {
        TEnv env;

        auto callContext = env.Start("open");
        env.FinishInterval();
        env.AssertIntervals(
            1,       // total
            1,       // available
            0,       // unavailable
            true);   // last interval available

        // the request was outstanding at the interval start and completed
        // with a non-EIO outcome during the interval => available
        env.AdvanceWithinInterval(TDuration::Minutes(1));
        env.CompleteOk(callContext);
        env.FinishInterval();
        env.AssertIntervals(
            2,       // total
            2,       // available
            0,       // unavailable
            true);   // last interval available
    }

    Y_UNIT_TEST(ShouldTreatLateEioCompletionOfOldRequestAsUnavailable)
    {
        TEnv env;

        auto callContext = env.Start("open");
        env.FinishInterval();
        env.AssertIntervals(
            1,       // total
            1,       // available
            0,       // unavailable
            true);   // last interval available

        // the only outstanding request of this type completed with EIO
        // during the interval => unavailable
        env.CompleteWithErrno(callContext, EIO);
        env.FinishInterval();
        env.AssertIntervals(
            2,       // total
            1,       // available
            1,       // unavailable
            false);  // last interval available
    }

    Y_UNIT_TEST(ShouldNotCountPendingRequestsAsAvailable)
    {
        TEnv env;

        // a request started during the interval and still outstanding at its
        // end is neutral: alone it does not make the interval unavailable...
        auto hungContext = env.Start("read");
        env.FinishInterval();
        env.AssertIntervals(
            1,       // total
            1,       // available
            0,       // unavailable
            true);   // last interval available

        // ...but it is not success evidence either: it does not make an
        // interval with a hung request available
        env.AdvanceWithinInterval(TDuration::Minutes(2));
        auto freshContext = env.Start("read");

        env.FinishInterval();
        env.AssertIntervals(
            2,       // total
            1,       // available
            1,       // unavailable
            false);  // last interval available

        // in the next interval both requests are hung
        env.FinishInterval();
        env.AssertIntervals(
            3,       // total
            1,       // available
            2,       // unavailable
            false);  // last interval available

        env.CompleteOk(hungContext);
        env.CompleteOk(freshContext);
        env.FinishInterval();
        env.AssertIntervals(
            4,       // total
            2,       // available
            2,       // unavailable
            true);   // last interval available
    }

    Y_UNIT_TEST(ShouldNotCountPendingRequestsAgainstEioCompletions)
    {
        TEnv env;

        // an EIO completion is failure evidence; a request started during
        // the interval and still pending at its end does not neutralize it
        auto eioContext = env.Start("write");
        env.CompleteWithErrno(eioContext, EIO);

        auto pendingContext = env.Start("write");

        env.FinishInterval();
        env.AssertIntervals(
            1,       // total
            0,       // available
            1,       // unavailable
            false);  // last interval available

        env.CompleteOk(pendingContext);
        env.FinishInterval();
        env.AssertIntervals(
            2,       // total
            1,       // available
            1,       // unavailable
            true);   // last interval available
    }

    Y_UNIT_TEST(ShouldDistinguishOldAndFreshRequestsOnCompletion)
    {
        TEnv env;

        // a pending request started within the interval is neutral
        auto oldContext = env.Start("read");
        env.FinishInterval();
        env.AssertIntervals(
            1,       // total
            1,       // available
            0,       // unavailable
            true);   // last interval available

        // The old request completes while a fresh one is also outstanding.
        // The completion must consume the old registration via the interval
        // sequence number: the fresh request then stays fresh-pending
        // (neutral) and the successful completion is the only evidence =>
        // available. Decrementing the fresh request's accounting instead
        // would classify it as hung and flip the interval to unavailable.
        auto freshContext = env.Start("read");
        env.CompleteOk(oldContext);

        env.FinishInterval();
        env.AssertIntervals(
            2,       // total
            2,       // available
            0,       // unavailable
            true);   // last interval available

        // the fresh request is old by now and hangs the third interval
        env.FinishInterval();
        env.AssertIntervals(
            3,       // total
            2,       // available
            1,       // unavailable
            false);  // last interval available

        env.CompleteOk(freshContext);
        env.FinishInterval();
        env.AssertIntervals(
            4,       // total
            3,       // available
            1,       // unavailable
            true);   // last interval available
    }

    Y_UNIT_TEST(ShouldClassifyRequestTypesIndependently)
    {
        TEnv env;

        // plenty of successful reads...
        for (int i = 0; i < 100; ++i) {
            auto callContext = env.Start("read");
            env.CompleteOk(callContext);
        }

        // ...do not mask a fully failed write type
        auto badContext = env.Start("write");
        env.CompleteWithErrno(badContext, EIO);

        env.FinishInterval();
        env.AssertIntervals(
            1,       // total
            0,       // available
            1,       // unavailable
            false);  // last interval available
    }

    Y_UNIT_TEST(ShouldNotTrackUntrackedRequestTypes)
    {
        TEnv env;

        auto eioContext = MakeIntrusive<TCallContext>();
        eioContext->AvailabilityRequestType = ERequestType::None;
        env.Counters.RequestStarted(*eioContext, env.Now);
        env.CompleteWithErrno(eioContext, EIO);
        // never completed
        auto hungContext = MakeIntrusive<TCallContext>();
        hungContext->AvailabilityRequestType = ERequestType::None;
        env.Counters.RequestStarted(*hungContext, env.Now);

        env.FinishInterval();
        env.AssertIntervals(
            1,       // total
            1,       // available
            0,       // unavailable
            true);   // last interval available

        env.FinishInterval();
        env.AssertIntervals(
            2,       // total
            2,       // available
            0,       // unavailable
            true);   // last interval available
    }

    Y_UNIT_TEST(ShouldNotFinishIntervalBeforeItsEnd)
    {
        TEnv env;

        env.AdvanceWithinInterval(TDuration::Minutes(4));
        env.AssertIntervals(
            0,       // total
            0,       // available
            0,       // unavailable
            true);   // last interval available

        env.AdvanceWithinInterval(
            TDuration::Minutes(1) - TDuration::Seconds(1));
        env.AssertIntervals(
            0,       // total
            0,       // available
            0,       // unavailable
            true);   // last interval available

        env.Now += TDuration::Seconds(1);
        env.Counters.UpdateStats(env.Now);
        env.AssertIntervals(
            1,       // total
            1,       // available
            0,       // unavailable
            true);   // last interval available
    }

    Y_UNIT_TEST(ShouldCountIntervalsMissedBeyondCatchUpLimit)
    {
        // exactly at the catch-up limit every elapsed interval is evaluated
        {
            TEnv env;
            env.Now += IntervalDuration * 30;
            env.Counters.UpdateStats(env.Now);
            env.AssertIntervals(
                30,      // total
                30,      // available
                0,       // unavailable
                true);   // last interval available
            UNIT_ASSERT_VALUES_EQUAL(0, env.MissingIntervals->Val());
        }

        // one interval beyond the limit is reported as missing, not dropped
        {
            TEnv env;
            env.Now += IntervalDuration * 31;
            env.Counters.UpdateStats(env.Now);
            env.AssertIntervals(
                30,      // total
                30,      // available
                0,       // unavailable
                true);   // last interval available
            UNIT_ASSERT_VALUES_EQUAL(1, env.MissingIntervals->Val());
        }

        // a five-hour gap: half evaluated, half reported as missing
        {
            TEnv env;
            env.Now += IntervalDuration * 60;
            env.Counters.UpdateStats(env.Now);
            env.AssertIntervals(
                30,      // total
                30,      // available
                0,       // unavailable
                true);   // last interval available
            UNIT_ASSERT_VALUES_EQUAL(30, env.MissingIntervals->Val());

            // measurement continues normally afterwards
            env.FinishInterval();
            env.AssertIntervals(
                31,      // total
                31,      // available
                0,       // unavailable
                true);   // last interval available
            UNIT_ASSERT_VALUES_EQUAL(30, env.MissingIntervals->Val());
        }
    }

    Y_UNIT_TEST(ShouldCountIntervalsMissedWithHungRequest)
    {
        // the request is started right away: the first interval sees a fresh
        // pending request (neutral => available), the remaining evaluated
        // intervals see it hung (=> unavailable)
        //
        // the intervals beyond the catch-up limit are reported as missing,
        // not as unavailable
        {
            TEnv env;
            env.Start("read");
            env.Now += IntervalDuration * 30;
            env.Counters.UpdateStats(env.Now);
            env.AssertIntervals(
                30,      // total
                1,       // available
                29,      // unavailable
                false);  // last interval available
            UNIT_ASSERT_VALUES_EQUAL(0, env.MissingIntervals->Val());
        }

        {
            TEnv env;
            env.Start("read");
            env.Now += IntervalDuration * 31;
            env.Counters.UpdateStats(env.Now);
            env.AssertIntervals(
                30,      // total
                1,       // available
                29,      // unavailable
                false);  // last interval available
            UNIT_ASSERT_VALUES_EQUAL(1, env.MissingIntervals->Val());
        }

        {
            TEnv env;
            env.Start("read");
            env.Now += IntervalDuration * 60;
            env.Counters.UpdateStats(env.Now);
            env.AssertIntervals(
                30,      // total
                1,       // available
                29,      // unavailable
                false);  // last interval available
            UNIT_ASSERT_VALUES_EQUAL(30, env.MissingIntervals->Val());
        }
    }

    Y_UNIT_TEST(ShouldCatchUpAfterUpdateStall)
    {
        TEnv env;

        // a request hangs and the stats updater stalls for 3 intervals
        auto callContext = env.Start("getattr");
        env.FinishInterval();
        env.AssertIntervals(
            1,       // total
            1,       // available
            0,       // unavailable
            true);   // last interval available

        env.Now += IntervalDuration * 3;
        env.Counters.UpdateStats(env.Now);

        // the request remained outstanding throughout all 3 intervals
        env.AssertIntervals(
            4,       // total
            1,       // available
            3,       // unavailable
            false);  // last interval available

        env.CompleteOk(callContext);
        env.FinishInterval();
        env.AssertIntervals(
            5,       // total
            2,       // available
            3,       // unavailable
            true);   // last interval available
    }

    Y_UNIT_TEST(ShouldCountRequestStartedAndFailedWithinOneInterval)
    {
        TEnv env;

        // an EIO completion of a request that both started and finished
        // within the interval still makes the interval unavailable
        auto callContext = env.Start("mkdir");
        env.AdvanceWithinInterval(TDuration::Seconds(30));
        env.CompleteWithErrno(callContext, EIO);

        env.FinishInterval();
        env.AssertIntervals(
            1,       // total
            0,       // available
            1,       // unavailable
            false);  // last interval available
    }

    Y_UNIT_TEST(ShouldAccountRequestTypesIndependently)
    {
        {
            // lookup and getattr both map to GetNodeAttr
            TEnv env;
            auto bad = env.Start("lookup");
            env.CompleteWithErrno(bad, EIO);
            auto good = env.Start("getattr");
            env.CompleteOk(good);

            env.FinishInterval();
            env.AssertIntervals(
                1,       // total
                0,       // available
                1,       // unavailable
                false);  // last interval available
            env.AssertRequestIntervals(
                "lookup",
                0,       // available
                1,       // unavailable
                false);  // last interval available
            env.AssertRequestIntervals(
                "getattr",
                1,       // available
                0,       // unavailable
                true);   // last interval available
        }

        {
            // open and create both map to CreateHandle
            TEnv env;
            auto bad = env.Start("open");
            env.CompleteWithErrno(bad, EIO);
            auto good = env.Start("create");
            env.CompleteOk(good);

            env.FinishInterval();
            env.AssertIntervals(
                1,       // total
                0,       // available
                1,       // unavailable
                false);  // last interval available
            env.AssertRequestIntervals(
                "open",
                0,       // available
                1,       // unavailable
                false);  // last interval available
            env.AssertRequestIntervals(
                "create",
                1,       // available
                0,       // unavailable
                true);   // last interval available
        }

        {
            // mkdir and symlink both map to CreateNode
            TEnv env;
            auto bad = env.Start("mkdir");
            env.CompleteWithErrno(bad, EIO);
            auto good = env.Start("symlink");
            env.CompleteOk(good);

            env.FinishInterval();
            env.AssertIntervals(
                1,       // total
                0,       // available
                1,       // unavailable
                false);  // last interval available
            env.AssertRequestIntervals(
                "mkdir",
                0,       // available
                1,       // unavailable
                false);  // last interval available
            env.AssertRequestIntervals(
                "symlink",
                1,       // available
                0,       // unavailable
                true);   // last interval available
        }

        {
            // write and write_buf both map to WriteData
            TEnv env;
            auto bad = env.Start("write");
            env.CompleteWithErrno(bad, EIO);
            auto good = env.Start("write_buf");
            env.CompleteOk(good);

            env.FinishInterval();
            env.AssertIntervals(
                1,       // total
                0,       // available
                1,       // unavailable
                false);  // last interval available
            env.AssertRequestIntervals(
                "write",
                0,       // available
                1,       // unavailable
                false);  // last interval available
            env.AssertRequestIntervals(
                "write_buf",
                1,       // available
                0,       // unavailable
                true);   // last interval available
        }
    }

    Y_UNIT_TEST(ShouldNotMarkPostBoundaryStartHung)
    {
        TEnv env;

        // move one second into the next wall-clock interval without any
        // updater tick
        env.Now += IntervalDuration + TDuration::Seconds(1);

        // the request starts in (wall-clock) interval 2; the start event
        // itself rolls the accounting past the boundary first
        env.Start("read");

        // the late updater closes interval 1, in which the request did not
        // exist
        env.Counters.UpdateStats(env.Now);

        // reach the end of the interval in which the request actually
        // started: it was fresh-pending there => neutral, not hung
        //
        // both intervals must be available
        env.Now += IntervalDuration - TDuration::Seconds(1);
        env.Counters.UpdateStats(env.Now);

        env.AssertIntervals(
            2,       // total
            2,       // available
            0,       // unavailable
            true);   // last interval available
    }

    Y_UNIT_TEST(ShouldKeepHungIntervalUnavailableOnPostBoundaryCompletion)
    {
        TEnv env;

        // the request is fresh-pending in the first interval => neutral
        auto callContext = env.Start("read");
        env.FinishInterval();
        env.AssertIntervals(
            1,       // total
            1,       // available
            0,       // unavailable
            true);   // last interval available

        // it stays outstanding through the entire second interval and
        // completes successfully one second after the boundary, before any
        // updater tick
        //
        // the completion event rolls the accounting first, so
        // the second interval is still classified with the request hung
        // through it, and the success lands in the third interval
        env.Now += IntervalDuration + TDuration::Seconds(1);
        env.CompleteOk(callContext);
        env.Counters.UpdateStats(env.Now);
        env.AssertIntervals(
            2,       // total
            1,       // available
            1,       // unavailable
            false);  // last interval available

        // the third interval carries the successful completion
        env.Now += IntervalDuration - TDuration::Seconds(1);
        env.Counters.UpdateStats(env.Now);
        env.AssertIntervals(
            3,       // total
            2,       // available
            1,       // unavailable
            true);   // last interval available
    }

    Y_UNIT_TEST(ShouldAccountActivityDuringUpdaterStall)
    {
        TEnv env;

        // no updater ticks happen between the events below: every event is
        // still assigned to its actual wall-clock interval because the
        // request hooks roll the accounting on demand

        env.Now += IntervalDuration + TDuration::Seconds(1);
        // interval 2: a starts (interval 1 elapsed empty => available)
        auto a = env.Start("read");

        env.Now += IntervalDuration;
        // interval 3: a completes successfully; a was fresh-pending in
        // interval 2 => neutral there => interval 2 is available; b starts
        env.CompleteOk(a);
        auto b = env.Start("write");

        env.Now += IntervalDuration;
        // interval 4: b completes with EIO; interval 3 saw a's success and
        // the fresh-pending b => available
        env.CompleteWithErrno(b, EIO);

        env.Counters.UpdateStats(env.Now);
        env.AssertIntervals(
            3,       // total
            3,       // available
            0,       // unavailable
            true);   // last interval available

        // interval 4 carries only b's EIO completion => unavailable
        env.Now += IntervalDuration - TDuration::Seconds(1);
        env.Counters.UpdateStats(env.Now);
        env.AssertIntervals(
            4,       // total
            3,       // available
            1,       // unavailable
            false);  // last interval available
        env.AssertRequestIntervals(
            "write",
            3,       // available
            1,       // unavailable
            false);  // last interval available
        env.AssertRequestIntervals(
            "read",
            4,       // available
            0,       // unavailable
            true);   // last interval available
    }

    Y_UNIT_TEST(ShouldEnableConcurrentlyWithRequestProcessing)
    {
        auto counterGroup = MakeIntrusive<NMonitoring::TDynamicCounters>();
        TAvailabilityCounters counters;
        const TInstant start = TInstant::Hours(100);

        // a request started before the tracking is enabled gets no stamp
        auto preEnable = MakeIntrusive<TCallContext>();
        preEnable->AvailabilityRequestType = ERequestType::Read;
        counters.RequestStarted(*preEnable, start);
        UNIT_ASSERT_VALUES_EQUAL(0, preEnable->AvailabilityIntervalSeqNo);

        // Request processing and the stats updater run concurrently with
        // two racing EnableAndRegister() calls. The updater advances the
        // shared clock, so interval boundaries are crossed while requests
        // are processed: the on-demand rollover in the request hooks races
        // the updater rollover.
        std::atomic<ui64> nowUs = start.MicroSeconds();
        std::atomic<ui64> iterations = 0;
        std::atomic<bool> stop = false;

        std::thread requester([&] {
            while (!stop.load()) {
                auto context = MakeIntrusive<TCallContext>();
                context->AvailabilityRequestType = ERequestType::Read;
                const auto now = TInstant::MicroSeconds(nowUs.load());
                counters.RequestStarted(*context, now);
                counters.RequestCompleted(*context, now);
                ++iterations;
            }
        });
        std::thread updater([&] {
            while (!stop.load()) {
                counters.UpdateStats(TInstant::MicroSeconds(nowUs.fetch_add(
                    TDuration::Seconds(1).MicroSeconds())));
            }
        });

        // at least one full iteration with the tracking disabled
        while (iterations.load() == 0) {}

        std::atomic<bool> go = false;
        std::thread enabler1([&] {
            while (!go.load()) {}
            counters.EnableAndRegister(IntervalDuration, *counterGroup);
        });
        std::thread enabler2([&] {
            while (!go.load()) {}
            counters.EnableAndRegister(IntervalDuration, *counterGroup);
        });
        go = true;
        enabler1.join();
        enabler2.join();

        // at least 100 full iterations and three elapsed intervals with
        // the tracking enabled
        const ui64 enabledFrom = iterations.load();
        const ui64 enabledNowUs = nowUs.load();
        while (iterations.load() < enabledFrom + 100 ||
               nowUs.load() <
                   enabledNowUs + 3 * IntervalDuration.MicroSeconds())
        {}

        stop = true;
        requester.join();
        updater.join();

        // The pre-enable request completes after enabling: it carries no
        // stamp and is ignored. A repeated completion of a tracked request
        // is ignored the same way, neither may disturb the accounting.
        const auto end = TInstant::MicroSeconds(nowUs.load());
        counters.RequestCompleted(*preEnable, end);
        auto repeated = MakeIntrusive<TCallContext>();
        repeated->AvailabilityRequestType = ERequestType::Read;
        counters.RequestStarted(*repeated, end);
        counters.RequestCompleted(*repeated, end);
        counters.RequestCompleted(*repeated, end);

        // The accounting stays consistent across the concurrent rollovers:
        // every closed interval is classified exactly once. Which intervals
        // a racing request lands in is timing-dependent, so per-interval
        // classification is not asserted here - the deterministic tests
        // above cover it.
        counters.UpdateStats(end + IntervalDuration);
        const i64 total = counterGroup->GetCounter(
            "Availability_TotalIntervals",
            true)->Val();
        const i64 available = counterGroup->GetCounter(
            "Availability_AvailableIntervals",
            true)->Val();
        const i64 unavailable = counterGroup->GetCounter(
            "Availability_UnavailableIntervals",
            true)->Val();
        UNIT_ASSERT(total >= 2);
        UNIT_ASSERT_VALUES_EQUAL(total, available + unavailable);
    }

    Y_UNIT_TEST(ShouldSkipPartiallyObservedFirstInterval)
    {
        auto counterGroup = MakeIntrusive<NMonitoring::TDynamicCounters>();
        TAvailabilityCounters counters;
        counters.EnableAndRegister(IntervalDuration, *counterGroup);

        // The measurement begins with the first observed activity after
        // enabling - here the request event itself, with no updater tick
        // before it - two minutes into a wall-clock interval.
        const TInstant alignedStart = TInstant::Hours(100);
        const TInstant firstEventTime = alignedStart + TDuration::Minutes(2);

        // an EIO completion within the partially observed interval
        auto context = MakeIntrusive<TCallContext>();
        context->AvailabilityRequestType = ERequestType::Read;
        counters.RequestStarted(*context, firstEventTime);
        context->GuestReplyErrno = EIO;
        counters.RequestCompleted(*context, firstEventTime);

        // The partial interval rolls over without classification even
        // though it saw an EIO, because its first two minutes were not
        // observed at all: neither the aggregated nor the per-type
        // counters are published for it.
        counters.UpdateStats(alignedStart + IntervalDuration);
        auto total =
            counterGroup->GetCounter("Availability_TotalIntervals", true);
        UNIT_ASSERT_VALUES_EQUAL(0, total->Val());
        auto readGroup = counterGroup->FindSubgroup("request", "read");
        UNIT_ASSERT(readGroup);
        UNIT_ASSERT_VALUES_EQUAL(
            0,
            readGroup->GetCounter(
                "Availability_UnavailableIntervals",
                true)->Val());

        // the first fully observed interval is classified normally
        counters.UpdateStats(alignedStart + IntervalDuration * 2);
        UNIT_ASSERT_VALUES_EQUAL(1, total->Val());
        UNIT_ASSERT_VALUES_EQUAL(
            1,
            counterGroup->GetCounter(
                "Availability_AvailableIntervals",
                true)->Val());
    }

    Y_UNIT_TEST(ShouldPublishPerRequestTypeCounters)
    {
        TEnv env;

        // the "request" label value is the lower-case FUSE request name as
        // listed in the SLA
        UNIT_ASSERT_VALUES_EQUAL(
            "write",
            TString(GetAvailabilityRequestTypeName(
                ERequestType::Write)));
        UNIT_ASSERT_VALUES_EQUAL(
            "write_buf",
            TString(GetAvailabilityRequestTypeName(
                ERequestType::WriteBuf)));

        auto badContext = env.Start("write");
        env.CompleteWithErrno(badContext, EIO);

        env.FinishInterval();

        // the failing request type is unavailable in its own counters...
        env.AssertRequestIntervals(
            "write",
            0,       // available
            1,       // unavailable
            false);  // last interval available
        // ...while an idle request type is available in the same interval...
        env.AssertRequestIntervals(
            "read",
            1,       // available
            0,       // unavailable
            true);   // last interval available
        // ...and the aggregated counters are the AND over the request types
        env.AssertIntervals(
            1,       // total
            0,       // available
            1,       // unavailable
            false);  // last interval available

        auto goodContext = env.Start("write");
        env.CompleteOk(goodContext);
        env.FinishInterval();

        env.AssertRequestIntervals(
            "write",
            1,       // available
            1,       // unavailable
            true);   // last interval available
        env.AssertRequestIntervals(
            "read",
            2,       // available
            0,       // unavailable
            true);   // last interval available
        env.AssertIntervals(
            2,       // total
            1,       // available
            1,       // unavailable
            true);   // last interval available
    }

    Y_UNIT_TEST(ShouldStartAsAvailable)
    {
        TEnv env;

        // before any interval is finished the gauges report available, both
        // aggregated and per request type
        env.AssertIntervals(
            0,       // total
            0,       // available
            0,       // unavailable
            true);   // last interval available
        env.AssertRequestIntervals(
            "lookup",
            0,       // available
            0,       // unavailable
            true);   // last interval available
    }
}

}   // namespace NCloud::NFileStore
