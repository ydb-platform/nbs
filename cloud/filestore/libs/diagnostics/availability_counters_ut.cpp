#include "availability_counters.h"

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/ptr.h>

#include <atomic>
#include <cerrno>
#include <thread>

namespace NCloud::NFileStore {

namespace {

////////////////////////////////////////////////////////////////////////////////

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

    TCallContextPtr Start(EFileStoreAvailabilityRequestType requestType)
    {
        auto callContext = MakeIntrusive<TCallContext>();
        callContext->AvailabilityRequestType = requestType;
        Counters.RequestStarted(*callContext);
        return callContext;
    }

    void CompleteOk(const TCallContextPtr& callContext)
    {
        // production success replies do not write GuestReplyErrno: they rely
        // on it being 0 for the current attempt
        Counters.RequestCompleted(*callContext);
    }

    void CompleteWithErrno(
        const TCallContextPtr& callContext,
        int guestReplyErrno)
    {
        callContext->GuestReplyErrno = guestReplyErrno;
        Counters.RequestCompleted(*callContext);
    }

    // Asserts against the per-request-type published counters on the
    // "request=<type>" subgroup.
    void AssertRequestIntervals(
        EFileStoreAvailabilityRequestType requestType,
        i64 available,
        i64 unavailable,
        bool lastAvailable)
    {
        auto group = CounterGroup->FindSubgroup(
            "request",
            GetAvailabilityRequestTypeName(requestType));
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
            1,   // total
            1,   // available
            0,   // unavailable
            true);   // last interval available

        env.FinishInterval();
        env.AssertIntervals(
            2,   // total
            2,   // available
            0,   // unavailable
            true);   // last interval available
    }

    Y_UNIT_TEST(ShouldReportSuccessfulRequestsAsAvailable)
    {
        TEnv env;

        auto callContext = env.Start(EFileStoreAvailabilityRequestType::Read);
        env.CompleteOk(callContext);

        env.FinishInterval();
        env.AssertIntervals(
            1,   // total
            1,   // available
            0,   // unavailable
            true);   // last interval available
    }

    Y_UNIT_TEST(ShouldReportNonEioErrorsAsAvailable)
    {
        TEnv env;

        // completion with an error response other than EIO is a normal
        // terminal outcome
        auto callContext =
            env.Start(EFileStoreAvailabilityRequestType::GetAttr);
        env.CompleteWithErrno(callContext, ENOENT);

        env.FinishInterval();
        env.AssertIntervals(
            1,   // total
            1,   // available
            0,   // unavailable
            true);   // last interval available
    }

    Y_UNIT_TEST(ShouldReportEioOnlyIntervalAsUnavailable)
    {
        TEnv env;

        for (int i = 0; i < 3; ++i) {
            auto callContext =
                env.Start(EFileStoreAvailabilityRequestType::Write);
            env.CompleteWithErrno(callContext, EIO);
        }

        env.FinishInterval();
        env.AssertIntervals(
            1,   // total
            0,   // available
            1,   // unavailable
            false);   // last interval available

        // the next interval has no requests => available again
        env.FinishInterval();
        env.AssertIntervals(
            2,   // total
            1,   // available
            1,   // unavailable
            true);   // last interval available
    }

    Y_UNIT_TEST(ShouldReportMixedEioAndSuccessAsAvailable)
    {
        TEnv env;

        auto bad = env.Start(EFileStoreAvailabilityRequestType::Write);
        env.CompleteWithErrno(bad, EIO);

        auto good = env.Start(EFileStoreAvailabilityRequestType::Write);
        env.CompleteOk(good);

        env.FinishInterval();
        env.AssertIntervals(
            1,   // total
            1,   // available
            0,   // unavailable
            true);   // last interval available
    }

    Y_UNIT_TEST(ShouldReportHungRequestAsUnavailable)
    {
        TEnv env;

        // the request is started in interval 1...
        auto callContext = env.Start(EFileStoreAvailabilityRequestType::Fsync);

        // ...so it is not hung during interval 1 (it has not been
        // outstanding throughout the entire interval)
        env.FinishInterval();
        env.AssertIntervals(
            1,   // total
            1,   // available
            0,   // unavailable
            true);   // last interval available

        // it remains outstanding throughout intervals 2 and 3 => hung
        env.FinishInterval();
        env.AssertIntervals(
            2,   // total
            1,   // available
            1,   // unavailable
            false);   // last interval available
        env.FinishInterval();
        env.AssertIntervals(
            3,   // total
            1,   // available
            2,   // unavailable
            false);   // last interval available

        // once it completes successfully, the interval becomes available
        env.CompleteOk(callContext);
        env.FinishInterval();
        env.AssertIntervals(
            4,   // total
            2,   // available
            2,   // unavailable
            true);   // last interval available
    }

    Y_UNIT_TEST(ShouldTreatLateNonEioCompletionOfOldRequestAsAvailable)
    {
        TEnv env;

        auto callContext = env.Start(EFileStoreAvailabilityRequestType::Open);
        env.FinishInterval();
        env.AssertIntervals(
            1,   // total
            1,   // available
            0,   // unavailable
            true);   // last interval available

        // the request was outstanding at the interval start and completed
        // with a non-EIO outcome during the interval => available
        env.AdvanceWithinInterval(TDuration::Minutes(1));
        env.CompleteOk(callContext);
        env.FinishInterval();
        env.AssertIntervals(
            2,   // total
            2,   // available
            0,   // unavailable
            true);   // last interval available
    }

    Y_UNIT_TEST(ShouldTreatLateEioCompletionOfOldRequestAsUnavailable)
    {
        TEnv env;

        auto callContext = env.Start(EFileStoreAvailabilityRequestType::Open);
        env.FinishInterval();
        env.AssertIntervals(
            1,   // total
            1,   // available
            0,   // unavailable
            true);   // last interval available

        // the only outstanding request of this type completed with EIO
        // during the interval => unavailable
        env.CompleteWithErrno(callContext, EIO);
        env.FinishInterval();
        env.AssertIntervals(
            2,   // total
            1,   // available
            1,   // unavailable
            false);   // last interval available
    }

    Y_UNIT_TEST(ShouldNotCountPendingRequestsAsAvailable)
    {
        TEnv env;

        // a request started during the interval and still outstanding at its
        // end is neutral: alone it does not make the interval unavailable...
        auto hungContext = env.Start(EFileStoreAvailabilityRequestType::Read);
        env.FinishInterval();
        env.AssertIntervals(
            1,   // total
            1,   // available
            0,   // unavailable
            true);   // last interval available

        // ...but it is not success evidence either: it does not make an
        // interval with a hung request available
        env.AdvanceWithinInterval(TDuration::Minutes(2));
        auto freshContext =
            env.Start(EFileStoreAvailabilityRequestType::Read);

        env.FinishInterval();
        env.AssertIntervals(
            2,   // total
            1,   // available
            1,   // unavailable
            false);   // last interval available

        // in the next interval both requests are hung
        env.FinishInterval();
        env.AssertIntervals(
            3,   // total
            1,   // available
            2,   // unavailable
            false);   // last interval available

        env.CompleteOk(hungContext);
        env.CompleteOk(freshContext);
        env.FinishInterval();
        env.AssertIntervals(
            4,   // total
            2,   // available
            2,   // unavailable
            true);   // last interval available
    }

    Y_UNIT_TEST(ShouldNotCountPendingRequestsAgainstEioCompletions)
    {
        TEnv env;

        // an EIO completion is failure evidence; a request started during
        // the interval and still pending at its end does not neutralize it
        auto eioContext = env.Start(EFileStoreAvailabilityRequestType::Write);
        env.CompleteWithErrno(eioContext, EIO);

        auto pendingContext =
            env.Start(EFileStoreAvailabilityRequestType::Write);

        env.FinishInterval();
        env.AssertIntervals(
            1,   // total
            0,   // available
            1,   // unavailable
            false);   // last interval available

        env.CompleteOk(pendingContext);
        env.FinishInterval();
        env.AssertIntervals(
            2,   // total
            1,   // available
            1,   // unavailable
            true);   // last interval available
    }

    Y_UNIT_TEST(ShouldDistinguishOldAndFreshRequestsOnCompletion)
    {
        TEnv env;

        // a pending request started within the interval is neutral
        auto oldContext = env.Start(EFileStoreAvailabilityRequestType::Read);
        env.FinishInterval();
        env.AssertIntervals(
            1,   // total
            1,   // available
            0,   // unavailable
            true);   // last interval available

        // The old request completes while a fresh one is also outstanding.
        // The completion must consume the old registration via the interval
        // sequence number: the fresh request then stays fresh-pending
        // (neutral) and the successful completion is the only evidence =>
        // available. Decrementing the fresh request's accounting instead
        // would classify it as hung and flip the interval to unavailable.
        auto freshContext =
            env.Start(EFileStoreAvailabilityRequestType::Read);
        env.CompleteOk(oldContext);

        env.FinishInterval();
        env.AssertIntervals(
            2,   // total
            2,   // available
            0,   // unavailable
            true);   // last interval available

        // the fresh request is old by now and hangs the third interval
        env.FinishInterval();
        env.AssertIntervals(
            3,   // total
            2,   // available
            1,   // unavailable
            false);   // last interval available

        env.CompleteOk(freshContext);
        env.FinishInterval();
        env.AssertIntervals(
            4,   // total
            3,   // available
            1,   // unavailable
            true);   // last interval available
    }

    Y_UNIT_TEST(ShouldClassifyRequestTypesIndependently)
    {
        TEnv env;

        // plenty of successful reads...
        for (int i = 0; i < 100; ++i) {
            auto callContext =
                env.Start(EFileStoreAvailabilityRequestType::Read);
            env.CompleteOk(callContext);
        }

        // ...do not mask a fully failed write type
        auto badContext =
            env.Start(EFileStoreAvailabilityRequestType::Write);
        env.CompleteWithErrno(badContext, EIO);

        env.FinishInterval();
        env.AssertIntervals(
            1,   // total
            0,   // available
            1,   // unavailable
            false);   // last interval available
    }

    Y_UNIT_TEST(ShouldNotTrackUntrackedRequestTypes)
    {
        TEnv env;

        // Requests with EFileStoreAvailabilityRequestType::None do not
        // affect the metric even if they fail with EIO or hang. (The FUSE
        // dispatch assigns None to the requests outside the availability
        // SLA - e.g. mknod, access, xattr and lock requests; that
        // assignment lives in the CALL table in loop.cpp and is not covered
        // here.)
        auto eioContext =
            env.Start(EFileStoreAvailabilityRequestType::None);
        env.CompleteWithErrno(eioContext, EIO);
        // never completed
        env.Start(EFileStoreAvailabilityRequestType::None);

        env.FinishInterval();
        env.AssertIntervals(
            1,   // total
            1,   // available
            0,   // unavailable
            true);   // last interval available

        env.FinishInterval();
        env.AssertIntervals(
            2,   // total
            2,   // available
            0,   // unavailable
            true);   // last interval available
    }

    Y_UNIT_TEST(ShouldNotFinishIntervalBeforeItsEnd)
    {
        TEnv env;

        env.AdvanceWithinInterval(TDuration::Minutes(4));
        env.AssertIntervals(
            0,   // total
            0,   // available
            0,   // unavailable
            true);   // last interval available

        env.AdvanceWithinInterval(
            TDuration::Minutes(1) - TDuration::Seconds(1));
        env.AssertIntervals(
            0,   // total
            0,   // available
            0,   // unavailable
            true);   // last interval available

        env.Now += TDuration::Seconds(1);
        env.Counters.UpdateStats(env.Now);
        env.AssertIntervals(
            1,   // total
            1,   // available
            0,   // unavailable
            true);   // last interval available
    }

    Y_UNIT_TEST(ShouldCountIntervalsMissedBeyondCatchUpLimit)
    {
        // exactly at the catch-up limit every elapsed interval is evaluated
        {
            TEnv env;
            env.Now += IntervalDuration * 12;
            env.Counters.UpdateStats(env.Now);
            env.AssertIntervals(
                12,   // total
                12,   // available
                0,   // unavailable
                true);   // last interval available
            UNIT_ASSERT_VALUES_EQUAL(0, env.MissingIntervals->Val());
        }

        // one interval beyond the limit is reported as missing, not dropped
        {
            TEnv env;
            env.Now += IntervalDuration * 13;
            env.Counters.UpdateStats(env.Now);
            env.AssertIntervals(
                12,   // total
                12,   // available
                0,   // unavailable
                true);   // last interval available
            UNIT_ASSERT_VALUES_EQUAL(1, env.MissingIntervals->Val());
        }

        // a two-hour gap: half evaluated, half reported as missing
        {
            TEnv env;
            env.Now += IntervalDuration * 24;
            env.Counters.UpdateStats(env.Now);
            env.AssertIntervals(
                12,   // total
                12,   // available
                0,   // unavailable
                true);   // last interval available
            UNIT_ASSERT_VALUES_EQUAL(12, env.MissingIntervals->Val());

            // measurement continues normally afterwards
            env.FinishInterval();
            env.AssertIntervals(
                13,   // total
                13,   // available
                0,   // unavailable
                true);   // last interval available
            UNIT_ASSERT_VALUES_EQUAL(12, env.MissingIntervals->Val());
        }
    }

    Y_UNIT_TEST(ShouldCountIntervalsMissedWithHungRequest)
    {
        // the request is started right away: the first interval sees a fresh
        // pending request (neutral => available), the remaining evaluated
        // intervals see it hung (=> unavailable); the intervals beyond the
        // catch-up limit are reported as missing, not as unavailable
        {
            TEnv env;
            env.Start(EFileStoreAvailabilityRequestType::Read);
            env.Now += IntervalDuration * 12;
            env.Counters.UpdateStats(env.Now);
            env.AssertIntervals(
                12,   // total
                1,   // available
                11,   // unavailable
                false);   // last interval available
            UNIT_ASSERT_VALUES_EQUAL(0, env.MissingIntervals->Val());
        }

        {
            TEnv env;
            env.Start(EFileStoreAvailabilityRequestType::Read);
            env.Now += IntervalDuration * 13;
            env.Counters.UpdateStats(env.Now);
            env.AssertIntervals(
                12,   // total
                1,   // available
                11,   // unavailable
                false);   // last interval available
            UNIT_ASSERT_VALUES_EQUAL(1, env.MissingIntervals->Val());
        }

        {
            TEnv env;
            env.Start(EFileStoreAvailabilityRequestType::Read);
            env.Now += IntervalDuration * 24;
            env.Counters.UpdateStats(env.Now);
            env.AssertIntervals(
                12,   // total
                1,   // available
                11,   // unavailable
                false);   // last interval available
            UNIT_ASSERT_VALUES_EQUAL(12, env.MissingIntervals->Val());
        }
    }

    Y_UNIT_TEST(ShouldCatchUpAfterUpdateStall)
    {
        TEnv env;

        // a request hangs and the stats updater stalls for 3 intervals
        auto callContext =
            env.Start(EFileStoreAvailabilityRequestType::GetAttr);
        env.FinishInterval();
        env.AssertIntervals(
            1,   // total
            1,   // available
            0,   // unavailable
            true);   // last interval available

        env.Now += IntervalDuration * 3;
        env.Counters.UpdateStats(env.Now);

        // the request remained outstanding throughout all 3 intervals
        env.AssertIntervals(
            4,   // total
            1,   // available
            3,   // unavailable
            false);   // last interval available

        env.CompleteOk(callContext);
        env.FinishInterval();
        env.AssertIntervals(
            5,   // total
            2,   // available
            3,   // unavailable
            true);   // last interval available
    }

    Y_UNIT_TEST(ShouldCountRequestStartedAndFailedWithinOneInterval)
    {
        TEnv env;

        // an EIO completion of a request that both started and finished
        // within the interval still makes the interval unavailable
        auto callContext =
            env.Start(EFileStoreAvailabilityRequestType::MkDir);
        env.AdvanceWithinInterval(TDuration::Seconds(30));
        env.CompleteWithErrno(callContext, EIO);

        env.FinishInterval();
        env.AssertIntervals(
            1,   // total
            0,   // available
            1,   // unavailable
            false);   // last interval available
    }

    Y_UNIT_TEST(ShouldAccountRequestTypesIndependently)
    {
        // EFileStoreRequest maps each of these request pairs onto one
        // backend request type; the SLA accounts them independently, so a
        // request type failing with EIO makes the interval unavailable even
        // when its backend sibling succeeds in the same interval. (That the
        // FUSE callbacks assign these enums is a property of the CALL table
        // in loop.cpp and is not covered here.)

        {
            // lookup and getattr both map to GetNodeAttr
            TEnv env;
            auto bad = env.Start(EFileStoreAvailabilityRequestType::Lookup);
            env.CompleteWithErrno(bad, EIO);
            auto good = env.Start(EFileStoreAvailabilityRequestType::GetAttr);
            env.CompleteOk(good);

            env.FinishInterval();
            env.AssertIntervals(
                1,   // total
                0,   // available
                1,   // unavailable
                false);   // last interval available
            env.AssertRequestIntervals(
                EFileStoreAvailabilityRequestType::Lookup,
                0,   // available
                1,   // unavailable
                false);   // last interval available
            env.AssertRequestIntervals(
                EFileStoreAvailabilityRequestType::GetAttr,
                1,   // available
                0,   // unavailable
                true);   // last interval available
        }

        {
            // open and create both map to CreateHandle
            TEnv env;
            auto bad = env.Start(EFileStoreAvailabilityRequestType::Open);
            env.CompleteWithErrno(bad, EIO);
            auto good = env.Start(EFileStoreAvailabilityRequestType::Create);
            env.CompleteOk(good);

            env.FinishInterval();
            env.AssertIntervals(
                1,   // total
                0,   // available
                1,   // unavailable
                false);   // last interval available
            env.AssertRequestIntervals(
                EFileStoreAvailabilityRequestType::Open,
                0,   // available
                1,   // unavailable
                false);   // last interval available
            env.AssertRequestIntervals(
                EFileStoreAvailabilityRequestType::Create,
                1,   // available
                0,   // unavailable
                true);   // last interval available
        }

        {
            // mkdir and symlink both map to CreateNode
            TEnv env;
            auto bad = env.Start(EFileStoreAvailabilityRequestType::MkDir);
            env.CompleteWithErrno(bad, EIO);
            auto good = env.Start(EFileStoreAvailabilityRequestType::SymLink);
            env.CompleteOk(good);

            env.FinishInterval();
            env.AssertIntervals(
                1,   // total
                0,   // available
                1,   // unavailable
                false);   // last interval available
            env.AssertRequestIntervals(
                EFileStoreAvailabilityRequestType::MkDir,
                0,   // available
                1,   // unavailable
                false);   // last interval available
            env.AssertRequestIntervals(
                EFileStoreAvailabilityRequestType::SymLink,
                1,   // available
                0,   // unavailable
                true);   // last interval available
        }

        {
            // write and write_buf both map to WriteData
            TEnv env;
            auto bad = env.Start(EFileStoreAvailabilityRequestType::Write);
            env.CompleteWithErrno(bad, EIO);
            auto good =
                env.Start(EFileStoreAvailabilityRequestType::WriteBuf);
            env.CompleteOk(good);

            env.FinishInterval();
            env.AssertIntervals(
                1,   // total
                0,   // available
                1,   // unavailable
                false);   // last interval available
            env.AssertRequestIntervals(
                EFileStoreAvailabilityRequestType::Write,
                0,   // available
                1,   // unavailable
                false);   // last interval available
            env.AssertRequestIntervals(
                EFileStoreAvailabilityRequestType::WriteBuf,
                1,   // available
                0,   // unavailable
                true);   // last interval available
        }
    }

    Y_UNIT_TEST(ShouldSupportCallContextReuse)
    {
        TEnv env;

        // Pins the contract for a call context that is completed and then
        // re-registered - the shape a vfs-layer retry would take. The hooks
        // are driven directly; no dispatch-level retry path is exercised
        // here.
        auto callContext = env.Start(EFileStoreAvailabilityRequestType::Read);
        env.CompleteWithErrno(callContext, EIO);

        env.Counters.RequestStarted(*callContext);

        // the EIO completion of the first attempt is failure evidence, and
        // the pending restarted attempt is neutral => unavailable
        env.FinishInterval();
        env.AssertIntervals(
            1,   // total
            0,   // available
            1,   // unavailable
            false);   // last interval available

        // the successful completion of the restarted attempt is classified
        // as a success: the re-registration cleared the stale EIO of the
        // first attempt, and the success reply itself - as in production -
        // does not write GuestReplyErrno
        env.CompleteOk(callContext);
        env.FinishInterval();
        env.AssertIntervals(
            2,   // total
            1,   // available
            1,   // unavailable
            true);   // last interval available
    }

    Y_UNIT_TEST(ShouldNotCountRequestStartedAfterBoundaryAsHung)
    {
        TEnv env;

        // the wall clock crosses the interval boundary while the stats
        // updater stalls...
        env.Now += IntervalDuration + TDuration::Seconds(1);

        // ...and a request starts in the gap between the boundary and the
        // late updater tick
        auto context = env.Start(EFileStoreAvailabilityRequestType::Read);

        // the late tick closes the first interval: the request counts as
        // fresh-pending (neutral) there, not hung
        env.Counters.UpdateStats(env.Now);
        env.AssertIntervals(
            1,   // total
            1,   // available
            0,   // unavailable
            true);   // last interval available

        env.CompleteOk(context);
        env.FinishInterval();
        env.AssertIntervals(
            2,   // total
            2,   // available
            0,   // unavailable
            true);   // last interval available
    }

    Y_UNIT_TEST(ShouldAccountActivityDuringUpdaterStall)
    {
        TEnv env;

        // a request outstanding from before the stall
        auto hungContext = env.Start(EFileStoreAvailabilityRequestType::Read);
        env.FinishInterval();
        env.AssertIntervals(
            1,   // total
            1,   // available
            0,   // unavailable
            true);   // last interval available

        // the updater stalls for three intervals; requests keep starting
        // and completing meanwhile
        auto eioContext = env.Start(EFileStoreAvailabilityRequestType::Read);
        env.CompleteWithErrno(eioContext, EIO);
        auto okContext = env.Start(EFileStoreAvailabilityRequestType::Read);
        env.CompleteOk(okContext);

        env.Now += IntervalDuration * 3;
        env.Counters.UpdateStats(env.Now);

        // Catch-up: the first stalled interval sees all the evidence
        // accumulated during the stall, and the successful completion makes
        // it available despite the EIO completion and the old hung request
        // (any success => available). The remaining two stalled intervals
        // see only the still-outstanding old request => hung =>
        // unavailable.
        env.AssertIntervals(
            4,   // total
            2,   // available
            2,   // unavailable
            false);   // last interval available

        env.CompleteOk(hungContext);
        env.FinishInterval();
        env.AssertIntervals(
            5,   // total
            3,   // available
            2,   // unavailable
            true);   // last interval available
    }

    Y_UNIT_TEST(ShouldEnableConcurrentlyWithRequestProcessing)
    {
        // A sanitizer-oriented test: request processing and the stats
        // updater run while the tracking is being enabled. The hooks must
        // be no-ops until the tracker is published and must never observe
        // a partially initialized one; run under TSAN to verify.
        auto counterGroup = MakeIntrusive<NMonitoring::TDynamicCounters>();
        TAvailabilityCounters counters;

        std::atomic<bool> stop = false;

        std::thread updater([&] {
            TInstant now = TInstant::Hours(100);
            while (!stop.load()) {
                counters.UpdateStats(now);
                now += TDuration::Seconds(1);
            }
        });

        std::thread requester([&] {
            while (!stop.load()) {
                auto context = MakeIntrusive<TCallContext>();
                context->AvailabilityRequestType =
                    EFileStoreAvailabilityRequestType::Read;
                counters.RequestStarted(*context);
                counters.RequestCompleted(*context);
            }
        });

        counters.EnableAndRegister(IntervalDuration, *counterGroup);

        stop = true;
        updater.join();
        requester.join();
    }

    Y_UNIT_TEST(ShouldPublishPerRequestTypeCounters)
    {
        TEnv env;

        // the "request" label value is the lower-case FUSE request name as
        // listed in the SLA
        UNIT_ASSERT_VALUES_EQUAL(
            "write",
            TString(GetAvailabilityRequestTypeName(
                EFileStoreAvailabilityRequestType::Write)));
        UNIT_ASSERT_VALUES_EQUAL(
            "write_buf",
            TString(GetAvailabilityRequestTypeName(
                EFileStoreAvailabilityRequestType::WriteBuf)));

        auto badContext = env.Start(EFileStoreAvailabilityRequestType::Write);
        env.CompleteWithErrno(badContext, EIO);

        env.FinishInterval();

        // the failing request type is unavailable in its own counters...
        env.AssertRequestIntervals(
            EFileStoreAvailabilityRequestType::Write,
            0,   // available
            1,   // unavailable
            false);   // last interval available
        // ...while an idle request type is available in the same interval...
        env.AssertRequestIntervals(
            EFileStoreAvailabilityRequestType::Read,
            1,   // available
            0,   // unavailable
            true);   // last interval available
        // ...and the aggregated counters are the AND over the request types
        env.AssertIntervals(
            1,   // total
            0,   // available
            1,   // unavailable
            false);   // last interval available

        auto goodContext =
            env.Start(EFileStoreAvailabilityRequestType::Write);
        env.CompleteOk(goodContext);
        env.FinishInterval();

        env.AssertRequestIntervals(
            EFileStoreAvailabilityRequestType::Write,
            1,   // available
            1,   // unavailable
            true);   // last interval available
        env.AssertRequestIntervals(
            EFileStoreAvailabilityRequestType::Read,
            2,   // available
            0,   // unavailable
            true);   // last interval available
        env.AssertIntervals(
            2,   // total
            1,   // available
            1,   // unavailable
            true);   // last interval available
    }

    Y_UNIT_TEST(ShouldStartAsAvailable)
    {
        TEnv env;

        // before any interval is finished the gauges report available, both
        // aggregated and per request type
        env.AssertIntervals(
            0,   // total
            0,   // available
            0,   // unavailable
            true);   // last interval available
        env.AssertRequestIntervals(
            EFileStoreAvailabilityRequestType::Lookup,
            0,   // available
            0,   // unavailable
            true);   // last interval available
    }
}

}   // namespace NCloud::NFileStore
