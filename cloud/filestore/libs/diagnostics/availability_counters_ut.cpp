#include "availability_counters.h"

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/ptr.h>

#include <cerrno>

namespace NCloud::NFileStore {

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr TDuration Interval = TAvailabilityCounters::DefaultIntervalDuration;

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
        Counters.Register(*CounterGroup);
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
        Now += Interval;
        Counters.UpdateStats(Now);
    }

    void AdvanceWithinInterval(TDuration duration)
    {
        Now += duration;
        UNIT_ASSERT(duration < Interval);
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
        env.AssertIntervals(1, 1, 0, true);

        env.FinishInterval();
        env.AssertIntervals(2, 2, 0, true);
    }

    Y_UNIT_TEST(ShouldReportSuccessfulRequestsAsAvailable)
    {
        TEnv env;

        auto callContext = env.Start(EFileStoreAvailabilityRequestType::Read);
        env.CompleteOk(callContext);

        env.FinishInterval();
        env.AssertIntervals(1, 1, 0, true);
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
        env.AssertIntervals(1, 1, 0, true);
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
        env.AssertIntervals(1, 0, 1, false);

        // the next interval has no requests => available again
        env.FinishInterval();
        env.AssertIntervals(2, 1, 1, true);
    }

    Y_UNIT_TEST(ShouldReportMixedEioAndSuccessAsAvailable)
    {
        TEnv env;

        auto bad = env.Start(EFileStoreAvailabilityRequestType::Write);
        env.CompleteWithErrno(bad, EIO);

        auto good = env.Start(EFileStoreAvailabilityRequestType::Write);
        env.CompleteOk(good);

        env.FinishInterval();
        env.AssertIntervals(1, 1, 0, true);
    }

    Y_UNIT_TEST(ShouldReportHungRequestAsUnavailable)
    {
        TEnv env;

        // the request is started in interval 1...
        auto callContext = env.Start(EFileStoreAvailabilityRequestType::Fsync);

        // ...so it is not hung during interval 1 (it has not been
        // outstanding throughout the entire interval)
        env.FinishInterval();
        env.AssertIntervals(1, 1, 0, true);

        // it remains outstanding throughout intervals 2 and 3 => hung
        env.FinishInterval();
        env.AssertIntervals(2, 1, 1, false);
        env.FinishInterval();
        env.AssertIntervals(3, 1, 2, false);

        // once it completes successfully, the interval becomes available
        env.CompleteOk(callContext);
        env.FinishInterval();
        env.AssertIntervals(4, 2, 2, true);
    }

    Y_UNIT_TEST(ShouldTreatLateNonEioCompletionOfOldRequestAsAvailable)
    {
        TEnv env;

        auto callContext = env.Start(EFileStoreAvailabilityRequestType::Open);
        env.FinishInterval();
        env.AssertIntervals(1, 1, 0, true);

        // the request was outstanding at the interval start and completed
        // with a non-EIO outcome during the interval => available
        env.AdvanceWithinInterval(TDuration::Minutes(1));
        env.CompleteOk(callContext);
        env.FinishInterval();
        env.AssertIntervals(2, 2, 0, true);
    }

    Y_UNIT_TEST(ShouldTreatLateEioCompletionOfOldRequestAsUnavailable)
    {
        TEnv env;

        auto callContext = env.Start(EFileStoreAvailabilityRequestType::Open);
        env.FinishInterval();
        env.AssertIntervals(1, 1, 0, true);

        // the only outstanding request of this type completed with EIO
        // during the interval => unavailable
        env.CompleteWithErrno(callContext, EIO);
        env.FinishInterval();
        env.AssertIntervals(2, 1, 1, false);
    }

    Y_UNIT_TEST(ShouldNotCountPendingRequestsAsAvailable)
    {
        TEnv env;

        // a request started during the interval and still outstanding at its
        // end is neutral: alone it does not make the interval unavailable...
        auto hungContext = env.Start(EFileStoreAvailabilityRequestType::Read);
        env.FinishInterval();
        env.AssertIntervals(1, 1, 0, true);

        // ...but it is not success evidence either: it does not make an
        // interval with a hung request available
        env.AdvanceWithinInterval(TDuration::Minutes(2));
        auto freshContext =
            env.Start(EFileStoreAvailabilityRequestType::Read);

        env.FinishInterval();
        env.AssertIntervals(2, 1, 1, false);

        // in the next interval both requests are hung
        env.FinishInterval();
        env.AssertIntervals(3, 1, 2, false);

        env.CompleteOk(hungContext);
        env.CompleteOk(freshContext);
        env.FinishInterval();
        env.AssertIntervals(4, 2, 2, true);
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
        env.AssertIntervals(1, 0, 1, false);

        env.CompleteOk(pendingContext);
        env.FinishInterval();
        env.AssertIntervals(2, 1, 1, true);
    }

    Y_UNIT_TEST(ShouldDistinguishOldAndFreshRequestsOnCompletion)
    {
        TEnv env;

        // An old hung request together with a fresh EIO completion: every
        // outstanding request is either hung or completed with EIO =>
        // unavailable. This requires correctly attributing the completion to
        // the fresh request via the interval sequence number.
        auto hungContext = env.Start(EFileStoreAvailabilityRequestType::Read);
        env.FinishInterval();
        env.AssertIntervals(1, 1, 0, true);

        auto freshContext =
            env.Start(EFileStoreAvailabilityRequestType::Read);
        env.CompleteWithErrno(freshContext, EIO);

        env.FinishInterval();
        env.AssertIntervals(2, 1, 1, false);

        env.CompleteOk(hungContext);
        env.FinishInterval();
        env.AssertIntervals(3, 2, 1, true);
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
        env.AssertIntervals(1, 0, 1, false);
    }

    Y_UNIT_TEST(ShouldNotTrackUntrackedRequestTypes)
    {
        TEnv env;

        // requests outside the availability SLA - e.g. mknod, access, xattr
        // and lock requests - get no availability request type at the FUSE
        // dispatch and do not affect the metric even if they fail with EIO
        // or hang
        auto eioContext =
            env.Start(EFileStoreAvailabilityRequestType::None);
        env.CompleteWithErrno(eioContext, EIO);
        // never completed
        env.Start(EFileStoreAvailabilityRequestType::None);

        env.FinishInterval();
        env.AssertIntervals(1, 1, 0, true);

        env.FinishInterval();
        env.AssertIntervals(2, 2, 0, true);
    }

    Y_UNIT_TEST(ShouldNotFinishIntervalBeforeItsEnd)
    {
        TEnv env;

        env.AdvanceWithinInterval(TDuration::Minutes(4));
        env.AssertIntervals(0, 0, 0, true);

        env.AdvanceWithinInterval(
            TDuration::Minutes(1) - TDuration::Seconds(1));
        env.AssertIntervals(0, 0, 0, true);

        env.Now += TDuration::Seconds(1);
        env.Counters.UpdateStats(env.Now);
        env.AssertIntervals(1, 1, 0, true);
    }

    Y_UNIT_TEST(ShouldCountIntervalsMissedBeyondCatchUpLimit)
    {
        // exactly at the catch-up limit every elapsed interval is evaluated
        {
            TEnv env;
            env.Now += Interval * 12;
            env.Counters.UpdateStats(env.Now);
            env.AssertIntervals(12, 12, 0, true);
            UNIT_ASSERT_VALUES_EQUAL(0, env.MissingIntervals->Val());
        }

        // one interval beyond the limit is reported as missing, not dropped
        {
            TEnv env;
            env.Now += Interval * 13;
            env.Counters.UpdateStats(env.Now);
            env.AssertIntervals(12, 12, 0, true);
            UNIT_ASSERT_VALUES_EQUAL(1, env.MissingIntervals->Val());
        }

        // a two-hour gap: half evaluated, half reported as missing
        {
            TEnv env;
            env.Now += Interval * 24;
            env.Counters.UpdateStats(env.Now);
            env.AssertIntervals(12, 12, 0, true);
            UNIT_ASSERT_VALUES_EQUAL(12, env.MissingIntervals->Val());

            // measurement continues normally afterwards
            env.FinishInterval();
            env.AssertIntervals(13, 13, 0, true);
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
            env.Now += Interval * 12;
            env.Counters.UpdateStats(env.Now);
            env.AssertIntervals(12, 1, 11, false);
            UNIT_ASSERT_VALUES_EQUAL(0, env.MissingIntervals->Val());
        }

        {
            TEnv env;
            env.Start(EFileStoreAvailabilityRequestType::Read);
            env.Now += Interval * 13;
            env.Counters.UpdateStats(env.Now);
            env.AssertIntervals(12, 1, 11, false);
            UNIT_ASSERT_VALUES_EQUAL(1, env.MissingIntervals->Val());
        }

        {
            TEnv env;
            env.Start(EFileStoreAvailabilityRequestType::Read);
            env.Now += Interval * 24;
            env.Counters.UpdateStats(env.Now);
            env.AssertIntervals(12, 1, 11, false);
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
        env.AssertIntervals(1, 1, 0, true);

        env.Now += Interval * 3;
        env.Counters.UpdateStats(env.Now);

        // the request remained outstanding throughout all 3 intervals
        env.AssertIntervals(4, 1, 3, false);

        env.CompleteOk(callContext);
        env.FinishInterval();
        env.AssertIntervals(5, 2, 3, true);
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
        env.AssertIntervals(1, 0, 1, false);
    }

    Y_UNIT_TEST(ShouldAccountFuseRequestTypesIndependently)
    {
        // EFileStoreRequest maps each of these FUSE request pairs onto one
        // backend request type; the SLA accounts them independently, so a
        // FUSE request type failing with EIO makes the interval unavailable
        // even when its backend sibling succeeds in the same interval

        {
            // lookup and getattr both map to GetNodeAttr
            TEnv env;
            auto bad = env.Start(EFileStoreAvailabilityRequestType::Lookup);
            env.CompleteWithErrno(bad, EIO);
            auto good = env.Start(EFileStoreAvailabilityRequestType::GetAttr);
            env.CompleteOk(good);

            env.FinishInterval();
            env.AssertIntervals(1, 0, 1, false);
            env.AssertRequestIntervals(
                EFileStoreAvailabilityRequestType::Lookup, 0, 1, false);
            env.AssertRequestIntervals(
                EFileStoreAvailabilityRequestType::GetAttr, 1, 0, true);
        }

        {
            // open and create both map to CreateHandle
            TEnv env;
            auto bad = env.Start(EFileStoreAvailabilityRequestType::Open);
            env.CompleteWithErrno(bad, EIO);
            auto good = env.Start(EFileStoreAvailabilityRequestType::Create);
            env.CompleteOk(good);

            env.FinishInterval();
            env.AssertIntervals(1, 0, 1, false);
        }

        {
            // mkdir and symlink both map to CreateNode
            TEnv env;
            auto bad = env.Start(EFileStoreAvailabilityRequestType::MkDir);
            env.CompleteWithErrno(bad, EIO);
            auto good = env.Start(EFileStoreAvailabilityRequestType::SymLink);
            env.CompleteOk(good);

            env.FinishInterval();
            env.AssertIntervals(1, 0, 1, false);
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
            env.AssertIntervals(1, 0, 1, false);
        }
    }

    Y_UNIT_TEST(ShouldSupportRestartedRequests)
    {
        TEnv env;

        // some request types are retried by the vfs layer by completing and
        // re-starting the same call context
        auto callContext = env.Start(EFileStoreAvailabilityRequestType::Read);
        env.CompleteWithErrno(callContext, EIO);

        env.Counters.RequestStarted(*callContext);

        // the EIO completion of the first attempt is failure evidence, and
        // the pending restarted attempt is neutral => unavailable
        env.FinishInterval();
        env.AssertIntervals(1, 0, 1, false);

        // the successful completion of the restarted attempt is classified
        // as a success: the re-registration cleared the stale EIO of the
        // first attempt, and the success reply itself - as in production -
        // does not write GuestReplyErrno
        env.CompleteOk(callContext);
        env.FinishInterval();
        env.AssertIntervals(2, 1, 1, true);
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
            EFileStoreAvailabilityRequestType::Write, 0, 1, false);
        // ...while an idle request type is available in the same interval...
        env.AssertRequestIntervals(
            EFileStoreAvailabilityRequestType::Read, 1, 0, true);
        // ...and the aggregated counters are the AND over the request types
        env.AssertIntervals(1, 0, 1, false);

        auto goodContext =
            env.Start(EFileStoreAvailabilityRequestType::Write);
        env.CompleteOk(goodContext);
        env.FinishInterval();

        env.AssertRequestIntervals(
            EFileStoreAvailabilityRequestType::Write, 1, 1, true);
        env.AssertRequestIntervals(
            EFileStoreAvailabilityRequestType::Read, 2, 0, true);
        env.AssertIntervals(2, 1, 1, true);
    }

    Y_UNIT_TEST(ShouldStartAsAvailable)
    {
        TEnv env;

        // before any interval is finished the gauges report available, both
        // aggregated and per request type
        env.AssertIntervals(0, 0, 0, true);
        env.AssertRequestIntervals(
            EFileStoreAvailabilityRequestType::Lookup, 0, 0, true);
    }
}

}   // namespace NCloud::NFileStore
