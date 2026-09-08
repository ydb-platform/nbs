#include "stable_read.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud {

namespace {

////////////////////////////////////////////////////////////////////////////////

const TInstant StartTime = TInstant::Seconds(1000000);
const TDuration HoldTime = TDuration::Minutes(30);

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TStableReadTest)
{
    Y_UNIT_TEST(ShouldIgnoreUnchangedContent)
    {
        TStableRead<TString> stableRead;

        UNIT_ASSERT_VALUES_EQUAL(
            EStableReadDecision::Unchanged,
            stableRead.Observe("a", "a", StartTime, HoldTime));
        UNIT_ASSERT(!stableRead.IsPending());
    }

    Y_UNIT_TEST(ShouldApplyContentAfterHoldTime)
    {
        TStableRead<TString> stableRead;

        UNIT_ASSERT_VALUES_EQUAL(
            EStableReadDecision::Wait,
            stableRead.Observe("a", "b", StartTime, HoldTime));
        UNIT_ASSERT(stableRead.IsPending());

        // Reading the same content again does not count until HoldTime has
        // passed since it was first seen, whoever triggers the read.
        UNIT_ASSERT_VALUES_EQUAL(
            EStableReadDecision::Wait,
            stableRead.Observe("a", "b", StartTime + HoldTime / 2, HoldTime));

        UNIT_ASSERT_VALUES_EQUAL(
            EStableReadDecision::Apply,
            stableRead.Observe("a", "b", StartTime + HoldTime, HoldTime));
    }

    Y_UNIT_TEST(ShouldRestartHoldTimeWhenContentChanges)
    {
        TStableRead<TString> stableRead;

        stableRead.Observe("a", "b", StartTime, HoldTime);
        UNIT_ASSERT_VALUES_EQUAL(
            EStableReadDecision::Wait,
            stableRead.Observe("a", "c", StartTime + HoldTime / 2, HoldTime));

        // Only half of HoldTime has passed since "c" was first seen.
        UNIT_ASSERT_VALUES_EQUAL(
            EStableReadDecision::Wait,
            stableRead.Observe("a", "c", StartTime + HoldTime, HoldTime));
        UNIT_ASSERT_VALUES_EQUAL(
            EStableReadDecision::Apply,
            stableRead.Observe(
                "a",
                "c",
                StartTime + HoldTime + HoldTime / 2,
                HoldTime));
    }

    Y_UNIT_TEST(ShouldForgetPendingContentWhenCurrentIsReadAgain)
    {
        TStableRead<TString> stableRead;

        stableRead.Observe("a", "b", StartTime, HoldTime);
        UNIT_ASSERT_VALUES_EQUAL(
            EStableReadDecision::Unchanged,
            stableRead.Observe("a", "a", StartTime + HoldTime / 2, HoldTime));
        UNIT_ASSERT(!stableRead.IsPending());

        // "b" is seen for the first time again.
        UNIT_ASSERT_VALUES_EQUAL(
            EStableReadDecision::Wait,
            stableRead.Observe("a", "b", StartTime + HoldTime, HoldTime));
    }

    Y_UNIT_TEST(ShouldResetPendingContent)
    {
        TStableRead<TString> stableRead;

        stableRead.Observe("a", "b", StartTime, HoldTime);
        stableRead.Reset();
        UNIT_ASSERT(!stableRead.IsPending());

        UNIT_ASSERT_VALUES_EQUAL(
            EStableReadDecision::Wait,
            stableRead.Observe("a", "b", StartTime + HoldTime, HoldTime));
    }

    Y_UNIT_TEST(ShouldKeepPendingContentWhenApplyIsRejected)
    {
        TStableRead<TString> stableRead;

        stableRead.Observe("a", "b", StartTime, HoldTime);
        UNIT_ASSERT_VALUES_EQUAL(
            EStableReadDecision::Apply,
            stableRead.Observe("a", "b", StartTime + HoldTime, HoldTime));

        // The caller failed to apply "b" and keeps "a": "b" stays pending
        // and is reported again on the next read.
        UNIT_ASSERT(stableRead.IsPending());
        UNIT_ASSERT_VALUES_EQUAL(
            EStableReadDecision::Apply,
            stableRead.Observe("a", "b", StartTime + 2 * HoldTime, HoldTime));
    }
}

}   // namespace NCloud
