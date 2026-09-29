#include "stable_read.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TStableReadTest)
{
    Y_UNIT_TEST(ShouldIgnoreUnchangedContent)
    {
        TStableRead<TString> stableRead;

        UNIT_ASSERT_VALUES_EQUAL(
            EStableReadDecision::Unchanged,
            stableRead.Observe("a", "a"));
        UNIT_ASSERT(!stableRead.IsPending());
    }

    Y_UNIT_TEST(ShouldApplyContentReadTwiceInARow)
    {
        TStableRead<TString> stableRead;

        UNIT_ASSERT_VALUES_EQUAL(
            EStableReadDecision::Wait,
            stableRead.Observe("a", "b"));
        UNIT_ASSERT(stableRead.IsPending());

        UNIT_ASSERT_VALUES_EQUAL(
            EStableReadDecision::Apply,
            stableRead.Observe("a", "b"));
    }

    Y_UNIT_TEST(ShouldRestartWhenContentChanges)
    {
        TStableRead<TString> stableRead;

        stableRead.Observe("a", "b");
        UNIT_ASSERT_VALUES_EQUAL(
            EStableReadDecision::Wait,
            stableRead.Observe("a", "c"));
        UNIT_ASSERT_VALUES_EQUAL(
            EStableReadDecision::Apply,
            stableRead.Observe("a", "c"));
    }

    Y_UNIT_TEST(ShouldForgetPendingContentWhenCurrentIsReadAgain)
    {
        TStableRead<TString> stableRead;

        stableRead.Observe("a", "b");
        UNIT_ASSERT_VALUES_EQUAL(
            EStableReadDecision::Unchanged,
            stableRead.Observe("a", "a"));
        UNIT_ASSERT(!stableRead.IsPending());

        // "b" is seen for the first time again.
        UNIT_ASSERT_VALUES_EQUAL(
            EStableReadDecision::Wait,
            stableRead.Observe("a", "b"));
    }

    Y_UNIT_TEST(ShouldResetPendingContent)
    {
        TStableRead<TString> stableRead;

        stableRead.Observe("a", "b");
        stableRead.Reset();
        UNIT_ASSERT(!stableRead.IsPending());

        UNIT_ASSERT_VALUES_EQUAL(
            EStableReadDecision::Wait,
            stableRead.Observe("a", "b"));
    }

    Y_UNIT_TEST(ShouldKeepPendingContentWhenApplyIsRejected)
    {
        TStableRead<TString> stableRead;

        stableRead.Observe("a", "b");
        UNIT_ASSERT_VALUES_EQUAL(
            EStableReadDecision::Apply,
            stableRead.Observe("a", "b"));

        // The caller failed to apply "b" and keeps "a": "b" stays pending
        // and is reported again on the next read.
        UNIT_ASSERT(stableRead.IsPending());
        UNIT_ASSERT_VALUES_EQUAL(
            EStableReadDecision::Apply,
            stableRead.Observe("a", "b"));
    }
}

}   // namespace NCloud
