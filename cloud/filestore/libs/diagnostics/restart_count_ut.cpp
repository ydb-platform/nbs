#include "restart_count.h"

#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/folder/tempdir.h>
#include <util/stream/file.h>

namespace NCloud::NFileStore {

using namespace NMonitoring;

namespace {

////////////////////////////////////////////////////////////////////////////////

TDynamicCountersPtr CreateCounters()
{
    return MakeIntrusive<TDynamicCounters>();
}

TDynamicCounters::TCounterPtr GetRestartsCountCounter(
    const TDynamicCountersPtr& counters)
{
    return counters->GetSubgroup("counters", "utils")
        ->GetCounter("RestartsCount", false);
}

TString ReadFile(const TFsPath& path)
{
    return TFileInput(path).ReadAll();
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TRestartCountTest)
{
    Y_UNIT_TEST(ShouldDoNothingForEmptyPath)
    {
        auto counters = CreateCounters();
        auto counter = GetRestartsCountCounter(counters);
        *counter = 42;

        PublishRestartsCount("", counters);

        UNIT_ASSERT_VALUES_EQUAL(42, counter->GetAtomic());
    }

    Y_UNIT_TEST(ShouldPublishZeroAndWriteOneForMissingFile)
    {
        const TTempDir tempDir;
        const TFsPath restartsCountFile = tempDir.Path() / "restarts_count";
        auto counters = CreateCounters();

        PublishRestartsCount(restartsCountFile, counters);

        auto counter = GetRestartsCountCounter(counters);
        UNIT_ASSERT_VALUES_EQUAL(0, counter->GetAtomic());
        UNIT_ASSERT_VALUES_EQUAL("1", ReadFile(restartsCountFile));
    }

    Y_UNIT_TEST(ShouldPublishCurrentValueAndWriteIncrementedValue)
    {
        const TTempDir tempDir;
        const TFsPath restartsCountFile = tempDir.Path() / "restarts_count";
        TFileOutput(restartsCountFile).Write("7");
        auto counters = CreateCounters();

        PublishRestartsCount(restartsCountFile, counters);

        auto counter = GetRestartsCountCounter(counters);
        UNIT_ASSERT_VALUES_EQUAL(7, counter->GetAtomic());
        UNIT_ASSERT_VALUES_EQUAL("8", ReadFile(restartsCountFile));
    }

    Y_UNIT_TEST(ShouldPublishZeroAndWriteOneForMalformedContent)
    {
        const TTempDir tempDir;
        const TFsPath restartsCountFile = tempDir.Path() / "restarts_count";
        TFileOutput(restartsCountFile).Write("not a number");
        auto counters = CreateCounters();

        PublishRestartsCount(restartsCountFile, counters);

        auto counter = GetRestartsCountCounter(counters);
        UNIT_ASSERT_VALUES_EQUAL(0, counter->GetAtomic());
        UNIT_ASSERT_VALUES_EQUAL("1", ReadFile(restartsCountFile));
    }
}

}   // namespace NCloud::NFileStore
