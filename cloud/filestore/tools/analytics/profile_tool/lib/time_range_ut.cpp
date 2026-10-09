#include "time_range.h"

#include <library/cpp/testing/unittest/registar.h>

#include <util/system/fstat.h>
#include <util/system/tempfile.h>

#include <chrono>
#include <filesystem>

namespace NCloud::NFileStore::NProfileTool {

namespace {

////////////////////////////////////////////////////////////////////////////////

TInstant Time(ui64 seconds)
{
    return TInstant::Seconds(seconds);
}

void AssertPaths(
    const TVector<TProfileLogFile>& actual,
    const TVector<TString>& expected)
{
    UNIT_ASSERT_VALUES_EQUAL(actual.size(), expected.size());
    for (size_t i = 0; i < expected.size(); ++i) {
        UNIT_ASSERT_VALUES_EQUAL(actual[i].Path, expected[i]);
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TProfileLogTimeRange)
{
    Y_UNIT_TEST(ShouldSelectChainWithoutOpeningFiles)
    {
        // These paths do not exist. Selection must use only supplied metadata.
        const TVector<TProfileLogFile> files = {
            {"/missing/first", Time(1000)},
            {"/missing/second", Time(2000)},
            {"/missing/third", Time(3000)},
            {"/missing/fourth", Time(4000)},
            {"/missing/fifth", Time(5000)}};
        AssertPaths(SelectProfileLogFiles(files, {}, {}),
            {"/missing/first", "/missing/second", "/missing/third",
             "/missing/fourth", "/missing/fifth"});
        AssertPaths(SelectProfileLogFiles(files, {}, Time(699)),
            {"/missing/first"});
        AssertPaths(SelectProfileLogFiles(files, Time(4301), {}),
            {"/missing/fifth"});
        const auto middle = SelectProfileLogFiles(files, Time(2301), Time(3699));
        AssertPaths(middle, {"/missing/third", "/missing/fourth"});
        UNIT_ASSERT_VALUES_EQUAL(*middle[0].EndTime, Time(3000));
        UNIT_ASSERT_VALUES_EQUAL(*middle[1].EndTime, Time(4000));
    }

    Y_UNIT_TEST(ShouldAllowDriftAtFileBoundaries)
    {
        const TVector<TProfileLogFile> files = {
            {"first", Time(1000)}, {"middle", Time(2000)}, {"last", Time(3000)}};
        AssertPaths(SelectProfileLogFiles(files, {}, Time(699)), {"first"});
        AssertPaths(SelectProfileLogFiles(files, {}, Time(700)), {"first", "middle"});
        AssertPaths(SelectProfileLogFiles(files, Time(1300), Time(1699)),
            {"first", "middle"});
        AssertPaths(SelectProfileLogFiles(files, Time(1301), Time(1699)), {"middle"});
        AssertPaths(SelectProfileLogFiles(files, Time(1301), Time(1700)),
            {"middle", "last"});
        AssertPaths(SelectProfileLogFiles(files, Time(2300), {}), {"middle", "last"});
        AssertPaths(SelectProfileLogFiles(files, Time(2301), {}), {"last"});
        AssertPaths(SelectProfileLogFiles(files, Time(4000), Time(1)), {});
        const auto last = SelectProfileLogFiles(files, Time(4000), {});
        UNIT_ASSERT_VALUES_EQUAL(*last[0].EndTime, Time(3000));
        AssertPaths(SelectProfileLogFiles(files, {}, TInstant::Max()),
            {"first", "middle", "last"});
    }

    Y_UNIT_TEST(ShouldAlwaysKeepSingleFile)
    {
        const TVector<TProfileLogFile> files = {{"only", Time(200000)}};
        AssertPaths(SelectProfileLogFiles(files, {}, Time(1)), {"only"});
        AssertPaths(SelectProfileLogFiles(files, Time(300000), {}), {"only"});
        AssertPaths(SelectProfileLogFiles(files, Time(300000), Time(300000)), {"only"});
        AssertPaths(SelectProfileLogFiles(files, Time(300000), Time(1)), {"only"});
        const auto selected = SelectProfileLogFiles(files, {}, {});
        AssertPaths(selected, {"only"});
        UNIT_ASSERT_VALUES_EQUAL(*selected[0].EndTime, Time(200000));
    }

    Y_UNIT_TEST(ShouldRetainFirstFileForEarlierQueries)
    {
        const TVector<TProfileLogFile> files = {
            {"first", Time(200000)}, {"second", Time(300000)}};
        AssertPaths(SelectProfileLogFiles(files, {}, Time(113600)), {"first"});
        AssertPaths(SelectProfileLogFiles(files, {}, Time(113601)), {"first"});
        AssertPaths(SelectProfileLogFiles(files, Time(200000), {}), {"first", "second"});
        AssertPaths(SelectProfileLogFiles(files, Time(200301), {}), {"second"});
    }

    Y_UNIT_TEST(ShouldAllowDriftAcrossEpoch)
    {
        const TVector<TProfileLogFile> files = {
            {"first", Time(100)}, {"second", Time(200)}};
        AssertPaths(SelectProfileLogFiles(files, {}, Time(0)), {"first", "second"});
        AssertPaths(SelectProfileLogFiles(files, Time(0), Time(1)), {"first", "second"});
    }

    Y_UNIT_TEST(ShouldRejectEmptyOrReversedQueryBeforeDrift)
    {
        const TVector<TProfileLogFile> files = {
            {"first", Time(1000)}, {"middle", Time(2000)}, {"last", Time(3000)}};
        AssertPaths(SelectProfileLogFiles(files, Time(1500), Time(1500)), {});
        AssertPaths(SelectProfileLogFiles(files, Time(1501), Time(1500)), {});
        AssertPaths(SelectProfileLogFiles(files, Time(3000), Time(1000)), {});
        AssertPaths(SelectProfileLogFiles({}, {}, {}), {});
    }

    Y_UNIT_TEST(ShouldTreatUnknownBoundsAsUnbounded)
    {
        const TVector<TProfileLogFile> files = {
            {"first", {}}, {"second", {}},
            {"third", Time(1000)}, {"last", Time(2000)}};
        AssertPaths(SelectProfileLogFiles(files, {}, Time(1)),
            {"first", "second", "third"});
        AssertPaths(SelectProfileLogFiles(files, Time(1700), {}),
            {"first", "second", "last"});
        AssertPaths(SelectProfileLogFiles(files, {}, {}),
            {"first", "second", "third", "last"});
        AssertPaths(SelectProfileLogFiles(
            {{"first", {}}, {"last", {}}}, Time(5000), Time(6000)),
            {"first", "last"});
    }

    Y_UNIT_TEST(ShouldHandleEqualEndTimes)
    {
        const TVector<TProfileLogFile> files = {
            {"first", Time(100)}, {"second", Time(100)}, {"last", Time(200)}};
        AssertPaths(SelectProfileLogFiles(files, {}, Time(100)), {"first", "second", "last"});
        AssertPaths(SelectProfileLogFiles(files, Time(100), {}),
            {"first", "second", "last"});
    }

    Y_UNIT_TEST(ShouldPreferFilenameEndAndOtherwiseStatFile)
    {
        TTempFileHandle base;
        // This dated path does not exist: a valid suffix must avoid stat.
        const auto dated = GetProfileLogEndTime(base.Name() + ".2026-09-26T10:25");
        const auto date = TInstant::ParseIso8601("2026-09-26T10:25:00Z");
        UNIT_ASSERT_VALUES_EQUAL(*dated, date);

        for (const auto* suffix: {
                 ".log", ".4", ".2026-13-26T10:25", ".2026-09-26T10:25.extra"})
        {
            TTempFileHandle input(base.Name() + suffix);
            std::filesystem::last_write_time(
                input.Name().c_str(),
                std::filesystem::file_time_type{} + std::chrono::seconds(100) +
                    std::chrono::nanoseconds(123456789));
            const TFileStat stat(input.Name());
            const auto expected = TInstant::Seconds(stat.MTime) +
                TDuration::MicroSeconds(stat.MTimeNSec / 1000);
            const auto end = GetProfileLogEndTime(input.Name());
            UNIT_ASSERT_VALUES_EQUAL(*end, expected);
        }
        UNIT_ASSERT(!GetProfileLogEndTime(base.Name() + ".missing"));
    }
}

}   // namespace NCloud::NFileStore::NProfileTool
