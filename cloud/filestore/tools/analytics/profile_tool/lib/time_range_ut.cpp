#include "time_range.h"

#include <library/cpp/testing/unittest/registar.h>

#include <cloud/filestore/libs/diagnostics/events/profile_events.ev.pb.h>
#include <library/cpp/eventlog/eventlog.h>
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

    Y_UNIT_TEST(ShouldPreferFilenameEndWithoutOpeningFile)
    {
        TTempFileHandle base;
        const auto file = GetProfileLogFile(base.Name() + ".2026-09-26T10:25");
        UNIT_ASSERT(!file.StartTime);
        UNIT_ASSERT_VALUES_EQUAL(
            *file.EndTime, TInstant::ParseIso8601("2026-09-26T10:25:00Z"));
    }

    Y_UNIT_TEST(ShouldReadBoundaryRecordsInsteadOfMtime)
    {
        TTempFileHandle base;
        for (const auto* suffix: {
                 ".log", ".4", ".2026-13-26T10:25", ".2026-09-26T10:25.extra"})
        {
            TTempFileHandle input(base.Name() + suffix);
            TEventLog log(input.Name(), 0);
            for (const auto& timestamps: TVector<TVector<ui64>>{
                     {1000, 900}, {2000}, {3000, 3100}})
            {
                TSelfFlushLogFrame frame(log);
                NProto::TProfileLogRecord record;
                for (const auto timestamp: timestamps) {
                    record.AddRequests()->SetTimestampMcs(Time(timestamp).MicroSeconds());
                }
                frame.LogEvent(record);
                frame.Flush();
            }
            log.CloseLog();
            std::filesystem::last_write_time(
                input.Name().c_str(),
                std::filesystem::file_time_type{} + std::chrono::seconds(100));
            const auto unprobed = GetProfileLogFile(input.Name());
            UNIT_ASSERT(!unprobed.StartTime);
            UNIT_ASSERT(!unprobed.EndTime);
            const auto file = GetProfileLogFile(input.Name(), true);
            UNIT_ASSERT(file.StartTime);
            UNIT_ASSERT(file.EndTime);
            UNIT_ASSERT_VALUES_EQUAL(*file.StartTime, Time(900));
            UNIT_ASSERT_VALUES_EQUAL(*file.EndTime, Time(3100));

            input.Resize(input.GetLength() - 1);
            const auto truncated = GetProfileLogFile(input.Name(), true);
            UNIT_ASSERT(!truncated.StartTime);
            UNIT_ASSERT(!truncated.EndTime);
        }
    }

    Y_UNIT_TEST(ShouldReadFirstAndLastRecordWithinOneFrame)
    {
        TTempFileHandle input;
        TEventLog log(input.Name(), 0);
        {
            TSelfFlushLogFrame frame(log);
            frame.LogEvent(NProto::TProfileLogRecord{});
            for (const auto timestamp: {1000, 2000, 3000}) {
                NProto::TProfileLogRecord record;
                record.AddRequests()->SetTimestampMcs(Time(timestamp).MicroSeconds());
                frame.LogEvent(record);
            }
            frame.LogEvent(NProto::TProfileLogRecord{});
            frame.Flush();
        }
        log.CloseLog();
        const auto file = GetProfileLogFile(input.Name(), true);
        UNIT_ASSERT(file.StartTime);
        UNIT_ASSERT(file.EndTime);
        UNIT_ASSERT_VALUES_EQUAL(*file.StartTime, Time(1000));
        UNIT_ASSERT_VALUES_EQUAL(*file.EndTime, Time(3000));
    }

    Y_UNIT_TEST(ShouldKeepEmptyAndUnreadableBoundsUnknown)
    {
        TTempFileHandle input;
        UNIT_ASSERT(!GetProfileLogFile(input.Name(), true).EndTime);
        input.Write("not a log", 9);
        UNIT_ASSERT(!GetProfileLogFile(input.Name(), true).EndTime);
        UNIT_ASSERT(!GetProfileLogFile(input.Name() + ".missing", true).EndTime);
    }

    Y_UNIT_TEST(ShouldUseDetectedStartAndEndForPruning)
    {
        const TVector<TProfileLogFile> files = {
            {"first", Time(2000), Time(1000)},
            {"last", Time(6000), Time(5000)}};
        AssertPaths(SelectProfileLogFiles(files, Time(3001), Time(3999)), {});
        AssertPaths(SelectProfileLogFiles(files, Time(100), Time(699)), {});
        AssertPaths(SelectProfileLogFiles(files, Time(6301), {}), {});
        AssertPaths(SelectProfileLogFiles(files, Time(1500), Time(1600)), {"first"});
    }
}

}   // namespace NCloud::NFileStore::NProfileTool
