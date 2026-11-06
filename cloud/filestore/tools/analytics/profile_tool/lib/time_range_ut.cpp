#include "time_range.h"

#include <cloud/filestore/libs/diagnostics/events/profile_events.ev.pb.h>

#include <library/cpp/eventlog/eventlog.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/system/tempfile.h>

namespace NCloud::NFileStore::NProfileTool {

namespace {

////////////////////////////////////////////////////////////////////////////////

TInstant Time(ui64 seconds)
{
    return TInstant::ParseIso8601("2000-01-01T00:00:00Z") +
           TDuration::Seconds(seconds);
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
    Y_UNIT_TEST(ShouldKeepSingleInputAndUnboundedQueries)
    {
        const TVector<TProfileLogFile> single = {
            {"/missing", Time(2000), Time(1000)}};
        AssertPaths(
            SelectProfileLogFiles(single, Time(3000), {}),
            {"/missing"});
        AssertPaths(SelectProfileLogFiles(single, {}, Time(1)), {"/missing"});
        AssertPaths(
            SelectProfileLogFiles(single, Time(3000), Time(1)),
            {"/missing"});
        AssertPaths(
            SelectProfileLogFiles(
                {{"/first", Time(1000)}, {"/last", Time(2000)}},
                {},
                {}),
            {"/first", "/last"});
        AssertPaths(SelectProfileLogFiles({}, Time(1000), Time(2000)), {});
    }

    Y_UNIT_TEST(ShouldSelectUsingCachedBoundsWithoutOpeningFiles)
    {
        const TVector<TProfileLogFile> files = {
            {"/missing/first", Time(2000), Time(1000)},
            {"/missing/last", Time(4000), Time(3000)}};
        AssertPaths(
            SelectProfileLogFiles(files, Time(1500), Time(1600)),
            {"/missing/first"});
    }

    Y_UNIT_TEST(ShouldSelectUsingRecordsRegardlessOfFilename)
    {
        TTempFileHandle base;
        TTempFileHandle first(base.Name() + ".2026-09-26T10:25");
        TTempFileHandle last(base.Name() + ".1970-01-01T00:00");
        for (const auto* input: {&first, &last}) {
            TEventLog log(input->Name(), 0);
            {
                TSelfFlushLogFrame frame(log);
                NProto::TProfileLogRecord record;
                record.AddRequests()->SetTimestampMcs(
                    Time(input == &first ? 1000 : 2000).MicroSeconds());
                frame.LogEvent(record);
            }
            log.CloseLog();
        }
        const TVector<TProfileLogFile> files = {
            GetProfileLogFile(first.Name(), true),
            GetProfileLogFile(last.Name(), true)};
        AssertPaths(
            SelectProfileLogFiles(files, Time(1000), Time(1000)),
            {first.Name()});
        AssertPaths(
            SelectProfileLogFiles(files, Time(1001), {}),
            {last.Name()});
        AssertPaths(
            SelectProfileLogFiles(files, {}, Time(1999)),
            {first.Name()});
        AssertPaths(
            SelectProfileLogFiles(files, Time(2000), {}),
            {last.Name()});
        AssertPaths(SelectProfileLogFiles(files, Time(2001), {}), {});
        AssertPaths(SelectProfileLogFiles(files, {}, Time(999)), {});
        AssertPaths(SelectProfileLogFiles(files, Time(2001), Time(999)), {});
        // A lone file is retained even when its records are outside the query.
        AssertPaths(
            SelectProfileLogFiles({files[0]}, Time(3000), {}),
            {first.Name()});
    }

    Y_UNIT_TEST(ShouldKeepUnknownRecordBoundsRegardlessOfFilename)
    {
        TTempFileHandle base;
        TTempFileHandle empty(base.Name() + ".1970-01-01T00:00");
        TTempFileHandle broken(base.Name() + ".2026-09-26T10:25");
        broken.Write("not a log", 9);
        AssertPaths(
            SelectProfileLogFiles(
                {GetProfileLogFile(empty.Name(), true),
                 GetProfileLogFile(broken.Name(), true)},
                Time(1000),
                Time(2000)),
            {empty.Name(), broken.Name()});
    }

    Y_UNIT_TEST(ShouldNotInferTimesFromFilename)
    {
        TTempFileHandle base;
        const auto file = GetProfileLogFile(base.Name() + ".2026-09-26T10:25");
        UNIT_ASSERT(!file.StartTime);
        UNIT_ASSERT(!file.EndTime);
    }

    Y_UNIT_TEST(ShouldReadBoundaryRecordsRegardlessOfFilename)
    {
        TTempFileHandle base;
        for (const auto* suffix:
             {".log",
              ".4",
              ".2026-09-26T10:25",
              ".2026-13-26T10:25",
              ".2026-09-26T10:25.extra"})
        {
            TTempFileHandle input(base.Name() + suffix);
            TEventLog log(input.Name(), 0);
            for (const auto& timestamps:
                 TVector<TVector<ui64>>{{1000, 900}, {2000}, {3000, 3100}})
            {
                TSelfFlushLogFrame frame(log);
                NProto::TProfileLogRecord record;
                for (const auto timestamp: timestamps) {
                    record.AddRequests()->SetTimestampMcs(
                        Time(timestamp).MicroSeconds());
                }
                frame.LogEvent(record);
                frame.Flush();
            }
            log.CloseLog();
            const auto unprobed = GetProfileLogFile(input.Name());
            UNIT_ASSERT(!unprobed.StartTime);
            UNIT_ASSERT(!unprobed.EndTime);
            const auto file = GetProfileLogFile(input.Name(), true);
            UNIT_ASSERT(file.StartTime);
            UNIT_ASSERT(file.EndTime);
            UNIT_ASSERT_VALUES_EQUAL(*file.StartTime, Time(900));
            UNIT_ASSERT_VALUES_EQUAL(*file.EndTime, Time(3100));

            const auto lastByteOffset = input.GetLength() - 1;
            char lastByte = 0;
            UNIT_ASSERT_VALUES_EQUAL(
                input.Pread(&lastByte, 1, lastByteOffset),
                1);
            const char damagedByte = lastByte ^ 1;
            input.Pwrite(&damagedByte, 1, lastByteOffset);
            const auto damaged = GetProfileLogFile(input.Name(), true);
            UNIT_ASSERT(damaged.StartTime);
            UNIT_ASSERT(damaged.EndTime);
            UNIT_ASSERT_VALUES_EQUAL(*damaged.StartTime, Time(900));
            UNIT_ASSERT_VALUES_EQUAL(*damaged.EndTime, Time(2000));
            input.Pwrite(&lastByte, 1, lastByteOffset);

            input.Resize(input.GetLength() - 1);
            const auto truncated = GetProfileLogFile(input.Name(), true);
            UNIT_ASSERT(truncated.StartTime);
            UNIT_ASSERT(truncated.EndTime);
            UNIT_ASSERT_VALUES_EQUAL(*truncated.StartTime, Time(900));
            UNIT_ASSERT_VALUES_EQUAL(*truncated.EndTime, Time(2000));
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
                record.AddRequests()->SetTimestampMcs(
                    Time(timestamp).MicroSeconds());
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

    Y_UNIT_TEST(ShouldUseAllRecordsInBoundaryFrames)
    {
        TTempFileHandle input;
        TTempFileHandle empty;
        TEventLog log(input.Name(), 0);
        const auto start = TInstant::ParseIso8601("2026-11-03T10:00:00Z");
        const auto end = TInstant::ParseIso8601("2026-11-03T11:00:00Z");
        for (const auto wallTime: {start, end}) {
            TSelfFlushLogFrame frame(log);
            // Filesystem grouping does not order records by request timestamp.
            for (const auto timestamp: {wallTime, TInstant::Seconds(1000)}) {
                NProto::TProfileLogRecord record;
                record.AddRequests()->SetTimestampMcs(timestamp.MicroSeconds());
                frame.LogEvent(record);
            }
            frame.Flush();
        }
        log.CloseLog();
        const auto bounds = GetProfileLogFile(input.Name(), true);
        UNIT_ASSERT(bounds.StartTime);
        UNIT_ASSERT(bounds.EndTime);
        UNIT_ASSERT_VALUES_EQUAL(*bounds.StartTime, start);
        UNIT_ASSERT_VALUES_EQUAL(*bounds.EndTime, end);
        AssertPaths(
            SelectProfileLogFiles(
                {bounds, GetProfileLogFile(empty.Name(), true)},
                end + TDuration::Seconds(1),
                {}),
            {empty.Name()});
        AssertPaths(
            SelectProfileLogFiles(
                {bounds, GetProfileLogFile(empty.Name(), true)},
                start,
                end),
            {input.Name(), empty.Name()});
    }

    Y_UNIT_TEST(ShouldIncludeBothBoundaryFramesInBounds)
    {
        for (const bool reverse: {false, true}) {
            TTempFileHandle input;
            TEventLog log(input.Name(), 0);
            TVector<TVector<ui64>> timestamps = {{10, 1000}, {20}};
            if (reverse) {
                std::swap(timestamps[0], timestamps[1]);
            }
            for (const auto& frameTimes: timestamps) {
                TSelfFlushLogFrame frame(log);
                NProto::TProfileLogRecord record;
                for (const auto timestamp: frameTimes) {
                    record.AddRequests()->SetTimestampMcs(Time(timestamp).MicroSeconds());
                }
                frame.LogEvent(record);
                frame.Flush();
            }
            log.CloseLog();
            const auto bounds = GetProfileLogFile(input.Name(), true);
            UNIT_ASSERT(bounds.StartTime);
            UNIT_ASSERT(bounds.EndTime);
            UNIT_ASSERT_VALUES_EQUAL(*bounds.StartTime, Time(10));
            UNIT_ASSERT_VALUES_EQUAL(*bounds.EndTime, Time(1000));
            const TVector<TProfileLogFile> files = {
                bounds, {"/outside", Time(2000), Time(2000)}};
            AssertPaths(SelectProfileLogFiles(files, Time(999), Time(1001)),
                {input.Name()});
            AssertPaths(SelectProfileLogFiles(files, Time(10), Time(11)),
                {input.Name()});
        }
    }

    Y_UNIT_TEST(ShouldIgnoreTimestampsBeforeYear2000)
    {
        const auto cutoff = Time(0);
        for (const bool includeValid: {false, true}) {
            TTempFileHandle input;
            TEventLog log(input.Name(), 0);
            {
                TSelfFlushLogFrame frame(log);
                NProto::TProfileLogRecord record;
                record.AddRequests()->SetTimestampMcs(0);
                record.AddRequests()->SetTimestampMcs(cutoff.MicroSeconds() - 1);
                if (includeValid) {
                    record.AddRequests()->SetTimestampMcs(cutoff.MicroSeconds());
                }
                frame.LogEvent(record);
                frame.Flush();
            }
            log.CloseLog();
            const auto bounds = GetProfileLogFile(input.Name(), true);
            UNIT_ASSERT_VALUES_EQUAL(bounds.StartTime.Defined(), includeValid);
            UNIT_ASSERT_VALUES_EQUAL(bounds.EndTime.Defined(), includeValid);
            if (includeValid) {
                UNIT_ASSERT_VALUES_EQUAL(*bounds.StartTime, cutoff);
                UNIT_ASSERT_VALUES_EQUAL(*bounds.EndTime, cutoff);
            }
        }
    }

    Y_UNIT_TEST(ShouldKeepEmptyAndUnreadableBoundsUnknown)
    {
        TTempFileHandle input;
        UNIT_ASSERT(!GetProfileLogFile(input.Name(), true).EndTime);
        input.Write("not a log", 9);
        UNIT_ASSERT(!GetProfileLogFile(input.Name(), true).EndTime);
        UNIT_ASSERT(
            !GetProfileLogFile(input.Name() + ".missing", true).EndTime);
    }
}

}   // namespace NCloud::NFileStore::NProfileTool
