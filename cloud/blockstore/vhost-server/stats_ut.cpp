#include "stats.h"
#include "critical_event.h"

#include <library/cpp/json/json_reader.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/datetime/cputimer.h>
#include <util/generic/size_literals.h>
#include <util/stream/str.h>

#include <chrono>
#include <tuple>

using namespace std::chrono_literals;
using namespace NCloud::NBlockStore::NVHostServer;

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TStatsTest)
{
    Y_UNIT_TEST(ShouldDumpStats)
    {
        constexpr ui64 cyclesPerSecond = 2000000000;
        SetCyclesPerSecond(cyclesPerSecond);

        TSimpleStats prevStats;
        TSimpleStats completionStats;
        std::array<TAtomicStats, 2> queueStats;

        auto dump = [&] (auto dt) {
            TStringStream ss;

            auto curStats = TCompleteStats{
                .SimpleStats = completionStats,
                .CriticalEvents = TakeAccumulatedCriticalEvents()};
            for (auto& s: queueStats) {
                curStats.SimpleStats += s;
            }

            DumpStats(curStats, prevStats, dt, ss, GetCyclesPerMillisecond());

            NJson::TJsonValue stats;
            NJson::ReadJsonTree(ss.Str(), &stats, true);

            return stats;
        };

        auto completed = [&] (int req, ui64 size, TDuration dt) {
            queueStats[req].Dequeued += 1;
            queueStats[req].Submitted += 1;

            completionStats.Completed += 1;
            completionStats.Requests[req].Count += 1;
            completionStats.Requests[req].Bytes += size;
            completionStats.Times[req].Increment(DurationToCycles(dt));
            completionStats.Sizes[req].Increment(size);
        };

        auto read = [&] (ui64 size, auto dt) {
            completed(0, size, dt);
        };

        auto write = [&] (ui64 size, auto dt) {
            completed(1, size, dt);
        };

        auto bucket = [] (auto& v) {
            auto& b = v.GetArray();
            UNIT_ASSERT_VALUES_EQUAL(2, b.size());
            return std::tuple {b[0].GetInteger(), b[1].GetInteger()};
        };

        completionStats.Requests[0].Unaligned = 12;
        completionStats.Requests[0].Errors = 2;

        for (int i = 0; i != 400; ++i) {
            read(4_KB, 100us);
        }

        for (int i = 0; i != 300; ++i) {
            read(8_KB, 500us);
        }

        for (int i = 0; i != 450; ++i) {
            write(4_KB, 200us);
        }

        for (int i = 0; i != 300; ++i) {
            write(4_MB, 1ms);
        }

        for (int i = 0; i != 50; ++i) {
            write(32_MB, 32ms);
        }

        // Fill crit events
        TMultiMap<TString, TString> testCritEvents{
            {"event1", "message 1"},
            {"event1", "message 2"},
            {"event2", "message n"}};
        for (const auto& [name, message]: testCritEvents) {
            ReportCriticalEvent(name, message);
        }

        {
            auto stats = dump(1s);

            UNIT_ASSERT_VALUES_EQUAL(1000, stats["elapsed_ms"].GetInteger());
            UNIT_ASSERT_VALUES_EQUAL(1500, stats["dequeued"].GetInteger());
            UNIT_ASSERT_VALUES_EQUAL(1500, stats["submitted"].GetInteger());
            UNIT_ASSERT_VALUES_EQUAL(0, stats["submission_failed"].GetInteger());
            UNIT_ASSERT_VALUES_EQUAL(1500, stats["completed"].GetInteger());
            UNIT_ASSERT_VALUES_EQUAL(0, stats["failed"].GetInteger());

            const auto& r = stats["read"];
            UNIT_ASSERT_VALUES_EQUAL(700, r["count"].GetInteger());
            UNIT_ASSERT_VALUES_EQUAL(4096000, r["bytes"].GetInteger());
            UNIT_ASSERT_VALUES_EQUAL(2, r["errors"].GetInteger());
            UNIT_ASSERT_VALUES_EQUAL(12, r["unaligned"].GetInteger());

            const auto& rtimes = r["times"].GetArray();
            UNIT_ASSERT_VALUES_EQUAL(2, rtimes.size());
            {
                auto [dt, count] = bucket(rtimes[0]);

                UNIT_ASSERT_VALUES_EQUAL_C(100, dt, "100us");
                UNIT_ASSERT_VALUES_EQUAL(400, count);
            }
            {
                auto [dt, count] = bucket(rtimes[1]);

                UNIT_ASSERT_VALUES_EQUAL_C(500, dt, "500us");
                UNIT_ASSERT_VALUES_EQUAL(300, count);
            }

            const auto& rsizes = r["sizes"].GetArray();
            UNIT_ASSERT_VALUES_EQUAL(2, rsizes.size());
            {
                auto [size, count] = bucket(rsizes[0]);

                UNIT_ASSERT_VALUES_EQUAL_C(4_KB, size, "4K");
                UNIT_ASSERT_VALUES_EQUAL(400, count);
            }
            {
                auto [size, count] = bucket(rsizes[1]);

                UNIT_ASSERT_VALUES_EQUAL_C(8_KB, size, "8K");
                UNIT_ASSERT_VALUES_EQUAL(300, count);
            }

            const auto& w = stats["write"];

            UNIT_ASSERT_VALUES_EQUAL(800, w["count"].GetInteger());
            UNIT_ASSERT_VALUES_EQUAL(2937856000, w["bytes"].GetInteger());
            UNIT_ASSERT_VALUES_EQUAL(0, w["errors"].GetInteger());
            UNIT_ASSERT_VALUES_EQUAL(0, w["unaligned"].GetInteger());

            const auto& wtimes = w["times"].GetArray();
            UNIT_ASSERT_VALUES_EQUAL(3, wtimes.size());
            {
                auto [dt, count] = bucket(wtimes[0]);

                UNIT_ASSERT_VALUES_EQUAL_C(200, dt, "200us");
                UNIT_ASSERT_VALUES_EQUAL(450, count);
            }
            {
                auto [dt, count] = bucket(wtimes[1]);

                UNIT_ASSERT_VALUES_EQUAL_C(1001, dt, "1ms");
                UNIT_ASSERT_VALUES_EQUAL(300, count);
            }
            {
                auto [dt, count] = bucket(wtimes[2]);

                UNIT_ASSERT_VALUES_EQUAL_C(32047, dt, "32ms");
                UNIT_ASSERT_VALUES_EQUAL(50, count);
            }

            const auto& wsizes = w["sizes"].GetArray();
            UNIT_ASSERT_VALUES_EQUAL(3, wsizes.size());
            {
                auto [size, count] = bucket(wsizes[0]);

                UNIT_ASSERT_VALUES_EQUAL_C(4_KB, size, "4K");
                UNIT_ASSERT_VALUES_EQUAL(450, count);
            }
            {
                auto [size, count] = bucket(wsizes[1]);

                UNIT_ASSERT_VALUES_EQUAL_C(4_MB, size, "4M");
                UNIT_ASSERT_VALUES_EQUAL(300, count);
            }
            {
                auto [size, count] = bucket(wsizes[2]);

                UNIT_ASSERT_VALUES_EQUAL_C(32_MB, size, "32M");
                UNIT_ASSERT_VALUES_EQUAL(50, count);
            }

            {   // Check crit events
                UNIT_ASSERT_VALUES_EQUAL(true, stats.Has("crit_events"));
                TMultiMap<TString, TString> critEvents;
                for (const auto& event: stats["crit_events"].GetArray()) {
                    const auto& name = event["name"].GetString();
                    const auto& message = event["message"].GetString();
                    critEvents.emplace(name, message);
                }
                UNIT_ASSERT_VALUES_EQUAL(testCritEvents, critEvents);
            }
        }

        for (int i = 0; i != 800; ++i) {
            read(4_KB, 40us);
        }

        for (int i = 0; i != 200; ++i) {
            read(8_KB, 70us);
        }

        for (int i = 0; i != 100; ++i) {
            read(4_KB, 100us);
        }

        for (int i = 0; i != 600; ++i) {
            write(1_MB, 300us);
        }

        for (int i = 0; i != 400; ++i) {
            write(512_KB, 200us);
        }

        for (int i = 0; i != 3; ++i) {
            write(100_GB, 2h);
        }

        {
            auto stats = dump(5s);

            UNIT_ASSERT_VALUES_EQUAL(5000, stats["elapsed_ms"].GetInteger());
            UNIT_ASSERT_VALUES_EQUAL(2103, stats["dequeued"].GetInteger());
            UNIT_ASSERT_VALUES_EQUAL(2103, stats["submitted"].GetInteger());
            UNIT_ASSERT_VALUES_EQUAL(0, stats["submission_failed"].GetInteger());
            UNIT_ASSERT_VALUES_EQUAL(2103, stats["completed"].GetInteger());
            UNIT_ASSERT_VALUES_EQUAL(0, stats["failed"].GetInteger());

            const auto& r = stats["read"];
            UNIT_ASSERT_VALUES_EQUAL(1100, r["count"].GetInteger());
            UNIT_ASSERT_VALUES_EQUAL(5324800, r["bytes"].GetInteger());
            UNIT_ASSERT_VALUES_EQUAL(0, r["errors"].GetInteger());
            UNIT_ASSERT_VALUES_EQUAL(0, r["unaligned"].GetInteger());

            const auto& rtimes = r["times"].GetArray();
            UNIT_ASSERT_VALUES_EQUAL(3, rtimes.size());
            {
                auto [dt, count] = bucket(rtimes[0]);

                UNIT_ASSERT_VALUES_EQUAL_C(40, dt, "40us");
                UNIT_ASSERT_VALUES_EQUAL(800, count);
            }
            {
                auto [dt, count] = bucket(rtimes[1]);

                UNIT_ASSERT_VALUES_EQUAL_C(69, dt, "70us");
                UNIT_ASSERT_VALUES_EQUAL(200, count);
            }
            {
                auto [dt, count] = bucket(rtimes[2]);

                UNIT_ASSERT_VALUES_EQUAL_C(100, dt, "100us");
                UNIT_ASSERT_VALUES_EQUAL(100, count);
            }

            const auto& rsizes = r["sizes"].GetArray();
            UNIT_ASSERT_VALUES_EQUAL(2, rsizes.size());
            {
                auto [size, count] = bucket(rsizes[0]);

                UNIT_ASSERT_VALUES_EQUAL_C(4_KB, size, "4K");
                UNIT_ASSERT_VALUES_EQUAL(900, count);
            }
            {
                auto [size, count] = bucket(rsizes[1]);

                UNIT_ASSERT_VALUES_EQUAL_C(8_KB, size, "8K");
                UNIT_ASSERT_VALUES_EQUAL(200, count);
            }

            const auto& w = stats["write"];
            UNIT_ASSERT_VALUES_EQUAL(1003, w["count"].GetInteger());
            UNIT_ASSERT_VALUES_EQUAL(322961408000, w["bytes"].GetInteger());
            UNIT_ASSERT_VALUES_EQUAL(0, w["errors"].GetInteger());
            UNIT_ASSERT_VALUES_EQUAL(0, w["unaligned"].GetInteger());

            const auto& wtimes = w["times"].GetArray();
            UNIT_ASSERT_VALUES_EQUAL(3, wtimes.size());
            {
                auto [dt, count] = bucket(wtimes[0]);

                UNIT_ASSERT_VALUES_EQUAL_C(200, dt, "200us");
                UNIT_ASSERT_VALUES_EQUAL(400, count);
            }
            {
                auto [dt, count] = bucket(wtimes[1]);

                UNIT_ASSERT_VALUES_EQUAL_C(300, dt, "300us");
                UNIT_ASSERT_VALUES_EQUAL(600, count);
            }
            {
                auto [dt, count] = bucket(wtimes[2]);

                UNIT_ASSERT_VALUES_EQUAL_C(7198365188, dt, "2h");
                UNIT_ASSERT_VALUES_EQUAL(3, count);
            }

            const auto& wsizes = w["sizes"].GetArray();
            UNIT_ASSERT_VALUES_EQUAL(3, wsizes.size());
            {
                auto [size, count] = bucket(wsizes[0]);

                UNIT_ASSERT_VALUES_EQUAL_C(512_KB, size, "512K");
                UNIT_ASSERT_VALUES_EQUAL(400, count);
            }
            {
                auto [size, count] = bucket(wsizes[1]);

                UNIT_ASSERT_VALUES_EQUAL_C(1_MB, size, "1M");
                UNIT_ASSERT_VALUES_EQUAL(600, count);
            }
            {
                auto [size, count] = bucket(wsizes[2]);

                UNIT_ASSERT_VALUES_EQUAL_C(96_GB, size, "100G");
                UNIT_ASSERT_VALUES_EQUAL(3, count);
            }
            {
                // The critical events list were taken in the last dump.
                // ReportCriticalEvent() was not called after that, so the key
                // "crit_events" absent.
                UNIT_ASSERT_VALUES_EQUAL(false, stats.Has("crit_events"));
            }
        }
    }
}
