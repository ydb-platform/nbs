#include "trace_processor_mon.h"
#include "trace_processor.h"
#include "trace_reader.h"

#include <cloud/storage/core/protos/media.pb.h>
#include <cloud/storage/core/libs/common/public.h>
#include <cloud/storage/core/libs/common/scheduler_test.h>
#include <cloud/storage/core/libs/common/timer.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>
#include <cloud/storage/core/libs/diagnostics/monitoring.h>

#include <library/cpp/json/json_reader.h>
#include <library/cpp/logger/backend.h>
#include <library/cpp/lwtrace/all.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/stream/str.h>
#include <util/string/split.h>
#include <util/system/event.h>
#include <util/system/spinlock.h>
#include <util/thread/pool.h>

namespace NCloud {

#define LWTRACE_UT_PROVIDER(PROBE, EVENT, GROUPS, TYPES, NAMES)                \
    PROBE(                                                                     \
        InitialProbe,                                                          \
        GROUPS("Group"),                                                       \
        TYPES(TString, ui32, ui64),                                            \
        NAMES("requestType", NProbeParam::MediaKind, "requestId")              \
    )                                                                          \
    PROBE(                                                                     \
        SomeProbe,                                                             \
        GROUPS("Group"),                                                       \
        TYPES(TString, ui64),                                                  \
        NAMES("requestType", "requestId")                                      \
    )                                                                          \
    PROBE(                                                                     \
        SomeOtherProbe,                                                        \
        GROUPS("Group"),                                                       \
        TYPES(TString, ui64),                                                  \
        NAMES("requestType", "requestId")                                      \
    )                                                                          \
    PROBE(                                                                     \
        SomeProbeWithExecutionTime,                                            \
        GROUPS("Group"),                                                       \
        TYPES(TString, ui64, ui64),                                            \
        NAMES("requestType", "requestId", NProbeParam::RequestExecutionTime)   \
    )                                                                          \
// LWTRACE_UT_PROVIDER

LWTRACE_DECLARE_PROVIDER(LWTRACE_UT_PROVIDER)
LWTRACE_DEFINE_PROVIDER(LWTRACE_UT_PROVIDER)
LWTRACE_USING(LWTRACE_UT_PROVIDER)

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TLogBackendImpl
    : TLogBackend
{
    TVector<std::pair<ELogPriority, TString>> Recs;
    TAdaptiveLock Lock;

    void WriteData(const TLogRecord& rec) override
    {
        with_lock (Lock) {
            Recs.emplace_back(rec.Priority, TString(rec.Data, rec.Len));
        }
    }

    void ReopenLog() override
    {
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TEnv
{
    std::unique_ptr<NLWTrace::TManager> LWManager;
    ITimerPtr Timer;
    std::shared_ptr<TTestScheduler> Scheduler;
    std::shared_ptr<TLogBackendImpl> LogBackend;

    TEnv()
        : Timer(CreateWallClockTimer())
        , Scheduler(new TTestScheduler())
        , LogBackend(new TLogBackendImpl())
    {
        RebuildLWManager();
    }

    void RebuildLWManager()
    {
        LWManager = std::make_unique<NLWTrace::TManager>(
            *Singleton<NLWTrace::TProbeRegistry>(), false);
        NLWTrace::TQuery q;
        auto* block = q.AddBlocks();
        auto* pd = block->MutableProbeDesc();
        pd->SetName("InitialProbe");
        pd->SetProvider("LWTRACE_UT_PROVIDER");
        block->AddAction()->MutableRunLogShuttleAction();
        LWManager->New("filter", q);
    }

    ITraceProcessorPtr CreateTraceProcessor(
        const TRequestThresholds& requestThresholds)
    {
        auto monitoring = CreateMonitoringServiceStub();
        auto logging = CreateLoggingService(LogBackend);
        TVector<ITraceReaderPtr> readers{
            SetupTraceReaderForSlowRequests(
                "filter",
                logging,
                "STORAGE_TRACE",
                requestThresholds
        )};
        return NCloud::CreateTraceProcessorMon(
            monitoring,
            NCloud::CreateTraceProcessor(
                Timer,
                Scheduler,
                logging,
                "STORAGE_TRACE",
                *LWManager,
                std::move(readers))
        );
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TTraceProcessorTest)
{
    const ui32 REQUEST_COUNT = 10;

    void Track(
        NProto::EStorageMediaKind mediaKind = NProto::STORAGE_MEDIA_HYBRID,
        ui64 executionTime = 0)
    {
        TVector<NLWTrace::TOrbit> orbits(REQUEST_COUNT);

        for (ui64 requestId = 0; requestId < orbits.size(); ++requestId) {
            LWTRACK(
                InitialProbe,
                orbits[requestId],
                "Compaction",
                static_cast<ui32>(mediaKind),
                requestId
            );
        }

        for (ui64 requestId = 0; requestId < orbits.size(); ++requestId) {
            LWTRACK(SomeProbe, orbits[requestId], "s1", requestId);
        }

        for (ui64 requestId = 0; requestId < orbits.size(); ++requestId) {
            if (executionTime) {
                LWTRACK(
                    SomeProbeWithExecutionTime,
                    orbits[requestId],
                    "s2",
                    requestId,
                    executionTime
                );
            } else {
                LWTRACK(SomeOtherProbe, orbits[requestId], "s2", requestId);
            }
        }
    }

    void Check(TEnv& env, ui32 requestCount, ui64 executionTime = 0)
    {
        env.Scheduler->RunAllScheduledTasks();

        ui32 count = 0;
        for (const auto& rec : env.LogBackend->Recs) {
            Cdbg << int(rec.first) << "\t" << rec.second;

            TVector<TString> parts;
            StringSplitter(rec.second).SplitByString("WARN: ").Collect(&parts);
            UNIT_ASSERT_VALUES_EQUAL(2, parts.size());

            NJson::TJsonValue v;
            if (!NJson::ReadJsonFastTree(parts[1], &v, false)) {
                continue;
            }

            ++count;

            const auto& arr = v.GetArray();
            UNIT_ASSERT_VALUES_EQUAL(4, arr.size());
            UNIT_ASSERT_VALUES_EQUAL("InitialProbe", arr[0][0].GetString());
            UNIT_ASSERT_VALUES_EQUAL("SomeProbe", arr[1][0].GetString());
            if (executionTime) {
                UNIT_ASSERT_VALUES_EQUAL(
                    "SomeProbeWithExecutionTime",
                    arr[2][0].GetString()
                );
                UNIT_ASSERT_VALUES_EQUAL(
                    executionTime,
                    FromString<ui64>(arr[2][4].GetString())
                );
            } else {
                UNIT_ASSERT_VALUES_EQUAL("SomeOtherProbe", arr[2][0].GetString());
            }
            UNIT_ASSERT_VALUES_EQUAL("SlowRequests", arr[3][0].GetString());
        }
        UNIT_ASSERT_VALUES_EQUAL(requestCount, count);

        env.LogBackend->Recs.clear();

        env.Scheduler->RunAllScheduledTasks();
        UNIT_ASSERT_GT(2, env.LogBackend->Recs.size());
    }

    Y_UNIT_TEST(ShouldCollectAllOverlappingTracks)
    {
        TEnv env;
        auto tp = env.CreateTraceProcessor({
            {NProto::STORAGE_MEDIA_DEFAULT, {{}, {}, {}}}
        });
        tp->Start();

        Track();
        Check(env, REQUEST_COUNT);
    }

    Y_UNIT_TEST(ShouldProcessLotsOfTracks)
    {
        TEnv env;
        auto tp = env.CreateTraceProcessor({
            {NProto::STORAGE_MEDIA_DEFAULT, {{}, {}, {}}}
        });
        tp->Start();

        auto threadPool = CreateThreadPool(20);
        const auto runs = 100;
        // lwtrace depot sizes are limited by 1000
        static_assert(runs <= 1000 / REQUEST_COUNT);

        TAtomic remaining = runs;
        TManualEvent ev;

        for (size_t j = 0; j < runs; ++j) {
            threadPool->SafeAddFunc([&remaining, &ev] () {
                Track();

                AtomicDecrement(remaining);
                ev.Signal();
            });
        }

        while (AtomicGet(remaining)) {
            ev.WaitI();
        }

        auto requestCount = Min<ui32>(runs * REQUEST_COUNT, DumpTracksLimit);
        Check(env, requestCount);

        threadPool->Stop();
    }

    Y_UNIT_TEST(SlowRequestThresholdByTrackLength)
    {
        // no way to override timestamps in track items
        // => will have to test only max+zero and zero+max thresholds
        TEnv env;
        auto tp = env.CreateTraceProcessor(
            {
                {NProto::STORAGE_MEDIA_HDD, {{}, {}, {}}},
                {NProto::STORAGE_MEDIA_DEFAULT, {{}, {}, {}}},
                {NProto::STORAGE_MEDIA_SSD, {TDuration::Max(), {}, {}}},
                {
                    NProto::STORAGE_MEDIA_SSD_NONREPLICATED,
                    {TDuration::Max(), {}, {}}
                }
            }
        );
        tp->Start();


        Track(NProto::STORAGE_MEDIA_HYBRID);
        Check(env, REQUEST_COUNT);
        Track(NProto::STORAGE_MEDIA_HDD);
        Check(env, REQUEST_COUNT);
        Track(NProto::STORAGE_MEDIA_SSD);
        Check(env, 0);
        Track(NProto::STORAGE_MEDIA_SSD_NONREPLICATED);
        Check(env, 0);

        env.RebuildLWManager();
        tp = env.CreateTraceProcessor(
            {
                {NProto::STORAGE_MEDIA_HDD, {TDuration::Max(), {}, {}}},
                {NProto::STORAGE_MEDIA_SSD, {{}, {}, {}}},
                {
                    NProto::STORAGE_MEDIA_SSD_NONREPLICATED,
                    {TDuration::Max(), {}, {}}
                },
                {NProto::STORAGE_MEDIA_DEFAULT, {TDuration::Max(), {}, {}}}
            }
        );
        tp->Start();

        Track(NProto::STORAGE_MEDIA_HYBRID);
        Check(env, 0);
        Track(NProto::STORAGE_MEDIA_HDD);
        Check(env, 0);
        Track(NProto::STORAGE_MEDIA_SSD);
        Check(env, REQUEST_COUNT);
        Track(NProto::STORAGE_MEDIA_SSD_NONREPLICATED);
        Check(env, 0);

        env.RebuildLWManager();
        tp = env.CreateTraceProcessor(
            {
                {NProto::STORAGE_MEDIA_HDD, {TDuration::Max(), {}, {}}},
                {NProto::STORAGE_MEDIA_SSD, {TDuration::Max(), {}, {}}},
                {
                    NProto::STORAGE_MEDIA_SSD_NONREPLICATED,
                    {{}, {}, {}}
                },
                {NProto::STORAGE_MEDIA_DEFAULT, {TDuration::Max(), {}, {}}}
            }
        );
        tp->Start();

        Track(NProto::STORAGE_MEDIA_HYBRID);
        Check(env, 0);
        Track(NProto::STORAGE_MEDIA_HDD);
        Check(env, 0);
        Track(NProto::STORAGE_MEDIA_SSD);
        Check(env, 0);
        Track(NProto::STORAGE_MEDIA_SSD_NONREPLICATED);
        Check(env, REQUEST_COUNT);
    }

    Y_UNIT_TEST(SlowRequestThresholdByExecutionTimeParam)
    {
        TEnv env;
        auto tp = env.CreateTraceProcessor(
            {
                {NProto::STORAGE_MEDIA_HDD, {TDuration::Seconds(20), {}, {}}},
                {
                    NProto::STORAGE_MEDIA_DEFAULT,
                    {TDuration::Seconds(20), {}, {}}
                },
                {NProto::STORAGE_MEDIA_SSD, {TDuration::Seconds(20), {}, {}}},
                {
                    NProto::STORAGE_MEDIA_SSD_NONREPLICATED,
                    {TDuration::Seconds(20), {}, {}}
                }
            }
        );
        tp->Start();

        Track(NProto::STORAGE_MEDIA_HYBRID, 15'000'000);
        Check(env, 0);
        Track(NProto::STORAGE_MEDIA_HYBRID, 25'000'000);
        Check(env, REQUEST_COUNT, 25'000'000);
    }

    Y_UNIT_TEST(SlowRequestByRequestType)
    {
        {
            TEnv env;
            TRequestThresholds thresholds = {
                {NProto::STORAGE_MEDIA_DEFAULT, {{}, {}, {}}},
                {
                    NProto::STORAGE_MEDIA_HYBRID,
                    {
                        {},
                        {},
                        {
                            {"Compaction", TDuration::Seconds(50)},
                            {"Other", TDuration::Seconds(60)}
                        }
                    }
                },
                {NProto::STORAGE_MEDIA_HDD, {TDuration::Seconds(20), {}, {}}},
                {NProto::STORAGE_MEDIA_SSD, {TDuration::Seconds(20), {}, {}}},
                {
                    NProto::STORAGE_MEDIA_SSD_NONREPLICATED,
                    {TDuration::Seconds(20), {}, {}}
                }
            };

            auto tp = env.CreateTraceProcessor(thresholds);
            tp->Start();
            Track(NProto::STORAGE_MEDIA_HYBRID, 30'000'000);
            Check(env, 0);
            Track(NProto::STORAGE_MEDIA_HYBRID, 55'000'000);
            Check(env, REQUEST_COUNT, 55'000'000);
        }

        {
            TEnv env;
            TRequestThresholds thresholds = {
                {NProto::STORAGE_MEDIA_HDD, {TDuration::Seconds(20), {}, {}}},
                {NProto::STORAGE_MEDIA_SSD, {TDuration::Seconds(20), {}, {}}},
                {
                    NProto::STORAGE_MEDIA_SSD_NONREPLICATED,
                    {TDuration::Seconds(20), {}, {}}
                },
                {
                    NProto::STORAGE_MEDIA_DEFAULT,
                    {
                        {},
                        {},
                        {{"Compaction", TDuration::Seconds(40)}}
                    }
                },
                {
                    NProto::STORAGE_MEDIA_HYBRID,
                    {
                        {},
                        {},
                        {{"Compaction", TDuration::Seconds(50)}}
                    }
                }
            };

            auto tp = env.CreateTraceProcessor(thresholds);
            tp->Start();

            Track(NProto::STORAGE_MEDIA_SSD, 30'000'000);
            Check(env, REQUEST_COUNT, 30'000'000);
        }

        {

            TEnv env;
            TRequestThresholds thresholds = {
                {NProto::STORAGE_MEDIA_HDD, {TDuration::Seconds(20), {}, {}}},
                {NProto::STORAGE_MEDIA_SSD, {TDuration::Seconds(20), {}, {}}},
                {
                    NProto::STORAGE_MEDIA_SSD_NONREPLICATED,
                    {TDuration::Seconds(20), {}, {}}
                },
                {
                    NProto::STORAGE_MEDIA_HYBRID,
                    {
                        {},
                        {},
                        {{"Compaction", TDuration::Seconds(50)}}
                    }
                }
            };

            auto tp = env.CreateTraceProcessor(thresholds);
            tp->Start();

            Track(NProto::STORAGE_MEDIA_SSD, 21'000'000);
            Check(env, REQUEST_COUNT, 21'000'000);
        }

        {
            TEnv env;
            // Check if no mediaKind and default thresholds
            TRequestThresholds thresholds = {
                {NProto::STORAGE_MEDIA_HDD, {TDuration::Seconds(20), {}, {}}}
            };

            auto tp = env.CreateTraceProcessor(thresholds);
            tp->Start();

            Track(NProto::STORAGE_MEDIA_SSD, 10'000'000);
            Check(env, REQUEST_COUNT, 10'000'000);
        }
    }

    Y_UNIT_TEST(ShouldConvertRequestThresholds)
    {
        {
            NCloud::TProtoRequestThresholds thresholds;
            auto protoTr = new NCloud::NProto::TLWTraceThreshold;
            protoTr->SetMediaKind(
                static_cast<NCloud::NProto::EStorageMediaKind>(
                    NProto::STORAGE_MEDIA_SSD_MIRROR3));
            protoTr->SetDefault(1000);
            auto& protoByRequestType = *protoTr->MutableByRequestType();
            protoByRequestType["type_first"] = 100;
            protoByRequestType["type_second"] = 200;
            thresholds.AddAllocated(protoTr);

            auto protoTrSecond = new NCloud::NProto::TLWTraceThreshold;
            protoTrSecond->SetMediaKind(
                static_cast<NCloud::NProto::EStorageMediaKind>(
                    NProto::STORAGE_MEDIA_SSD_LOCAL));
            protoTrSecond->SetDefault(1);
            thresholds.AddAllocated(protoTrSecond);

            auto result = ConvertRequestThresholds(thresholds);

            UNIT_ASSERT_VALUES_EQUAL(result.size(), 2);

            auto& mirrorResult = result[NProto::STORAGE_MEDIA_SSD_MIRROR3];
            UNIT_ASSERT_VALUES_EQUAL(
                mirrorResult.Default, TDuration::MilliSeconds(1000));
            UNIT_ASSERT_VALUES_EQUAL(
                mirrorResult.ByRequestType["type_first"],
                TDuration::MilliSeconds(100)
            );
            UNIT_ASSERT_VALUES_EQUAL(
                mirrorResult.ByRequestType["type_second"],
                TDuration::MilliSeconds(200)
            );

            auto localResult = result[NProto::STORAGE_MEDIA_SSD_LOCAL];
            UNIT_ASSERT_VALUES_EQUAL(
                localResult.Default,
                TDuration::MilliSeconds(1)
            );
            UNIT_ASSERT(localResult.ByRequestType.empty());
        }
        {
            NCloud::TProtoRequestThresholds thresholds;
            auto result = ConvertRequestThresholds(thresholds);
            UNIT_ASSERT(result.empty());
        }
    }

    Y_UNIT_TEST(ShouldOutputRequestThresholds)
    {
        TStringStream out;
        TRequestThresholds requestThresholds{
            {
                NProto::STORAGE_MEDIA_DEFAULT,
                {TDuration::MilliSeconds(200), {}, {}}},
            {
                NProto::STORAGE_MEDIA_HYBRID,
                {
                    TDuration::Seconds(10),
                    {},
                    {
                        {"Compaction", TDuration::Seconds(50)}
                    }
                }
            }
        };

        OutRequestThresholds(out, requestThresholds);

        const char* first = "Default: 10000\n"
            "ByRequestType {\n"
            "  key: \"Compaction\"\n"
            "  value: 50000\n"
            "}\n"
            "MediaKind: STORAGE_MEDIA_HYBRID\n";

        UNIT_ASSERT(out.Str().Contains(first));

        const char* second = "Default: 200\n";

        UNIT_ASSERT(out.Str().Contains(first));
        UNIT_ASSERT(out.Str().Contains(second));
    }

    Y_UNIT_TEST(ShouldGetThresholdByRequestType)
    {
        {
            auto duration = GetThresholdByRequestType(
                NProto::STORAGE_MEDIA_HYBRID,
                {
                    {
                        NProto::STORAGE_MEDIA_DEFAULT,
                        {TDuration::MilliSeconds(200), {}, {}}
                    }
                }, nullptr, nullptr);
            UNIT_ASSERT_VALUES_EQUAL(duration, TDuration::MilliSeconds(200));
        }
        {
            auto duration = GetThresholdByRequestType(
                NProto::STORAGE_MEDIA_HYBRID,
                {
                    {
                        NProto::STORAGE_MEDIA_HYBRID,
                        {TDuration::MilliSeconds(200), {}, {}}
                    },
                    {
                        NProto::STORAGE_MEDIA_DEFAULT,
                        {TDuration::MilliSeconds(220), {}, {}}
                    }
                }, nullptr, nullptr);
            UNIT_ASSERT_VALUES_EQUAL(duration, TDuration::MilliSeconds(200));
        }
        {
            TString str = "Compaction";
            NLWTrace::TParam param;
            NLWTrace::TTraceParam traceParam;
            traceParam.SetStrValue(str);
            NLWTrace::LoadParamFromPb<TString>(traceParam, param);

            auto duration = GetThresholdByRequestType(
                NProto::STORAGE_MEDIA_HYBRID,
                {
                    {
                        NProto::STORAGE_MEDIA_HYBRID,
                        {TDuration::MilliSeconds(200), {}, {}}
                    },
                    {
                        NProto::STORAGE_MEDIA_DEFAULT,
                        {
                            TDuration::MilliSeconds(220),
                            {},
                            {{"Compaction", TDuration::MilliSeconds(230)}}
                        }
                    }
                }, &param, nullptr);
            UNIT_ASSERT_VALUES_EQUAL(duration, TDuration::MilliSeconds(200));

            duration = GetThresholdByRequestType(
                NProto::STORAGE_MEDIA_HYBRID,
                {
                    {
                        NProto::STORAGE_MEDIA_HYBRID,
                        {
                            TDuration::MilliSeconds(200),
                            {},
                            {
                                {
                                    {"Compaction", TDuration::MilliSeconds(230)},
                                    {"NotCompaction", TDuration::MilliSeconds(250)},
                                }
                            }
                        }
                    },
                    {
                        NProto::STORAGE_MEDIA_DEFAULT,
                        {
                            TDuration::MilliSeconds(220),
                            {},
                            {{"Compaction", TDuration::MilliSeconds(240)}}
                        }
                    }
                }, &param, nullptr);
            UNIT_ASSERT_VALUES_EQUAL(duration, TDuration::MilliSeconds(230));

            param.Destruct<TString>();
        }
    }

    Y_UNIT_TEST(ShouldCalculateSlowRequestThresholdUsingRequestSize)
    {
        NLWTrace::TParam param;
        NLWTrace::TTraceParam traceParam;
        traceParam.SetUintValue(1_MB);
        NLWTrace::LoadParamFromPb<ui64>(traceParam, param);

        TRequestThresholds thresholds = {
            {
                NProto::STORAGE_MEDIA_HYBRID,
                {
                    TDuration::MilliSeconds(100),
                    TDuration::MilliSeconds(1),
                    {}
                }
            },
        };

        auto duration = GetThresholdByRequestType(
            NProto::STORAGE_MEDIA_HYBRID,
            thresholds,
            nullptr,
            &param);
        UNIT_ASSERT_VALUES_EQUAL(duration, TDuration::MilliSeconds(1124));

        param.Destruct<ui64>();
    }
}

}   // namespace NCloud
