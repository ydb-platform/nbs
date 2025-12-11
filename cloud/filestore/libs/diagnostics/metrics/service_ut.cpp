#include "service.h"

#include "label.h"
#include "registry.h"
#include "visitor.h"

#include <cloud/storage/core/libs/common/scheduler.h>
#include <cloud/storage/core/libs/common/timer_test.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/system/event.h>

#include <atomic>

namespace NCloud::NFileStore::NMetrics {

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TEnv: public NUnitTest::TBaseFixture
{
    std::shared_ptr<TTestTimer> Timer;
    ISchedulerPtr Scheduler;

    TMetricsServiceConfig Config;
    NMonitoring::TDynamicCountersPtr Counters;
    IMetricsServicePtr Service;

    TStringStream Data;
    IRegistryVisitorPtr Visitor;

    TEnv()
        : Timer(std::make_shared<TTestTimer>())
        , Scheduler(CreateScheduler(Timer))
        , Counters(new NMonitoring::TDynamicCounters())
        , Visitor(CreateRegistryVisitor(Data))
    {
        Config.UpdateInterval = TDuration::Seconds(5);
    }

    void SetUp(NUnitTest::TTestContext& /*context*/) override
    {
        Service = CreateMetricsService(Config, Timer, Scheduler);
        Service->SetupCounters(Counters);

        Scheduler->Start();
        Service->Start();
    }

    void TearDown(NUnitTest::TTestContext& /*context*/) override
    {
        if (Service) {
            Service->Stop();
        }
        Scheduler->Stop();
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TMetricsServiceTest)
{
    Y_UNIT_TEST_F(ShouldUseCorrectCountersLabel, TEnv)
    {
        std::atomic<i64> value(42);
        Service->GetRegistry()->Register({CreateSensor("test")}, value);

        // test == 0, because Update was not called.
        Counters->OutputPlainText(Data);
        UNIT_ASSERT_VALUES_EQUAL(
            "\n"
            "counters=filestore:\n"
            "    sensor=test: 0\n",
            Data.Str());
        Data.Clear();
    }

    Y_UNIT_TEST_F(ShouldUpdateOnTimeInConfig, TEnv)
    {
        std::atomic<i64> value(42);
        Service->GetRegistry()->Register({CreateSensor("test")}, value);

        auto notifier = std::make_shared<TAutoEvent>();
        Scheduler->Schedule(Timer->Now(), [notifier] { notifier->Signal(); });
        UNIT_ASSERT_C(notifier->Wait(), "Locked on scheduler");

        // value == 0, because Registry->Update() was not called.
        Counters->OutputPlainText(Data);
        UNIT_ASSERT_VALUES_EQUAL(
            "\n"
            "counters=filestore:\n"
            "    sensor=test: 0\n",
            Data.Str());
        Data.Clear();

        Scheduler->Schedule(
            Timer->Now() + Config.UpdateInterval - TDuration::MilliSeconds(1),
            [notifier] { notifier->Signal(); });

        Timer->AdvanceTime(Config.UpdateInterval - TDuration::MilliSeconds(1));

        UNIT_ASSERT_C(notifier->Wait(), "Locked on scheduler");

        // value == 0, because Registry->Update() was not called.
        Counters->OutputPlainText(Data);
        UNIT_ASSERT_VALUES_EQUAL(
            "\n"
            "counters=filestore:\n"
            "    sensor=test: 0\n",
            Data.Str());
        Data.Clear();

        Scheduler->Schedule(
            Timer->Now() + TDuration::MilliSeconds(2),
            [notifier] { notifier->Signal(); });

        Timer->AdvanceTime(TDuration::MilliSeconds(2));

        UNIT_ASSERT_C(notifier->Wait(), "Locked on scheduler");

        // value == 42, because Registry->Update() was called.
        Counters->OutputPlainText(Data);
        UNIT_ASSERT_VALUES_EQUAL(
            "\n"
            "counters=filestore:\n"
            "    sensor=test: 42\n",
            Data.Str());
        Data.Clear();

        value = 100;

        Scheduler->Schedule(
            Timer->Now() + Config.UpdateInterval - TDuration::MilliSeconds(1),
            [notifier] { notifier->Signal(); });

        Timer->AdvanceTime(Config.UpdateInterval - TDuration::MilliSeconds(1));

        UNIT_ASSERT_C(notifier->Wait(), "Locked on scheduler");

        // value == 42, because Registry->Update() was not called.
        Counters->OutputPlainText(Data);
        UNIT_ASSERT_VALUES_EQUAL(
            "\n"
            "counters=filestore:\n"
            "    sensor=test: 42\n",
            Data.Str());
        Data.Clear();

        Scheduler->Schedule(
            Timer->Now() + TDuration::MilliSeconds(2),
            [notifier] { notifier->Signal(); });

        Timer->AdvanceTime(TDuration::MilliSeconds(2));

        UNIT_ASSERT_C(notifier->Wait(), "Locked on scheduler");

        // value == 100, because Registry->Update() was called.
        Counters->OutputPlainText(Data);
        UNIT_ASSERT_VALUES_EQUAL(
            "\n"
            "counters=filestore:\n"
            "    sensor=test: 100\n",
            Data.Str());
        Data.Clear();
    }

    Y_UNIT_TEST_F(
        ShouldNotCrushOnUpdateIfMetricsServiceDestroyedBeforeStop,
        TEnv)
    {
        std::atomic<i64> value(42);
        Service->GetRegistry()->Register({CreateSensor("test")}, value);

        auto notifier = std::make_shared<TAutoEvent>();
        Scheduler->Schedule(
            Timer->Now() + Config.UpdateInterval + TDuration::MilliSeconds(1),
            [notifier] { notifier->Signal(); });

        Timer->AdvanceTime(Config.UpdateInterval + TDuration::MilliSeconds(1));

        UNIT_ASSERT_C(notifier->Wait(), "Locked on scheduler");

        Counters->OutputPlainText(Data);
        UNIT_ASSERT_VALUES_EQUAL(
            "\n"
            "counters=filestore:\n"
            "    sensor=test: 42\n",
            Data.Str());
        Data.Clear();

        Scheduler->Schedule(
            Timer->Now() + Config.UpdateInterval + TDuration::MilliSeconds(1),
            [notifier] { notifier->Signal(); });

        // Destroy service.
        Service.reset();
        Timer->AdvanceTime(Config.UpdateInterval + TDuration::MilliSeconds(1));

        UNIT_ASSERT_C(notifier->Wait(), "Locked on scheduler");

        // Empty counters, because Registry clears all subgroups on destruction.
        Counters->OutputPlainText(Data);
        UNIT_ASSERT_VALUES_EQUAL("", Data.Str());
        Data.Clear();
    }
}

}   // namespace NCloud::NFileStore::NMetrics
