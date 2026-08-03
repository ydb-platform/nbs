#include "busy_idle_calculator.h"

#include <library/cpp/testing/unittest/registar.h>

#include <util/system/event.h>
#include <util/thread/factory.h>

namespace NCloud {

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TTestTimer: ITimer
{
    std::atomic<ui64> T;

    TInstant Now() override
    {
        Cdbg << "T=" << T.load() << Endl;
        return TInstant::MicroSeconds(1 + T.fetch_add(1));
    }

    void Sleep(TDuration duration) override
    {
        Y_UNUSED(duration);

        UNIT_ASSERT(false);
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TFixture
{
    std::shared_ptr<TTestTimer> Timer = std::make_shared<TTestTimer>();
    TBusyIdleTimeCalculatorAtomics Calc{Timer};

    std::atomic<i64> Busy;
    std::atomic<i64> Idle;

    TFixture()
    {
        Calc.Register(&Busy, &Idle);
    }

};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TBusyIdleTimeCalculatorTest)
{
    Y_UNIT_TEST(BasicOps)
    {
        TFixture fx;
        fx.Calc.OnRequestStarted();
        fx.Calc.OnRequestCompleted();

        UNIT_ASSERT_VALUES_EQUAL(1, fx.Busy.load());
        UNIT_ASSERT_VALUES_EQUAL(1, fx.Idle.load());

        fx.Calc.OnRequestStarted();

        UNIT_ASSERT_VALUES_EQUAL(1, fx.Busy.load());
        UNIT_ASSERT_VALUES_EQUAL(2, fx.Idle.load());

        fx.Calc.OnRequestStarted();
        fx.Calc.OnRequestCompleted();

        UNIT_ASSERT_VALUES_EQUAL(1, fx.Busy.load());
        UNIT_ASSERT_VALUES_EQUAL(2, fx.Idle.load());

        fx.Calc.OnRequestCompleted();

        UNIT_ASSERT_VALUES_EQUAL(2, fx.Busy.load());
        UNIT_ASSERT_VALUES_EQUAL(2, fx.Idle.load());

        fx.Calc.OnUpdateStats();

        UNIT_ASSERT_VALUES_EQUAL(2, fx.Busy.load());
        UNIT_ASSERT_VALUES_EQUAL(3, fx.Idle.load());

        fx.Calc.OnRequestStarted();

        UNIT_ASSERT_VALUES_EQUAL(2, fx.Busy.load());
        UNIT_ASSERT_VALUES_EQUAL(4, fx.Idle.load());

        fx.Calc.OnUpdateStats();

        UNIT_ASSERT_VALUES_EQUAL(3, fx.Busy.load());
        UNIT_ASSERT_VALUES_EQUAL(4, fx.Idle.load());
    }

    Y_UNIT_TEST(ThreadSafety)
    {
        TFixture fx;

        const ui32 iters = 10'000;
        TManualEvent e1;
        TManualEvent e2;

        SystemThreadFactory()->Run([&] () {
            for (ui32 i = 0; i < iters; ++i) {
                fx.Calc.OnRequestStarted();
                fx.Calc.OnRequestCompleted();
            }

            e1.Signal();
        });

        SystemThreadFactory()->Run([&] () {
            for (ui32 i = 0; i < iters; ++i) {
                fx.Calc.OnRequestStarted();
                fx.Calc.OnRequestCompleted();
            }

            e2.Signal();
        });

        e1.WaitI();
        e2.WaitI();

        Cerr << "Busy=" << fx.Busy.load() << Endl;
        Cerr << "Idle=" << fx.Idle.load() << Endl;
        Cerr << "T=" << fx.Timer->T.load() << Endl;

        UNIT_ASSERT_VALUES_EQUAL(
            fx.Timer->T.load() - 1,
            fx.Busy.load() + fx.Idle.load());
    }
}

}   // namespace NCloud
