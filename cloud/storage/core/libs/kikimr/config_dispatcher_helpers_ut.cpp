#include "config_dispatcher_helpers.h"

#include <contrib/ydb/core/protos/console_config.pb.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/string.h>

#include <variant>

namespace NCloud {

using namespace NStorage;
using namespace NKikimr::NConfig;

namespace {

////////////////////////////////////////////////////////////////////////////////

auto SetupCriticalEvent()
{
    NMonitoring::TDynamicCountersPtr counters =
        new NMonitoring::TDynamicCounters();
    InitCriticalEventsCounter(counters);
    return counters->GetCounter(
        "AppCriticalEvents/ConfigDispatcherItemParseError",
        true);
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TConfigDispatcherHelpersTest)
{
    Y_UNIT_TEST(ShouldRaiseCriticalEventIfCannotParseConfigDispatcherEkind)
    {
        {
            auto counter = SetupCriticalEvent();

            NKikimr::NConfig::TConfigsDispatcherInitInfo config;
            NProto::TYdbConfigDispatcherSettings settings;
            settings.MutableAllowList()->AddNames("xyz");
            SetupConfigDispatcher(settings, &config);

            UNIT_ASSERT(
                std::holds_alternative<std::monostate>(config.ItemsServeRules));
            UNIT_ASSERT_VALUES_EQUAL(1, counter->Val());
        }

        {
            auto counter = SetupCriticalEvent();

            NKikimr::NConfig::TConfigsDispatcherInitInfo config;
            NProto::TYdbConfigDispatcherSettings settings;
            settings.MutableDenyList()->AddNames("xyz");
            SetupConfigDispatcher(settings, &config);

            UNIT_ASSERT(
                std::holds_alternative<std::monostate>(config.ItemsServeRules));
            UNIT_ASSERT_VALUES_EQUAL(1, counter->Val());
        }
    }

    Y_UNIT_TEST(ShouldParseConfigDispatcherEkind)
    {
        NKikimr::NConfig::TConfigsDispatcherInitInfo config;
        auto counter = SetupCriticalEvent();

        NProto::TYdbConfigDispatcherSettings settings;

        constexpr auto minVal = NKikimrConsole::TConfigItem_EKind_EKind_MIN;
        constexpr auto maxVal = NKikimrConsole::TConfigItem_EKind_EKind_MAX;
        ui32 cnt = 0;
        for (ui32 id = minVal; id < maxVal; ++id)  {
            if (!NKikimrConsole::TConfigItem_EKind_IsValid(id)) {
                continue;
            }
            ++cnt;
            settings.MutableAllowList()->AddNames(
                NKikimrConsole::TConfigItem_EKind_Name(id));
        }

        SetupConfigDispatcher(settings, &config);

        UNIT_ASSERT(
            std::holds_alternative<TAllowList>(config.ItemsServeRules));
        UNIT_ASSERT_VALUES_EQUAL(
            cnt,
            std::get<TAllowList>(config.ItemsServeRules).Items.size());
        UNIT_ASSERT_VALUES_EQUAL(0, counter->Val());
    }
}

}   // namespace NCloud
