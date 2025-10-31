#include "config_dispatcher_helpers.h"

#include <ydb/core/protos/console_config.pb.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/guid.h>
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
    Y_UNIT_TEST(ShouldRaiseCriticalEventIfCannotParseConfigDispatcherItem)
    {
        {
            auto counter = SetupCriticalEvent();

            NKikimr::NConfig::TConfigsDispatcherInitInfo config;
            NProto::TConfigDispatcherSettings settings;
            settings.MutableAllowList()->AddNames("xyz");
            SetupConfigDispatcher(settings, {}, {}, &config);

            UNIT_ASSERT(
                std::holds_alternative<TAllowList>(config.ItemsServeRules));
            UNIT_ASSERT(std::get<TAllowList>(config.ItemsServeRules).Items.empty());
            UNIT_ASSERT_VALUES_EQUAL(1, counter->Val());
        }

        {
            auto counter = SetupCriticalEvent();

            NKikimr::NConfig::TConfigsDispatcherInitInfo config;
            NProto::TConfigDispatcherSettings settings;
            settings.MutableDenyList()->AddNames("xyz");
            SetupConfigDispatcher(settings, {}, {}, &config);

            UNIT_ASSERT(
                std::holds_alternative<TDenyList>(config.ItemsServeRules));
            UNIT_ASSERT(std::get<TDenyList>(config.ItemsServeRules).Items.empty());
            UNIT_ASSERT_VALUES_EQUAL(1, counter->Val());
        }
    }

    Y_UNIT_TEST(ShouldAllowAllConfigsWhenAllSettingsAreEmpty)
    {
        auto counter = SetupCriticalEvent();

        NKikimr::NConfig::TConfigsDispatcherInitInfo config;
        NProto::TConfigDispatcherSettings settings;
        SetupConfigDispatcher(settings, {}, {}, &config);

        UNIT_ASSERT(
            std::holds_alternative<std::monostate>(config.ItemsServeRules));
        UNIT_ASSERT_VALUES_EQUAL(0, counter->Val());
    }

    Y_UNIT_TEST(ShouldParseConfigDispatcherItems)
    {
        NKikimr::NConfig::TConfigsDispatcherInitInfo config;
        auto counter = SetupCriticalEvent();

        NProto::TConfigDispatcherSettings settings;

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

        SetupConfigDispatcher(settings, {}, {}, &config);

        UNIT_ASSERT(
            std::holds_alternative<TAllowList>(config.ItemsServeRules));
        UNIT_ASSERT_VALUES_EQUAL(
            cnt,
            std::get<TAllowList>(config.ItemsServeRules).Items.size());
        UNIT_ASSERT_VALUES_EQUAL(0, counter->Val());
    }

    Y_UNIT_TEST(ShouldFillTenantAndNodeTypeLabels)
    {
        auto counter = SetupCriticalEvent();

        const TString tenant = CreateGuidAsString();
        const TString node = CreateGuidAsString();

        NKikimr::NConfig::TConfigsDispatcherInitInfo config;
        NProto::TConfigDispatcherSettings settings;
        SetupConfigDispatcher(settings, tenant, node, &config);

        UNIT_ASSERT(
            std::holds_alternative<std::monostate>(config.ItemsServeRules));
        UNIT_ASSERT_VALUES_EQUAL(0, counter->Val());
        UNIT_ASSERT_VALUES_EQUAL(tenant, config.Labels["tenant"]);
        UNIT_ASSERT_VALUES_EQUAL(node, config.Labels["node_type"]);
    }
}

}   // namespace NCloud
