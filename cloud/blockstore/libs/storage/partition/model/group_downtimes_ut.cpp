#include "group_downtimes.h"

#include <cloud/blockstore/libs/diagnostics/downtime_history.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NBlockStore::NStorage::NPartition {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TGroupDowntimesTest)
{
    Y_UNIT_TEST(ShouldRegisterGroupDowntimes)
    {
        TGroupDowntimes groupDowntimes;

        const auto& dts = groupDowntimes.GetGroupId2Downtimes();
        UNIT_ASSERT_VALUES_EQUAL(0, dts.size());

        const ui32 group1 = 11111;
        const ui32 group2 = 22222;

        auto now = TInstant::Seconds(10);
        groupDowntimes.RegisterSuccess(now, group1);
        now += TDuration::Seconds(1);
        groupDowntimes.RegisterDowntime(now, group2);

        const auto& updatedDts = groupDowntimes.GetGroupId2Downtimes();
        UNIT_ASSERT_VALUES_EQUAL(1, updatedDts.size());
        auto* g2dts = updatedDts.FindPtr(group2);
        UNIT_ASSERT(g2dts);
        UNIT_ASSERT(g2dts->HasRecentState(now, EDowntimeStateChange::DOWN));
        auto recent = g2dts->RecentEvents(now);
        UNIT_ASSERT_VALUES_EQUAL(1, recent.size());
        UNIT_ASSERT_VALUES_EQUAL(now, recent[0].first);
        UNIT_ASSERT_VALUES_EQUAL(
            static_cast<int>(EDowntimeStateChange::DOWN),
            static_cast<int>(recent[0].second));

        now += TDuration::Minutes(1);
        groupDowntimes.RegisterSuccess(now, group2);
        const auto& updatedDts2 = groupDowntimes.GetGroupId2Downtimes();
        g2dts = updatedDts2.FindPtr(group2);
        UNIT_ASSERT(g2dts);
        UNIT_ASSERT(g2dts->HasRecentState(now, EDowntimeStateChange::DOWN));
        recent = g2dts->RecentEvents(now);
        UNIT_ASSERT_VALUES_EQUAL(2, recent.size());
        UNIT_ASSERT_VALUES_EQUAL(now - TDuration::Minutes(1), recent[0].first);
        UNIT_ASSERT_VALUES_EQUAL(
            static_cast<int>(EDowntimeStateChange::DOWN),
            static_cast<int>(recent[0].second));
        UNIT_ASSERT_VALUES_EQUAL(now, recent[1].first);
        UNIT_ASSERT_VALUES_EQUAL(
            static_cast<int>(EDowntimeStateChange::UP),
            static_cast<int>(recent[1].second));

        // the last event outside of the 1 hour window is always kept
        // that's why we still have DOWN state after moving the window by
        // 59 minutes
        now += TDuration::Minutes(59);
        const auto& updatedDts3 = groupDowntimes.GetGroupId2Downtimes();
        g2dts = updatedDts3.FindPtr(group2);
        UNIT_ASSERT(g2dts);
        UNIT_ASSERT(g2dts->HasRecentState(now, EDowntimeStateChange::DOWN));

        now += TDuration::Minutes(1);
        const auto& updatedDts4 = groupDowntimes.GetGroupId2Downtimes();
        g2dts = updatedDts4.FindPtr(group2);
        UNIT_ASSERT(g2dts);
        UNIT_ASSERT(!g2dts->HasRecentState(now, EDowntimeStateChange::DOWN));
        recent = g2dts->RecentEvents(now);
        UNIT_ASSERT_VALUES_EQUAL(1, recent.size());
        UNIT_ASSERT_VALUES_EQUAL(now - TDuration::Minutes(60), recent[0].first);
        UNIT_ASSERT_VALUES_EQUAL(
            static_cast<int>(EDowntimeStateChange::UP),
            static_cast<int>(recent[0].second));

        groupDowntimes.RegisterSuccess(now, group2);
        const auto& finalDts = groupDowntimes.GetGroupId2Downtimes();
        UNIT_ASSERT_VALUES_EQUAL(0, finalDts.size());
    }
}

}   // namespace NCloud::NBlockStore::NStorage::NPartition
