#include <cloud/blockstore/libs/cells/iface/inbound_activity.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NBlockStore::NCells {

Y_UNIT_TEST_SUITE(TCellInboundActivityTest)
{
    Y_UNIT_TEST(ShouldRecordAndSnapshotBySource)
    {
        TCellInboundActivity activity;
        const auto now = TInstant::Seconds(100);

        activity.Record(
            "peer-a", "disk-1", "client-x", now);
        activity.Record(
            "peer-a", "disk-1", "client-x", now);
        activity.Record(
            "peer-b", "disk-2", "client-y", now);

        auto rows = activity.Snapshot(now);
        UNIT_ASSERT_VALUES_EQUAL(2, rows.size());

        // rows for the same source collapse into one
        const auto* a = FindIfPtr(
            rows, [](const auto& row) { return row.Peer == "peer-a"; });
        UNIT_ASSERT(a);
        UNIT_ASSERT_VALUES_EQUAL("disk-1", a->DiskId);
    }

    Y_UNIT_TEST(ShouldPruneRowsPastTheTtl)
    {
        TCellInboundActivity activity;
        const auto start = TInstant::Seconds(100);

        activity.Record(
            "peer-a", "disk-1", "client-x", start);

        // still within the TTL
        UNIT_ASSERT_VALUES_EQUAL(
            1,
            activity.Snapshot(start + TCellInboundActivity::Ttl).size());

        // just past it
        UNIT_ASSERT_VALUES_EQUAL(
            0,
            activity.Snapshot(
                start + TCellInboundActivity::Ttl + TDuration::Seconds(1))
                .size());
    }
}

}   // namespace NCloud::NBlockStore::NCells
