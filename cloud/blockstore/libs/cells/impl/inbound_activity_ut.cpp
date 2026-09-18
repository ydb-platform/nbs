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
            "cell-7", "peer-a", "disk-1", "client-x",
            EBlockStoreRequest::MountVolume, now);
        activity.Record(
            "cell-7", "peer-a", "disk-1", "client-x",
            EBlockStoreRequest::MountVolume, now);
        activity.Record(
            "cell-9", "peer-b", "disk-2", "client-y",
            EBlockStoreRequest::UnmountVolume, now);

        auto rows = activity.Snapshot(now);
        UNIT_ASSERT_VALUES_EQUAL(2, rows.size());

        // rows for the same source collapse and their counters add up
        const auto* seven = FindIfPtr(
            rows, [](const auto& row) { return row.CellId == "cell-7"; });
        UNIT_ASSERT(seven);
        UNIT_ASSERT_VALUES_EQUAL("disk-1", seven->DiskId);
        UNIT_ASSERT_VALUES_EQUAL(2, seven->Mounts);
        UNIT_ASSERT_VALUES_EQUAL(0, seven->Unmounts);
    }

    Y_UNIT_TEST(ShouldPruneRowsPastTheTtl)
    {
        TCellInboundActivity activity;
        const auto start = TInstant::Seconds(100);

        activity.Record(
            "cell-7", "peer-a", "disk-1", "client-x",
            EBlockStoreRequest::MountVolume, start);

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
