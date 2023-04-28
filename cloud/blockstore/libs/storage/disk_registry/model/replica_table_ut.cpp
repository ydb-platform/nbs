#include "replica_table.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NBlockStore::NStorage {

namespace {

////////////////////////////////////////////////////////////////////////////////

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TReplicaTableTest)
{
    Y_UNIT_TEST(ShouldTryReplaceDevice)
    {
        TReplicaTable t;
        t.AddReplica("disk-1", {"d1-1", "d1-2", "d1-3", "d1-4"});
        t.AddReplica("disk-1", {"d2-1", "d2-2", "d2-3", "d2-4"});
        t.AddReplica("disk-1", {"d3-1", "d3-2", "d3-3", "d3-4"});
        t.MarkReplacementDevice("disk-1", "d2-2", true);
        UNIT_ASSERT(t.IsReplacementAllowed("disk-1", "d1-2"));
        UNIT_ASSERT(t.ReplaceDevice("disk-1", "d1-2", "d1-2'"));
        UNIT_ASSERT(!t.IsReplacementAllowed("disk-1", "d1-2"));
        UNIT_ASSERT(!t.ReplaceDevice("disk-1", "d1-2", "d1-2'"));
        UNIT_ASSERT(!t.IsReplacementAllowed("disk-1", "d3-2"));
        UNIT_ASSERT(t.IsReplacementAllowed("disk-1", "d1-2'"));
        UNIT_ASSERT(t.ReplaceDevice("disk-1", "d1-2'", "d1-2''"));
        UNIT_ASSERT(t.IsReplacementAllowed("disk-1", "d2-2"));
        UNIT_ASSERT(t.ReplaceDevice("disk-1", "d2-2", "d2-2'"));
        UNIT_ASSERT(!t.IsReplacementAllowed("disk-1", "d3-2"));
        t.MarkReplacementDevice("disk-1", "d2-2'", false);
        UNIT_ASSERT(t.IsReplacementAllowed("disk-1", "d3-2"));
        UNIT_ASSERT(t.ReplaceDevice("disk-1", "d3-2", "d3-2'"));

        UNIT_ASSERT(t.RemoveMirroredDisk("disk-1"));
        UNIT_ASSERT(!t.RemoveMirroredDisk("disk-1"));
    }

    Y_UNIT_TEST(ShouldCalculateReplicaStats)
    {
        TReplicaTable t;
        t.AddReplica("disk-1", {"d1-1-1", "d1-1-2", "d1-1-3", "d1-1-4"});
        t.AddReplica("disk-1", {"d1-2-1", "d1-2-2", "d1-2-3", "d1-2-4"});
        t.AddReplica("disk-1", {"d1-3-1", "d1-3-2", "d1-3-3", "d1-3-4"});
        t.AddReplica("disk-2", {"d2-1-1", "d2-1-2", "d2-1-3", "d2-1-4"});
        t.AddReplica("disk-2", {"d2-2-1", "d2-2-2", "d2-2-3", "d2-2-4"});

        auto replicaCountStats = t.CalculateReplicaCountStats();
        UNIT_ASSERT_VALUES_EQUAL(1, replicaCountStats[3][0]);
        UNIT_ASSERT_VALUES_EQUAL(0, replicaCountStats[3][1]);
        UNIT_ASSERT_VALUES_EQUAL(0, replicaCountStats[3][2]);
        UNIT_ASSERT_VALUES_EQUAL(0, replicaCountStats[3][3]);
        UNIT_ASSERT_VALUES_EQUAL(1, replicaCountStats[2][0]);
        UNIT_ASSERT_VALUES_EQUAL(0, replicaCountStats[2][1]);
        UNIT_ASSERT_VALUES_EQUAL(0, replicaCountStats[2][2]);

        t.MarkReplacementDevice("disk-1", "d1-2-2", true);

        replicaCountStats = t.CalculateReplicaCountStats();
        UNIT_ASSERT_VALUES_EQUAL(0, replicaCountStats[3][0]);
        UNIT_ASSERT_VALUES_EQUAL(1, replicaCountStats[3][1]);
        UNIT_ASSERT_VALUES_EQUAL(0, replicaCountStats[3][2]);
        UNIT_ASSERT_VALUES_EQUAL(0, replicaCountStats[3][3]);
        UNIT_ASSERT_VALUES_EQUAL(1, replicaCountStats[2][0]);
        UNIT_ASSERT_VALUES_EQUAL(0, replicaCountStats[2][1]);
        UNIT_ASSERT_VALUES_EQUAL(0, replicaCountStats[2][2]);

        t.MarkReplacementDevice("disk-1", "d1-1-1", true);
        t.MarkReplacementDevice("disk-1", "d1-1-2", true);
        t.MarkReplacementDevice("disk-1", "d1-2-3", true);
        t.MarkReplacementDevice("disk-1", "d1-3-1", true);

        t.MarkReplacementDevice("disk-2", "d2-2-1", true);

        replicaCountStats = t.CalculateReplicaCountStats();
        UNIT_ASSERT_VALUES_EQUAL(0, replicaCountStats[3][0]);
        UNIT_ASSERT_VALUES_EQUAL(0, replicaCountStats[3][1]);
        UNIT_ASSERT_VALUES_EQUAL(1, replicaCountStats[3][2]);
        UNIT_ASSERT_VALUES_EQUAL(0, replicaCountStats[3][3]);
        UNIT_ASSERT_VALUES_EQUAL(0, replicaCountStats[2][0]);
        UNIT_ASSERT_VALUES_EQUAL(1, replicaCountStats[2][1]);
        UNIT_ASSERT_VALUES_EQUAL(0, replicaCountStats[2][2]);

        t.MarkReplacementDevice("disk-1", "d1-2-1", true);

        replicaCountStats = t.CalculateReplicaCountStats();
        UNIT_ASSERT_VALUES_EQUAL(0, replicaCountStats[3][0]);
        UNIT_ASSERT_VALUES_EQUAL(0, replicaCountStats[3][1]);
        UNIT_ASSERT_VALUES_EQUAL(0, replicaCountStats[3][2]);
        UNIT_ASSERT_VALUES_EQUAL(1, replicaCountStats[3][3]);
        UNIT_ASSERT_VALUES_EQUAL(0, replicaCountStats[2][0]);
        UNIT_ASSERT_VALUES_EQUAL(1, replicaCountStats[2][1]);
        UNIT_ASSERT_VALUES_EQUAL(0, replicaCountStats[2][2]);
    }
}

}   // namespace NCloud::NBlockStore::NStorage
