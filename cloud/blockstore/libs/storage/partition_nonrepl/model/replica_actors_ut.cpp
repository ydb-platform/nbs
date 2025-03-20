#include "replica_actors.h"

#include <cloud/blockstore/libs/storage/testlib/ut_helpers.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TReplicaActorsTest)
{
    Y_UNIT_TEST(Test)
    {
        TReplicaActors replicaActors;
        UNIT_ASSERT_VALUES_EQUAL(0, replicaActors.Size());

        auto actor1 = TActorId{0, "test1"};
        auto actor2 = TActorId{0, "test2"};
        auto actor3 = TActorId{0, "test3"};
        replicaActors.AddReplicaActor(actor1);
        UNIT_ASSERT_VALUES_EQUAL(1, replicaActors.Size());
        replicaActors.AddReplicaActor(actor2);
        UNIT_ASSERT_VALUES_EQUAL(2, replicaActors.Size());
        replicaActors.AddReplicaActor(actor3);
        UNIT_ASSERT_VALUES_EQUAL(3, replicaActors.Size());

        auto expectedVector = TVector<TActorId>({actor1, actor2, actor3});
        ASSERT_VECTORS_EQUAL(expectedVector, replicaActors.GetReplicaActors());
        ASSERT_VECTORS_EQUAL(expectedVector, replicaActors.GetReplicaActorsBypassingProxies());

        UNIT_ASSERT_VALUES_EQUAL(actor1, replicaActors.GetReplicaActor(0));
        UNIT_ASSERT_VALUES_EQUAL(actor2, replicaActors.GetReplicaActor(1));
        UNIT_ASSERT_VALUES_EQUAL(actor3, replicaActors.GetReplicaActor(2));
        UNIT_ASSERT_VALUES_EQUAL(0, replicaActors.GetReplicaIndex(actor1));
        UNIT_ASSERT_VALUES_EQUAL(1, replicaActors.GetReplicaIndex(actor2));
        UNIT_ASSERT_VALUES_EQUAL(2, replicaActors.GetReplicaIndex(actor3));
        UNIT_ASSERT_VALUES_EQUAL(
            Max<ui32>(),
            replicaActors.GetReplicaIndex(TActorId{0, "unknown"}));

        UNIT_ASSERT_VALUES_EQUAL(0, replicaActors.LaggingReplicaCount());

        auto laggingActor3 = TActorId{0, "laggingTest3"};
        replicaActors.SetLaggingReplicaProxy(2, laggingActor3);

        UNIT_ASSERT_VALUES_EQUAL(laggingActor3, replicaActors.GetReplicaActor(2));
        UNIT_ASSERT_VALUES_EQUAL(2, replicaActors.GetReplicaIndex(laggingActor3));
        UNIT_ASSERT_VALUES_EQUAL(2, replicaActors.GetReplicaIndex(actor3));
        UNIT_ASSERT_VALUES_EQUAL(1, replicaActors.LaggingReplicaCount());

        auto laggingActor1 = TActorId{0, "laggingTest1"};
        replicaActors.SetLaggingReplicaProxy(0, laggingActor1);

        UNIT_ASSERT_VALUES_EQUAL(laggingActor1, replicaActors.GetReplicaActor(0));
        UNIT_ASSERT_VALUES_EQUAL(0, replicaActors.GetReplicaIndex(laggingActor1));
        UNIT_ASSERT_VALUES_EQUAL(0, replicaActors.GetReplicaIndex(actor1));
        UNIT_ASSERT_VALUES_EQUAL(2, replicaActors.LaggingReplicaCount());
        UNIT_ASSERT_VALUES_EQUAL(3, replicaActors.Size());

        replicaActors.ResetLaggingReplicaProxy(2);
        UNIT_ASSERT_VALUES_EQUAL(actor3, replicaActors.GetReplicaActor(2));
        UNIT_ASSERT_VALUES_EQUAL(Max<ui32>(), replicaActors.GetReplicaIndex(laggingActor3));
        UNIT_ASSERT_VALUES_EQUAL(2, replicaActors.GetReplicaIndex(actor3));
        UNIT_ASSERT_VALUES_EQUAL(1, replicaActors.LaggingReplicaCount());
        UNIT_ASSERT_VALUES_EQUAL(3, replicaActors.Size());

        replicaActors.ResetLaggingReplicaProxy(0);
        UNIT_ASSERT_VALUES_EQUAL(actor1, replicaActors.GetReplicaActor(0));
        UNIT_ASSERT_VALUES_EQUAL(Max<ui32>(), replicaActors.GetReplicaIndex(laggingActor1));
        UNIT_ASSERT_VALUES_EQUAL(0, replicaActors.GetReplicaIndex(actor1));
        UNIT_ASSERT_VALUES_EQUAL(0, replicaActors.LaggingReplicaCount());
        UNIT_ASSERT_VALUES_EQUAL(3, replicaActors.Size());
    }
}

}   // namespace NCloud::NBlockStore::NStorage
