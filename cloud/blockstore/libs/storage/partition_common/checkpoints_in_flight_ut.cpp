#include "checkpoints_in_flight.h"

#include <cloud/blockstore/libs/storage/core/tablet.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TCheckpointsInFlightTest)
{
    Y_UNIT_TEST(ShouldTrackInFlightCheckpoints)
    {
        TCheckpointsInFlight inFlight;

        UNIT_ASSERT(!inFlight.HasCheckpoint("cp1"));
        UNIT_ASSERT(!inFlight.HasCheckpoint("cp2"));

        bool added = inFlight.AddTx("cp1", nullptr, 10);
        UNIT_ASSERT(added);
        UNIT_ASSERT(inFlight.HasCheckpoint("cp1"));
        UNIT_ASSERT(!inFlight.HasCheckpoint("cp2"));


        added = inFlight.AddTx("cp2", nullptr);
        UNIT_ASSERT(added);
        UNIT_ASSERT(inFlight.HasCheckpoint("cp1"));
        UNIT_ASSERT(inFlight.HasCheckpoint("cp2"));

        added = inFlight.AddTx("cp1", nullptr, 13);
        UNIT_ASSERT(!added);

        added = inFlight.AddTx("cp2", nullptr, 14);
        UNIT_ASSERT(!added);

        inFlight.PopTx("cp1");
        UNIT_ASSERT(!inFlight.HasCheckpoint("cp1"));
        UNIT_ASSERT(inFlight.HasCheckpoint("cp2"));

        inFlight.PopTx("cp2");
        UNIT_ASSERT(!inFlight.HasCheckpoint("cp2"));
    }
}

}   // namespace NCloud::NBlockStore::NStorage
