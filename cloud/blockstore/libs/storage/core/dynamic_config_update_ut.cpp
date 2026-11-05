#include "dynamic_config_update.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NBlockStore::NStorage {

Y_UNIT_TEST_SUITE(TDynamicConfigUpdateTest)
{
    // Verify that one consumer accepts increasing revisions from its first
    // owner.
    Y_UNIT_TEST(ShouldRejectStaleAndForeignUpdates)
    {
        // Bind the consumer to a Volume, allowing its initial revision zero.
        TDynamicConfigUpdateState state;
        const NActors::TActorId owner(0, "volume");
        UNIT_ASSERT(!state.TryUpdate({}, 1));
        UNIT_ASSERT(state.TryUpdate(owner, 0));

        // Preserve the accepted publication on duplicates and older messages.
        UNIT_ASSERT(!state.TryUpdate(owner, 0));
        UNIT_ASSERT(state.TryUpdate(owner, 2));
        UNIT_ASSERT(!state.TryUpdate(owner, 1));
        UNIT_ASSERT(!state.TryUpdate(owner, 2));

        // Reject another Volume even if its local revision is larger.
        UNIT_ASSERT(!state.TryUpdate(NActors::TActorId(0, "other"), 100));
        UNIT_ASSERT(state.TryUpdate(owner, 3));
    }
}

}   // namespace NCloud::NBlockStore::NStorage
