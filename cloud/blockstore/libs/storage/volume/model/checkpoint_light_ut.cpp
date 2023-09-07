#include "checkpoint_light.h"

#include <library/cpp/testing/unittest/registar.h>


namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TCheckpointLightTests)
{
    Y_UNIT_TEST(ShouldFullFirstCheckpoint)
    {
        const int blocksCount = 10;

        TCheckpointLight checkpoint(blocksCount);
        {
            const auto& data = checkpoint.GetCheckpointData();
            UNIT_ASSERT_VALUES_EQUAL(data.Count(), blocksCount);
            for (int i = 0; i < blocksCount; ++i) {
                UNIT_ASSERT(data.Test(i));
            }
        }

        checkpoint.CreateCheckpoint("Checkpoint_1");

        {
            const auto& data = checkpoint.GetCheckpointData();
            UNIT_ASSERT_VALUES_EQUAL(data.Count(), blocksCount);
            for (int i = 0; i < blocksCount; ++i) {
                UNIT_ASSERT(data.Test(i));
            }
        }
    }

    Y_UNIT_TEST(ShouldDiffCheckpoint)
    {
        const int blocksCount = 10;
        const std::pair fillRange{1, 4};

        TCheckpointLight checkpoint(blocksCount);
        checkpoint.CreateCheckpoint("Checkpoint_1");

        checkpoint.Set(fillRange.first, fillRange.second);
        checkpoint.CreateCheckpoint("Checkpoint_2");
        {
            const auto& data = checkpoint.GetCheckpointData();
            UNIT_ASSERT_VALUES_EQUAL(
                data.Count(),
                fillRange.second - fillRange.first);
            for (int i = 0; i < blocksCount; ++i) {
                if (i >= fillRange.first && i < fillRange.second) {
                    UNIT_ASSERT(data.Test(i));
                } else {
                    UNIT_ASSERT(!data.Test(i));
                }
            }
        }
    }

    Y_UNIT_TEST(ShouldFillFirstCheckpointAfterInitialCheckpoint)
    {
        const int blocksCount = 10;

        TCheckpointLight checkpoint(blocksCount, "Checkpoint_1");

        {
            const auto& data = checkpoint.GetCheckpointData();
            UNIT_ASSERT_VALUES_EQUAL(data.Count(), blocksCount);
            for (int i = 0; i < blocksCount; ++i) {
                UNIT_ASSERT(data.Test(i));
            }
        }

        {
            const std::pair fillRange{1, 4};

            checkpoint.Set(fillRange.first, fillRange.second);
            checkpoint.CreateCheckpoint("Checkpoint_2");

            const auto& data = checkpoint.GetCheckpointData();
            UNIT_ASSERT_VALUES_EQUAL(data.Count(), blocksCount);
            for (int i = 0; i < blocksCount; ++i) {
                UNIT_ASSERT(data.Test(i));
            }
        }

        {
            const std::pair fillRange{5, 7};

            checkpoint.Set(fillRange.first, fillRange.second);
            checkpoint.CreateCheckpoint("Checkpoint_3");

            const auto& data = checkpoint.GetCheckpointData();
            UNIT_ASSERT_VALUES_EQUAL(
                data.Count(),
                fillRange.second - fillRange.first);
            for (int i = 0; i < blocksCount; ++i) {
                if (i >= fillRange.first && i < fillRange.second) {
                    UNIT_ASSERT(data.Test(i));
                } else {
                    UNIT_ASSERT(!data.Test(i));
                }
            }
        }
    }
}

}   // namespace NCloud::NBlockStore::NStorage
