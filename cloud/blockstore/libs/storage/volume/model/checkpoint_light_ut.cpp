#include "checkpoint_light.h"

#include <cloud/storage/core/libs/common/error.h>

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
            const auto& data = checkpoint.GetCurrentDirtyBlocks();
            UNIT_ASSERT_VALUES_EQUAL(data.Count(), blocksCount);
            for (int i = 0; i < blocksCount; ++i) {
                UNIT_ASSERT(data.Test(i));
            }
        }

        checkpoint.CreateCheckpoint("Checkpoint_1");

        {
            const auto& data = checkpoint.GetCurrentDirtyBlocks();
            UNIT_ASSERT_VALUES_EQUAL(data.Count(), blocksCount);
            for (int i = 0; i < blocksCount; ++i) {
                UNIT_ASSERT(data.Test(i));
            }
        }
    }

    Y_UNIT_TEST(ShouldDiffCheckpoint)
    {
        const int blocksCount = 10;
        const auto& fillRange = TBlockRange64::MakeHalfOpenInterval(1, 4);

        TCheckpointLight checkpoint(blocksCount);
        checkpoint.CreateCheckpoint("Checkpoint_1");

        checkpoint.Set(fillRange);
        checkpoint.CreateCheckpoint("Checkpoint_2");
        {
            const auto& data = checkpoint.GetCurrentDirtyBlocks();
            UNIT_ASSERT_VALUES_EQUAL(
                data.Count(),
                fillRange.Size());
            for (ui64 i = 0; i < blocksCount; ++i) {
                if (i >= fillRange.Start && i <= fillRange.End) {
                    UNIT_ASSERT(data.Test(i));
                } else {
                    UNIT_ASSERT(!data.Test(i));
                }
            }
        }
    }

    Y_UNIT_TEST(ShouldValidateBlockRange)
    {
        const int blocksCount = 10;
        TCheckpointLight checkpoint(blocksCount);
        checkpoint.CreateCheckpoint("Checkpoint_1");
        checkpoint.CreateCheckpoint("Checkpoint_2");

        UNIT_ASSERT(!SUCCEEDED(checkpoint.FindDirtyBlocksBetweenCheckpoints(
            TBlockRange64::MakeHalfOpenInterval(0, 11),
            nullptr).GetCode()));
        UNIT_ASSERT(!SUCCEEDED(checkpoint.FindDirtyBlocksBetweenCheckpoints(
            TBlockRange64::MakeHalfOpenInterval(5, 12),
            nullptr).GetCode()));

        TString mask;
        UNIT_ASSERT_VALUES_EQUAL(S_OK, checkpoint.FindDirtyBlocksBetweenCheckpoints(
            TBlockRange64::MakeHalfOpenInterval(0, 10),
            &mask).GetCode());
        UNIT_ASSERT_VALUES_EQUAL(S_OK, checkpoint.FindDirtyBlocksBetweenCheckpoints(
            TBlockRange64::MakeHalfOpenInterval(3, 5),
            &mask).GetCode());
    }

    Y_UNIT_TEST(ShouldReturnDirtyBlocksBetweenCheckpoints)
    {
        const int blocksCount = 10;
        TCheckpointLight checkpoint(blocksCount);

        TString mask;

        checkpoint.CreateCheckpoint("Checkpoint_1");
        checkpoint.Set(TBlockRange64::MakeHalfOpenInterval(2, 4));
        checkpoint.Set(TBlockRange64::MakeHalfOpenInterval(6, 9));
        checkpoint.CreateCheckpoint("Checkpoint_2");

        {
            auto error = checkpoint.FindDirtyBlocksBetweenCheckpoints(TBlockRange64::MakeHalfOpenInterval(0, 10), &mask);
            UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
            UNIT_ASSERT_VALUES_EQUAL("\xCC\x01", mask); // 0011001110
        }
        {
            auto error = checkpoint.FindDirtyBlocksBetweenCheckpoints(TBlockRange64::MakeHalfOpenInterval(3, 9), &mask);
            UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
            UNIT_ASSERT_VALUES_EQUAL("\x39", mask); // 100111
        }
        {
            auto error = checkpoint.FindDirtyBlocksBetweenCheckpoints(TBlockRange64::MakeHalfOpenInterval(0, 8), &mask);
            UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
            UNIT_ASSERT_VALUES_EQUAL("\xCC", mask); // 00110011
        }
        {
            auto error = checkpoint.FindDirtyBlocksBetweenCheckpoints(TBlockRange64::MakeHalfOpenInterval(8, 10), &mask);
            UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
            UNIT_ASSERT_VALUES_EQUAL("\x01", mask); // 10
        }
    }
}

}   // namespace NCloud::NBlockStore::NStorage
