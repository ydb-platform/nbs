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
        const auto& fillRange = TBlockRange64::WithLength(1, 3);

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

        TString mask;
        UNIT_ASSERT(!SUCCEEDED(checkpoint.FindDirtyBlocksBetweenCheckpoints(
            "Checkpoint_1",
            "Checkpoint_2",
            TBlockRange64::WithLength(0, 11),
            &mask).GetCode()));
        UNIT_ASSERT(!SUCCEEDED(checkpoint.FindDirtyBlocksBetweenCheckpoints(
            "Checkpoint_1",
            "Checkpoint_2",
            TBlockRange64::WithLength(5, 7),
            &mask).GetCode()));

        UNIT_ASSERT_VALUES_EQUAL(S_OK, checkpoint.FindDirtyBlocksBetweenCheckpoints(
            "Checkpoint_1",
            "Checkpoint_2",
            TBlockRange64::WithLength(0, 10),
            &mask).GetCode());
        UNIT_ASSERT_VALUES_EQUAL(S_OK, checkpoint.FindDirtyBlocksBetweenCheckpoints(
            "Checkpoint_1",
            "Checkpoint_2",
            TBlockRange64::WithLength(3, 2),
            &mask).GetCode());
    }

    Y_UNIT_TEST(ShouldReturnDirtyBlocksBetweenCheckpoints)
    {
        const int blocksCount = 10;
        TCheckpointLight checkpoint(blocksCount);

        const auto& blockRangeFull = TBlockRange64::WithLength(0, 10);

        checkpoint.CreateCheckpoint("Checkpoint_1");
        checkpoint.Set(TBlockRange64::WithLength(2, 2));
        checkpoint.Set(TBlockRange64::WithLength(6, 3));
        checkpoint.CreateCheckpoint("Checkpoint_2");

        {
            TString mask;
            auto error = checkpoint.FindDirtyBlocksBetweenCheckpoints(
                "Checkpoint_1",
                "Checkpoint_2",
                blockRangeFull,
                &mask);
            UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
            UNIT_ASSERT_VALUES_EQUAL(0b11001100, ui8(mask[0]));
            UNIT_ASSERT_VALUES_EQUAL(0b00000001, ui8(mask[1]));
        }
        {
            TString mask;
            auto error = checkpoint.FindDirtyBlocksBetweenCheckpoints(
                "Checkpoint_1",
                "Checkpoint_2",
                TBlockRange64::WithLength(3, 6),
                &mask);
            UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
            UNIT_ASSERT_VALUES_EQUAL(0b00111001, ui8(mask[0]));
        }
        {
            TString mask;
            auto error = checkpoint.FindDirtyBlocksBetweenCheckpoints(
                "Checkpoint_1",
                "Checkpoint_2",
                TBlockRange64::WithLength(0, 8),
                &mask);
            UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
            UNIT_ASSERT_VALUES_EQUAL(0b11001100, ui8(mask[0]));
        }
        {
            TString mask;
            auto error = checkpoint.FindDirtyBlocksBetweenCheckpoints(
                "Checkpoint_1",
                "Checkpoint_2",
                TBlockRange64::WithLength(8, 2),
                &mask);
            UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
            UNIT_ASSERT_VALUES_EQUAL(0b00000001, ui8(mask[0]));
        }
        {
            TString mask;
            auto error = checkpoint.FindDirtyBlocksBetweenCheckpoints(
                "Checkpoint_2",
                "",
                blockRangeFull,
                &mask);
            UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
            UNIT_ASSERT_VALUES_EQUAL(0, ui8(mask[0]));
        }

        {
            TString mask;
            auto error = checkpoint.FindDirtyBlocksBetweenCheckpoints(
                "",
                "Checkpoint_2",
                blockRangeFull,
                &mask);
            UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
            UNIT_ASSERT_VALUES_EQUAL(0b11111111, ui8(mask[0]));
            UNIT_ASSERT_VALUES_EQUAL(0b00000011, ui8(mask[1]));
        }
        {
            TString mask;
            auto error = checkpoint.FindDirtyBlocksBetweenCheckpoints(
                "Checkpoint_x",
                "Checkpoint_2",
                blockRangeFull,
                &mask);
            UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
            UNIT_ASSERT_VALUES_EQUAL(0b11111111, ui8(mask[0]));
            UNIT_ASSERT_VALUES_EQUAL(0b00000011, ui8(mask[1]));
        }
        {
            TString mask;
            auto error = checkpoint.FindDirtyBlocksBetweenCheckpoints(
                "Checkpoint_1",
                "Checkpoint_x",
                blockRangeFull,
                &mask);
            UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
            UNIT_ASSERT_VALUES_EQUAL(0b11111111, ui8(mask[0]));
            UNIT_ASSERT_VALUES_EQUAL(0b00000011, ui8(mask[1]));
        }
        {
            TString mask;
            auto error = checkpoint.FindDirtyBlocksBetweenCheckpoints(
                "Checkpoint_2",
                "Checkpoint_1",
                blockRangeFull,
                &mask);
            UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
            UNIT_ASSERT_VALUES_EQUAL(0b11111111, ui8(mask[0]));
            UNIT_ASSERT_VALUES_EQUAL(0b00000011, ui8(mask[1]));
        }
        {
            TString mask;
            auto error = checkpoint.FindDirtyBlocksBetweenCheckpoints(
                "",
                "",
                blockRangeFull,
                &mask);
            UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
            UNIT_ASSERT_VALUES_EQUAL(0b11111111, ui8(mask[0]));
            UNIT_ASSERT_VALUES_EQUAL(0b00000011, ui8(mask[1]));
        }
        {
            TString mask;
            auto error = checkpoint.FindDirtyBlocksBetweenCheckpoints(
                "Checkpoint_1",
                "Checkpoint_1",
                blockRangeFull,
                &mask);
            UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
            UNIT_ASSERT_VALUES_EQUAL(0b11111111, ui8(mask[0]));
            UNIT_ASSERT_VALUES_EQUAL(0b00000011, ui8(mask[1]));
        }
        {
            TString mask;
            auto error = checkpoint.FindDirtyBlocksBetweenCheckpoints(
                "Checkpoint_2",
                "Checkpoint_2",
                blockRangeFull,
                &mask);
            UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
            UNIT_ASSERT_VALUES_EQUAL(0b11111111, ui8(mask[0]));
            UNIT_ASSERT_VALUES_EQUAL(0b00000011, ui8(mask[1]));
        }
        {
            TString mask;
            auto error = checkpoint.FindDirtyBlocksBetweenCheckpoints(
                "Checkpoint_1",
                "",
                blockRangeFull,
                &mask);
            UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
            UNIT_ASSERT_VALUES_EQUAL(0b11001100, ui8(mask[0]));
            UNIT_ASSERT_VALUES_EQUAL(0b00000001, ui8(mask[1]));
        }

        checkpoint.DeleteCheckpoint("Checkpoint_1");
        {
            TString mask;
            auto error = checkpoint.FindDirtyBlocksBetweenCheckpoints(
                "",
                "Checkpoint_2",
                blockRangeFull,
                &mask);
            UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
            UNIT_ASSERT_VALUES_EQUAL(0b11111111, ui8(mask[0]));
            UNIT_ASSERT_VALUES_EQUAL(0b00000011, ui8(mask[1]));
        }
        {
            TString mask;
            auto error = checkpoint.FindDirtyBlocksBetweenCheckpoints(
                "Checkpoint_1",
                "Checkpoint_2",
                blockRangeFull,
                &mask);
            UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
            UNIT_ASSERT_VALUES_EQUAL(0b11111111, ui8(mask[0]));
            UNIT_ASSERT_VALUES_EQUAL(0b00000011, ui8(mask[1]));
        }
    }
}

}   // namespace NCloud::NBlockStore::NStorage
