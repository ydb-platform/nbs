#include "vdisk_huge_slots.h"

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/size_literals.h>

namespace NCloud::NStorage {

namespace {

////////////////////////////////////////////////////////////////////////////////
// Part size calculators which reproduce TErasureType::PartSize(CrcModeNone).

// ErasureNone, mirror-3, mirror-3-dc: the part is the whole blob.
ui64 MirrorPartSize(ui64 blobSize)
{
    return blobSize;
}

// Parity erasures: the blob is split into MinimalBlockSize-sized blocks and
// each block contributes (Prime - 1) * sizeof(ui64) bytes to every part.
// block-4-2: DataParts = 4, Prime = 5 => MinimalBlockSize = 128, 32 bytes per
// block per part => a part is exactly 1/4 of a 128-byte-aligned blob.
ui64 Block42PartSize(ui64 blobSize)
{
    const ui64 minimalBlockSize = (5 - 1) * 4 * sizeof(ui64);
    const ui64 blocks = (blobSize + minimalBlockSize - 1) / minimalBlockSize;
    return blocks * sizeof(ui64) * (5 - 1);
}

// block-3-2 (and stripe-3-2): DataParts = 3, Prime = 3 => MinimalBlockSize =
// 48, 16 bytes per block per part => a part is 1/3 of a 48-byte-aligned blob.
ui64 Block32PartSize(ui64 blobSize)
{
    const ui64 minimalBlockSize = (3 - 1) * 3 * sizeof(ui64);
    const ui64 blocks = (blobSize + minimalBlockSize - 1) / minimalBlockSize;
    return blocks * sizeof(ui64) * (3 - 1);
}

ui32 Adjust(
    const TVDiskHugeSlots& slots,
    const TPartSizeCalculator& calc,
    ui32 blockSize,
    ui32 maxBlobSize,
    ui32 maxPadding = 5)
{
    return AdjustMaxBlocksInBlobToVDiskSlots(
        slots,
        calc,
        blockSize,
        maxBlobSize / blockSize,
        maxPadding);
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TVDiskHugeSlotsTest)
{
    Y_UNIT_TEST(ShouldBuildSlotLadder)
    {
        const TVDiskHugeSlots slots;
        const auto& sizes = slots.GetSlotSizes();

        // The values are taken from the chain table of a real VDisk
        // (MinHugeBlobInBytes = 64KiB, MilestoneHugeBlobInBytes = 512KiB,
        // HugeBlobOverhead = 8, AppendBlockSize = 4064).
        // clang-format off
        const TVector<ui64> expectedPrefixInBlocks = {
            18, 21, 24, 28, 32, 37, 42, 48, 55, 62, 70, 79, 89, 101, 114,
            // milestone - 512KiB = 129 blocks
            129,
            145, 163, 183, 205, 230, 258, 290, 326, 366, 411, 462, 519, 583,
            655, 736, 828, 931, 1047, 1177,
        };
        // clang-format on

        UNIT_ASSERT(sizes.size() > expectedPrefixInBlocks.size());
        for (size_t i = 0; i < expectedPrefixInBlocks.size(); ++i) {
            UNIT_ASSERT_VALUES_EQUAL_C(
                expectedPrefixInBlocks[i] * 4064,
                sizes[i],
                "slot #" << i);
        }

        // the ladder must cover MaxHugeBlobInBytes
        UNIT_ASSERT(sizes.back() >= slots.GetLayout().MaxHugeBlobInBytes);
    }

    Y_UNIT_TEST(ShouldBuildSlotLadderForRotationalDisks)
    {
        // For ROT disks MinHugeBlobInBytes == MilestoneHugeBlobInBytes =>
        // there is no downward part of the ladder, the upward part is the
        // same.
        const TVDiskHugeSlots ssd;
        const TVDiskHugeSlots rot(
            TVDiskHugeSlotLayout{.MinHugeBlobInBytes = 512_KB});

        const auto& ssdSizes = ssd.GetSlotSizes();
        const auto& rotSizes = rot.GetSlotSizes();

        UNIT_ASSERT_VALUES_EQUAL(145 * 4064, rotSizes[0]);
        UNIT_ASSERT(ssdSizes.size() > rotSizes.size());
        const size_t offset = ssdSizes.size() - rotSizes.size();
        for (size_t i = 0; i < rotSizes.size(); ++i) {
            UNIT_ASSERT_VALUES_EQUAL(ssdSizes[offset + i], rotSizes[i]);
        }

        // 1MiB parts are placed identically
        UNIT_ASSERT_VALUES_EQUAL(ssd.GetSlotSize(1_MB), rot.GetSlotSize(1_MB));
    }

    Y_UNIT_TEST(ShouldFindSlotForPart)
    {
        const TVDiskHugeSlots slots;

        // parts smaller than MinHugeBlobInBytes (header included) are not
        // huge
        UNIT_ASSERT_VALUES_EQUAL(0, slots.GetSlotSize(0));
        UNIT_ASSERT_VALUES_EQUAL(0, slots.GetSlotSize(64_KB - 6));
        // 64KiB - 5 + 5 bytes of header == MinHugeBlobInBytes => 17 blocks
        // => (16, 18] => 18 blocks
        UNIT_ASSERT_VALUES_EQUAL(18 * 4064, slots.GetSlotSize(64_KB - 5));

        // The example from the issue: a 1MiB part (+5 bytes of header) takes
        // 259 blocks => (258, 290] => 290 blocks = 1178560 bytes ~ 1150KiB
        UNIT_ASSERT_VALUES_EQUAL(290 * 4064, slots.GetSlotSize(1_MB));
        UNIT_ASSERT_VALUES_EQUAL(1178560, slots.GetSlotSize(1_MB));
        UNIT_ASSERT_VALUES_EQUAL(
            11,
            slots.CalculateSlotPaddingPercentage(1_MB));

        // 258 blocks hold 1048512 bytes => a part of 1048507 bytes fits
        UNIT_ASSERT_VALUES_EQUAL(258 * 4064, slots.GetSlotSize(1048507));
        UNIT_ASSERT_VALUES_EQUAL(
            0,
            slots.CalculateSlotPaddingPercentage(1048507));
        // and 1 byte more doesn't
        UNIT_ASSERT_VALUES_EQUAL(290 * 4064, slots.GetSlotSize(1048508));

        // 4MiB part (mirror-3-dc) => 1033 blocks => (931, 1047]
        UNIT_ASSERT_VALUES_EQUAL(1047 * 4064, slots.GetSlotSize(4_MB));
        UNIT_ASSERT_VALUES_EQUAL(1, slots.CalculateSlotPaddingPercentage(4_MB));

        // exact slot boundaries: a part of (slot - header) bytes is the
        // largest part that fits into the slot, 1 byte more => next slot
        const auto& sizes = slots.GetSlotSizes();
        for (size_t i = 0; i < sizes.size(); ++i) {
            UNIT_ASSERT_VALUES_EQUAL(sizes[i], slots.GetSlotSize(sizes[i] - 5));
            if (i + 1 < sizes.size()) {
                UNIT_ASSERT_VALUES_EQUAL(
                    sizes[i + 1],
                    slots.GetSlotSize(sizes[i] - 5 + 1));
            }
            UNIT_ASSERT_VALUES_EQUAL(
                i ? sizes[i - 1] : 0,
                slots.GetPrevSlotSize(sizes[i]));
        }

        // too large parts
        UNIT_ASSERT_VALUES_EQUAL(0, slots.GetSlotSize(1_GB));
    }

    Y_UNIT_TEST(ShouldFindPrevSlot)
    {
        const TVDiskHugeSlots slots;

        UNIT_ASSERT_VALUES_EQUAL(0, slots.GetPrevSlotSize(0));
        UNIT_ASSERT_VALUES_EQUAL(0, slots.GetPrevSlotSize(18 * 4064));
        UNIT_ASSERT_VALUES_EQUAL(18 * 4064, slots.GetPrevSlotSize(21 * 4064));
        UNIT_ASSERT_VALUES_EQUAL(258 * 4064, slots.GetPrevSlotSize(290 * 4064));
        // not a slot boundary => the largest slot below the value
        UNIT_ASSERT_VALUES_EQUAL(
            258 * 4064,
            slots.GetPrevSlotSize(290 * 4064 - 1));
    }

    Y_UNIT_TEST(ShouldAdjustBlobSizeForBlock42)
    {
        const TVDiskHugeSlots slots;

        // 4KiB blocks: 1024 blocks => 1MiB parts => 11% padding
        // 1023 blocks => 1047552 + 5 bytes => fits into the 258-block slot
        UNIT_ASSERT_VALUES_EQUAL(
            1023,
            Adjust(slots, Block42PartSize, 4_KB, 4_MB));
        UNIT_ASSERT_VALUES_EQUAL(
            0,
            slots.CalculateSlotPaddingPercentage(Block42PartSize(1023 * 4_KB)));

        // 16KiB blocks: 256 => 255 (1044480 + 5 bytes)
        UNIT_ASSERT_VALUES_EQUAL(
            255,
            Adjust(slots, Block42PartSize, 16_KB, 4_MB));
        // 64KiB blocks: 64 => 63 (1032192 + 5 bytes, 1.5% padding)
        UNIT_ASSERT_VALUES_EQUAL(
            63,
            Adjust(slots, Block42PartSize, 64_KB, 4_MB));
        // 128KiB blocks: 32 => 31 (1015808 + 5 bytes, 3.1% padding)
        UNIT_ASSERT_VALUES_EQUAL(
            31,
            Adjust(slots, Block42PartSize, 128_KB, 4_MB));
        // 128KiB blocks with a stricter limit: 31 (3.1%) => 28 (1.8%)
        UNIT_ASSERT_VALUES_EQUAL(
            28,
            Adjust(slots, Block42PartSize, 128_KB, 4_MB, 2));

        // the result never exceeds the configured value
        for (ui32 blocks = 1; blocks <= 1024; ++blocks) {
            const auto adjusted = AdjustMaxBlocksInBlobToVDiskSlots(
                slots,
                Block42PartSize,
                4_KB,
                blocks,
                5);
            UNIT_ASSERT_C(adjusted <= blocks, blocks << " -> " << adjusted);
            UNIT_ASSERT_C(adjusted >= blocks / 2, blocks << " -> " << adjusted);
            UNIT_ASSERT_C(adjusted > 0, blocks);
        }
    }

    Y_UNIT_TEST(ShouldAdjustBlobSizeForBlock32)
    {
        const TVDiskHugeSlots slots;

        // 4MiB blob => 1398112 + 5 bytes parts => 345 blocks => (326, 366]
        // => 1487424 bytes slot => 6% padding => shrink to the 326-block slot
        // (1324864 bytes): 1324864 - 5 = 1324859 bytes per part => 3974577
        // bytes blob => 970 4KiB blocks (3973120 bytes => 1324384 parts)
        UNIT_ASSERT_VALUES_EQUAL(
            6,
            slots.CalculateSlotPaddingPercentage(Block32PartSize(4_MB)));
        UNIT_ASSERT_VALUES_EQUAL(
            970,
            Adjust(slots, Block32PartSize, 4_KB, 4_MB));
        UNIT_ASSERT_VALUES_EQUAL(
            326 * 4064,
            slots.GetSlotSize(Block32PartSize(970 * 4_KB)));
        UNIT_ASSERT_VALUES_EQUAL(
            0,
            slots.CalculateSlotPaddingPercentage(Block32PartSize(970 * 4_KB)));

        // a lax limit keeps the configured value
        UNIT_ASSERT_VALUES_EQUAL(
            1024,
            Adjust(slots, Block32PartSize, 4_KB, 4_MB, 6));
    }

    Y_UNIT_TEST(ShouldKeepBlobSizeForMirrorIfPaddingIsSmall)
    {
        const TVDiskHugeSlots slots;

        // 4MiB parts => (931, 1047] => 1.4% padding => nothing to adjust
        UNIT_ASSERT_VALUES_EQUAL(
            1024,
            Adjust(slots, MirrorPartSize, 4_KB, 4_MB));
        UNIT_ASSERT_VALUES_EQUAL(
            32,
            Adjust(slots, MirrorPartSize, 128_KB, 4_MB));

        // with a zero limit the blob shrinks to the 931-block slot:
        // 3783584 - 5 bytes => 923 4KiB blocks
        UNIT_ASSERT_VALUES_EQUAL(
            923,
            Adjust(slots, MirrorPartSize, 4_KB, 4_MB, 0));

        // 1MiB blobs: 1MiB parts => the same 11% padding as for 4MiB blobs
        // in block-4-2 groups => 1023 blocks
        UNIT_ASSERT_VALUES_EQUAL(
            11,
            slots.CalculateSlotPaddingPercentage(MirrorPartSize(1_MB)));
        UNIT_ASSERT_VALUES_EQUAL(
            255,
            Adjust(slots, MirrorPartSize, 4_KB, 1_MB));
    }

    Y_UNIT_TEST(ShouldHandleSteepLayouts)
    {
        // An artificial layout with the largest possible overhead: the slot
        // sizes double: (1MiB, 2MiB], (2MiB, 4MiB], (4MiB, 8MiB], ... => a
        // 4MiB part + 5 bytes of header => 8MiB slot => ~50% padding => the
        // largest blob that fits into the 4MiB slot is 4MiB - 5 bytes =>
        // 1023 4KiB blocks.
        const TVDiskHugeSlots slots(TVDiskHugeSlotLayout{
            .AppendBlockSize = 1_MB,
            .MinHugeBlobInBytes = 1_MB,
            .MilestoneHugeBlobInBytes = 1_MB,
            .HugeBlobOverhead = 1,
            .MaxHugeBlobInBytes = 64_MB,
        });

        UNIT_ASSERT_VALUES_EQUAL(2_MB, slots.GetSlotSizes()[0]);
        UNIT_ASSERT_VALUES_EQUAL(8_MB, slots.GetSlotSize(4_MB));
        UNIT_ASSERT_VALUES_EQUAL(4_MB, slots.GetSlotSize(4_MB - 5));
        UNIT_ASSERT_VALUES_EQUAL(
            1023,
            Adjust(slots, MirrorPartSize, 4_KB, 4_MB));

        // without the header 4MiB parts fit into the 4MiB slot
        const TVDiskHugeSlots slotsWithoutHeader(TVDiskHugeSlotLayout{
            .AppendBlockSize = 1_MB,
            .MinHugeBlobInBytes = 1_MB,
            .MilestoneHugeBlobInBytes = 1_MB,
            .HugeBlobOverhead = 1,
            .MaxHugeBlobInBytes = 64_MB,
            .PartHeaderSize = 0,
        });
        UNIT_ASSERT_VALUES_EQUAL(
            1024,
            Adjust(slotsWithoutHeader, MirrorPartSize, 4_KB, 4_MB));

        // 4MiB + 1KiB blob (4097 1KiB blocks) => 8MiB slot (49% padding) =>
        // 4096 blocks (0% padding, 4096 >= 4097 / 2 => allowed)
        UNIT_ASSERT_VALUES_EQUAL(
            4096,
            AdjustMaxBlocksInBlobToVDiskSlots(
                slotsWithoutHeader,
                MirrorPartSize,
                1_KB,
                4097,
                5));
        // 2MiB + 1KiB blob => 4MiB slot (49% padding) => 2048 blocks
        UNIT_ASSERT_VALUES_EQUAL(
            2048,
            AdjustMaxBlocksInBlobToVDiskSlots(
                slotsWithoutHeader,
                MirrorPartSize,
                1_KB,
                2049,
                5));
        // 1MiB + 1KiB blob => 2MiB slot (49% padding) but 2MiB is the
        // smallest slot => there is nothing to descend to => the configured
        // value is kept
        UNIT_ASSERT_VALUES_EQUAL(
            1025,
            AdjustMaxBlocksInBlobToVDiskSlots(
                slotsWithoutHeader,
                MirrorPartSize,
                1_KB,
                1025,
                5));
    }

    Y_UNIT_TEST(ShouldNotShrinkBlobsMoreThanTwice)
    {
        // Doubling slots, 5-byte header, 1MiB blocks, 9 blocks:
        // 9MiB + 5 => 16MiB slot (43%) => 7 blocks (7MiB + 5 => 8MiB slot,
        // 12%) => 3 blocks (3MiB + 5 => 4MiB slot, 25%) - but 3 < 9 / 2 =>
        // not even considered => 7 blocks is the answer (it's also the best
        // one among the visited candidates).
        const TVDiskHugeSlots slots(TVDiskHugeSlotLayout{
            .AppendBlockSize = 1_MB,
            .MinHugeBlobInBytes = 1_MB,
            .MilestoneHugeBlobInBytes = 1_MB,
            .HugeBlobOverhead = 1,
            .MaxHugeBlobInBytes = 64_MB,
        });

        UNIT_ASSERT_VALUES_EQUAL(
            7,
            AdjustMaxBlocksInBlobToVDiskSlots(
                slots,
                MirrorPartSize,
                1_MB,
                9,
                5));
        // 5 blocks: 5MiB + 5 => 8MiB (37%) => 3 blocks (25%) => 1 block
        // (1MiB + 5 => 2MiB, 49%) - 1 < 5 / 2 => not considered => 3
        UNIT_ASSERT_VALUES_EQUAL(
            3,
            AdjustMaxBlocksInBlobToVDiskSlots(
                slots,
                MirrorPartSize,
                1_MB,
                5,
                5));
    }

    Y_UNIT_TEST(ShouldNotAdjustSmallBlobs)
    {
        const TVDiskHugeSlots slots;

        // 64KiB blobs in a block-4-2 group => 16KiB parts => not huge =>
        // nothing to adjust
        UNIT_ASSERT_VALUES_EQUAL(
            16,
            Adjust(slots, Block42PartSize, 4_KB, 64_KB));
        // 256KiB blobs => 64KiB parts + 5 bytes => huge => 18 blocks slot
        // (73152 bytes) => 10% padding => 17 blocks slot (69088 bytes) =>
        // 69083 bytes per part => 276332 bytes blob => hmm, 69088 is not a
        // slot (18 blocks follows 16 directly) => the previous slot is
        // absent => keep the configured value
        UNIT_ASSERT_VALUES_EQUAL(18 * 4064, slots.GetSlotSize(64_KB));
        UNIT_ASSERT_VALUES_EQUAL(0, slots.GetPrevSlotSize(18 * 4064));
        UNIT_ASSERT_VALUES_EQUAL(
            64,
            Adjust(slots, Block42PartSize, 4_KB, 256_KB));

        UNIT_ASSERT_VALUES_EQUAL(
            0,
            AdjustMaxBlocksInBlobToVDiskSlots(
                slots,
                Block42PartSize,
                4_KB,
                0,
                5));
    }
}

}   // namespace NCloud::NStorage
