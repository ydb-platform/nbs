#include "vdisk_huge_slots.h"

#include <util/generic/algorithm.h>
#include <util/generic/ylimits.h>
#include <util/system/yassert.h>

namespace NCloud {

namespace {

////////////////////////////////////////////////////////////////////////////////

ui64 SizeToBlocks(ui64 size, ui32 appendBlockSize)
{
    return (size + appendBlockSize - 1) / appendBlockSize;
}

// Reproduces NKikimr::NHuge::NPrivate::TChainLayoutBuilder. The builder
// produces a sequence of segments (Left, Right] in append blocks, a part of
// Right blocks is the largest part that fits into the slot of the segment, so
// the slot sizes are the right boundaries of the segments.
TVector<ui64>
BuildSlotSizesInBlocks(ui32 left, ui32 milestone, ui32 right, ui32 overhead)
{
    TVector<ui64> slots;

    // BuildDownward: (prev, cur] where prev + prev / overhead = cur
    {
        TVector<ui64> downward;
        ui32 cur = milestone;
        while (cur > left) {
            ui32 prev = overhead * cur / (overhead + 1);
            if (prev == cur) {
                prev = cur - 1;
            }

            downward.push_back(cur);
            cur = prev;
        }

        Reverse(downward.begin(), downward.end());
        slots = std::move(downward);
    }

    // BuildUpward: (val, val + val / overhead]
    {
        ui32 val = milestone;
        while (val < right) {
            ui32 shift = val / overhead;
            if (shift == 0) {
                shift = 1;
            }

            slots.push_back(val + shift);
            val += shift;
        }
    }

    return slots;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TVDiskHugeSlots::TVDiskHugeSlots(TVDiskHugeSlotLayout layout)
    : Layout(layout)
{
    Y_ABORT_UNLESS(Layout.AppendBlockSize);
    Y_ABORT_UNLESS(Layout.HugeBlobOverhead);
    Y_ABORT_UNLESS(Layout.MinHugeBlobInBytes);
    Y_ABORT_UNLESS(
        Layout.MinHugeBlobInBytes <= Layout.MilestoneHugeBlobInBytes);
    Y_ABORT_UNLESS(Layout.MilestoneHugeBlobInBytes < Layout.MaxHugeBlobInBytes);

    const auto slotsInBlocks = BuildSlotSizesInBlocks(
        Layout.MinHugeBlobInBytes / Layout.AppendBlockSize,
        Layout.MilestoneHugeBlobInBytes / Layout.AppendBlockSize,
        SizeToBlocks(Layout.MaxHugeBlobInBytes, Layout.AppendBlockSize),
        Layout.HugeBlobOverhead);

    SlotSizes.reserve(slotsInBlocks.size());
    for (const auto blocks: slotsInBlocks) {
        SlotSizes.push_back(blocks * Layout.AppendBlockSize);
    }

    Y_ABORT_UNLESS(!SlotSizes.empty());
    Y_ABORT_UNLESS(IsSorted(SlotSizes.begin(), SlotSizes.end()));
}

ui64 TVDiskHugeSlots::GetSlotSize(ui64 partSize) const
{
    // See THugeBlobCtx::IsHugeBlob and TAllChains::GetChain.
    const ui64 size = partSize + Layout.PartHeaderSize;
    if (size < Layout.MinHugeBlobInBytes) {
        return 0;
    }

    // The first slot that can hold the part. The search table built by
    // TAllChains::BuildSearchTable maps a size to the first chain whose
    // SlotSize >= size.
    const auto it = LowerBound(SlotSizes.begin(), SlotSizes.end(), size);
    if (it == SlotSizes.end()) {
        return 0;
    }

    return *it;
}

ui64 TVDiskHugeSlots::GetPrevSlotSize(ui64 slotSize) const
{
    const auto it = LowerBound(SlotSizes.begin(), SlotSizes.end(), slotSize);
    if (it == SlotSizes.begin()) {
        return 0;
    }

    return *(it - 1);
}

ui32 TVDiskHugeSlots::CalculateSlotPaddingPercentage(ui64 partSize) const
{
    const ui64 slotSize = GetSlotSize(partSize);
    if (!slotSize) {
        return 0;
    }

    const ui64 size = partSize + Layout.PartHeaderSize;
    Y_ABORT_UNLESS(size <= slotSize);
    return 100 * (slotSize - size) / slotSize;
}

////////////////////////////////////////////////////////////////////////////////

ui32 AdjustMaxBlocksInBlobToVDiskSlots(
    const TVDiskHugeSlots& slots,
    const TPartSizeCalculator& calculatePartSize,
    ui32 blockSize,
    ui32 maxBlocksInBlob,
    ui32 maxSlotPaddingPercentage)
{
    Y_ABORT_UNLESS(blockSize);

    if (!maxBlocksInBlob) {
        return 0;
    }

    const ui32 headerSize = slots.GetLayout().PartHeaderSize;

    // Never shrink the blobs more than twice - the slot padding can't be
    // worse than 1/HugeBlobOverhead (12.5%) anyway and smaller blobs cost
    // CPU and index space.
    const ui32 minBlocksInBlob = Max<ui32>(1, maxBlocksInBlob / 2);

    ui32 bestBlocks = maxBlocksInBlob;
    ui32 bestPadding = Max<ui32>();

    ui32 blocks = maxBlocksInBlob;
    while (true) {
        const ui64 partSize = calculatePartSize(blocks * ui64(blockSize));
        const ui64 slotSize = slots.GetSlotSize(partSize);
        if (!slotSize) {
            // Such parts are not stored in huge slots => nothing to adjust.
            break;
        }

        const ui32 padding = slots.CalculateSlotPaddingPercentage(partSize);
        if (padding < bestPadding) {
            bestPadding = padding;
            bestBlocks = blocks;
        }

        if (padding <= maxSlotPaddingPercentage) {
            break;
        }

        const ui64 prevSlotSize = slots.GetPrevSlotSize(slotSize);
        if (prevSlotSize <= headerSize) {
            break;
        }

        // The largest number of blocks whose parts fit into the previous
        // slot. calculatePartSize is monotonic => binary search.
        const ui64 maxPartSize = prevSlotSize - headerSize;
        ui32 lo = 0;
        ui32 hi = blocks - 1;
        while (lo < hi) {
            const ui32 mid = lo + (hi - lo + 1) / 2;
            if (calculatePartSize(mid * ui64(blockSize)) <= maxPartSize) {
                lo = mid;
            } else {
                hi = mid - 1;
            }
        }

        if (lo < minBlocksInBlob) {
            break;
        }

        blocks = lo;
    }

    return bestBlocks;
}

}   // namespace NCloud
