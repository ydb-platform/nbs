#pragma once

#include <util/generic/size_literals.h>
#include <util/generic/vector.h>
#include <util/system/types.h>

#include <functional>

namespace NCloud {

////////////////////////////////////////////////////////////////////////////////
// A model of the VDisk huge blob heap.
//
// Every part of a "huge" blob (a part whose size is >= MinHugeBlobInBytes) is
// stored by the VDisk in a slot of a fixed size. Slot sizes form a geometric
// ladder (each slot size is ~1/HugeBlobOverhead larger than the previous one),
// so a part that is just slightly larger than some slot size ends up occupying
// the next slot size and wastes up to 1/HugeBlobOverhead (12.5% by default) of
// the slot. E.g. a 1MiB part (a part of a 4MiB blob in a block-4-2 group) ends
// up in a ~1150KiB slot which wastes ~11% of space.
//
// The ladder is built by NKikimr::NHuge::NPrivate::TChainLayoutBuilder (see
// contrib/ydb/core/blobstorage/vdisk/huge/blobstorage_hullhugeheap.cpp) and
// this class reproduces it in order to choose blob sizes whose parts fit
// their slots as tightly as possible.
////////////////////////////////////////////////////////////////////////////////

struct TVDiskHugeSlotLayout
{
    // PDisk sector payload: 4096 (sector size) - 24 (TDataSectorFooter)
    // - 8 (canary). See contrib/ydb/core/blobstorage/pdisk/
    // blobstorage_pdisk_data.h. Slot sizes are multiples of this value.
    ui32 AppendBlockSize = 4064;

    // TVDiskConfig::MinHugeBlobInBytes. 64KiB for SSD/NVMe, 512KiB for ROT.
    // Parts smaller than this value are not stored in huge slots at all (they
    // go to the fresh segment / SSTs together with the index). The part of
    // the ladder above MilestoneHugeBlobInBytes doesn't depend on this value.
    ui32 MinHugeBlobInBytes = 64_KB;

    // TVDiskConfig::MilestoneHugeBlobInBytes - "for compatibility reasons it
    // must be 512KB". The ladder is built downwards and upwards from this
    // value.
    ui32 MilestoneHugeBlobInBytes = 512_KB;

    // TVDiskConfig::HugeBlobOverhead. Each slot size is larger than the
    // previous one by 1/HugeBlobOverhead.
    ui32 HugeBlobOverhead = 8;

    // The largest part that can be stored in a huge slot. Only limits the
    // length of the ladder, the real value (MaxLogoBlobDataSize-based) is
    // irrelevant for the blob sizes used by the tablets.
    ui32 MaxHugeBlobInBytes = 16_MB;

    // TDiskBlob::HeaderSize = sizeof(ui32) + sizeof(ui8). Written in front of
    // every part when TVDiskConfig::AddHeader is set (which is the default)
    // and accounted for when the slot is chosen for the part.
    ui32 PartHeaderSize = 5;
};

////////////////////////////////////////////////////////////////////////////////

class TVDiskHugeSlots
{
private:
    const TVDiskHugeSlotLayout Layout;
    // Ascending.
    TVector<ui64> SlotSizes;

public:
    explicit TVDiskHugeSlots(TVDiskHugeSlotLayout layout = {});

    const TVDiskHugeSlotLayout& GetLayout() const
    {
        return Layout;
    }

    const TVector<ui64>& GetSlotSizes() const
    {
        return SlotSizes;
    }

    // Returns the size of the slot which is used by the VDisk to store a part
    // of the specified size (part header excluded). Returns 0 if such parts
    // are not stored in huge slots.
    ui64 GetSlotSize(ui64 partSize) const;

    // Returns the largest slot size which is strictly less than slotSize.
    // Returns 0 if there is no such slot.
    ui64 GetPrevSlotSize(ui64 slotSize) const;

    // The amount of space wasted in the slot by a part of the specified size.
    // Returns 0 if such parts are not stored in huge slots.
    ui32 CalculateSlotPaddingPercentage(ui64 partSize) const;
};

////////////////////////////////////////////////////////////////////////////////

// Maps blob size to the size of its largest part (erasure-dependent, see
// TBlobStorageGroupType::MaxPartSize). Must be monotonically non-decreasing.
using TPartSizeCalculator = std::function<ui64(ui64 blobSize)>;

// Adjusts the maximum number of blocks per blob so that the parts of the blobs
// written by the tablet don't waste more than maxSlotPaddingPercentage of their
// VDisk huge slots (if it's possible without shrinking the blobs more than
// twice).
//
// Starts with maxBlocksInBlob and, while the padding is too large, descends
// to the largest number of blocks whose parts fit into the previous (smaller)
// slot. Returns the first number of blocks which satisfies the padding limit
// or the number of blocks with the smallest padding among the visited ones.
ui32 AdjustMaxBlocksInBlobToVDiskSlots(
    const TVDiskHugeSlots& slots,
    const TPartSizeCalculator& calculatePartSize,
    ui32 blockSize,
    ui32 maxBlocksInBlob,
    ui32 maxSlotPaddingPercentage);

}   // namespace NCloud
