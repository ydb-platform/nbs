#include "zero_ranges_stat.h"

#include <cloud/blockstore/libs/common/block_checksum.h>
#include <cloud/blockstore/libs/service/request.h>

#include <util/stream/file.h>
#include <util/string/builder.h>

namespace NCloud::NBlockStore {

namespace {

///////////////////////////////////////////////////////////////////////////////

constexpr size_t NonreplicatedDeviceSize = 24379392 * 4_KB;   // 93 GiB
constexpr size_t RangeSize = 4_MB;
constexpr size_t RangesPerDevice = NonreplicatedDeviceSize / RangeSize;

ui32 CalcZeroChecksum()
{
    const TString zero(RangeSize, '\0');
    TBlockChecksum calc;
    calc.Extend(zero.data(), RangeSize);
    return calc.GetValue();
}

}   // namespace

TZeroRangesStat::TZeroRangesStat(const TString& filename)
    : Filename(filename)
    , ZeroChecksum(CalcZeroChecksum())
{}

TZeroRangesStat::~TZeroRangesStat()
{
    Dump();
}

void TZeroRangesStat::ProcessRequest(
    const TString& diskId,
    TInstant timestamp,
    ui32 requestType,
    TBlockRange64 blockRange,
    TDuration duration,
    const TReplicaChecksums& replicaChecksums)
{
    Y_UNUSED(timestamp);
    Y_UNUSED(duration);

    if (requestType != static_cast<ui32>(ESysRequestType::Scrubbing)) {
        return;
    }

    bool isZeroRange = true;
    for (const auto& replicaChecksum: replicaChecksums) {
        for (ui32 checksum: replicaChecksum.GetChecksums()) {
            isZeroRange = isZeroRange && checksum == ZeroChecksum;
        }
    }

    auto& volume = Volumes[diskId];
    const ui32 rangeIndex = blockRange.Start / blockRange.Size();
    if (volume.KnownRanges.Size() <= rangeIndex) {
        const ui32 deviceIndex = rangeIndex / RangesPerDevice;
        const ui32 rangeCount = (deviceIndex + 1) * RangesPerDevice;
        volume.KnownRanges.Reserve(rangeCount);
        volume.ZeroRanges.Reserve(rangeCount);
    }
    volume.KnownRanges.Set(rangeIndex);
    if (isZeroRange) {
        volume.ZeroRanges.Set(rangeIndex);
    } else {
        volume.ZeroRanges.Reset(rangeIndex);
    }
}

void TZeroRangesStat::Dump()
{
    TFileOutput out(Filename);
    out.Write("DiskId\tZeroRanges\tKnownRanges\tRatio %\n");

    for (const auto& [diskId, volume]: Volumes) {
        const ui32 knownRanges = volume.KnownRanges.Count();
        const ui32 zeroRanges = volume.ZeroRanges.Count();
        TStringBuilder sb;
        sb << diskId << "\t" << zeroRanges << "\t" << knownRanges << "\t"
           << 100.0 * zeroRanges / knownRanges << "\n";
        out.Write(sb);
    }
}

}   // namespace NCloud::NBlockStore
