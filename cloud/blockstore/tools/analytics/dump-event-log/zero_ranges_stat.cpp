#include "zero_ranges_stat.h"

#include <cloud/blockstore/libs/common/block_checksum.h>
#include <cloud/blockstore/libs/service/request.h>

#include <util/stream/file.h>
#include <util/string/builder.h>

namespace NCloud::NBlockStore {

namespace {

///////////////////////////////////////////////////////////////////////////////

constexpr size_t ReservationSize = 8192;
constexpr size_t RangeSize = 4_MB;
constexpr size_t MinSegmentSize = RangeSize;
constexpr size_t MaxSegmentSize = 1_GB;

ui32 CalcZeroChecksum()
{
    const TString zero(RangeSize, '\0');
    TBlockChecksum calc;
    calc.Extend(zero.data(), RangeSize);
    return calc.GetValue();
}

}   // namespace

void TZeroRangesStat::TZeroRanges::Set(ui64 rangeIndx, bool isZero)
{
    if (KnownRanges.Size() <= rangeIndx) {
        KnownRanges.Reserve(KnownRanges.Size() + ReservationSize);
        NonZeroRanges.Reserve(KnownRanges.Size() + ReservationSize);
    }

    KnownRanges.Set(rangeIndx);
    if (!isZero) {
        NonZeroRanges.Set(rangeIndx);
    }
}

TString TZeroRangesStat::TZeroRanges::Print() const
{
    TStringBuilder sb;
    size_t known = KnownRanges.Count();
    size_t nonZero = NonZeroRanges.Count();
    size_t zero = known - nonZero;
    sb << known << "\t" << zero << "\t" << (100.0 * zero / known);

    return sb;
}
void TZeroRangesStat::TZeroRangesBySegmentSize::Set(
    ui64 rangeIndx4Mib,
    bool isZero)
{
    for (size_t segmentSize = MinSegmentSize, rangeIndx = rangeIndx4Mib;
         segmentSize <= MaxSegmentSize;
         segmentSize *= 2, rangeIndx /= 2)
    {
        BySegmentSize[segmentSize].Set(rangeIndx, isZero);
    }
}

TString TZeroRangesStat::TZeroRangesBySegmentSize::Print() const
{
    TStringBuilder sb;
    for (size_t segmentSize = MinSegmentSize; segmentSize <= MaxSegmentSize;
         segmentSize *= 2)
    {
        sb << BySegmentSize.FindPtr(segmentSize)->Print()
           << (segmentSize == MaxSegmentSize ? "" : "\t");
    }
    return sb;
}

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
    const ui32 rangeIndex4MiB = blockRange.Start / blockRange.Size();
    volume.Set(rangeIndex4MiB, isZeroRange);
}

void TZeroRangesStat::Dump()
{
    TFileOutput out(Filename);
    {
        TStringBuilder sb;
        sb << "DiskId\t";
        for (size_t segmentSize = MinSegmentSize; segmentSize <= MaxSegmentSize;
             segmentSize *= 2)
        {
            sb << "Known_" << segmentSize / 1_MB << "\t";
            sb << "Zero_" << segmentSize / 1_MB << "\t";
            sb << "Ratio_" << segmentSize / 1_MB << "%\t";
        }
        sb << "\n";
        out.Write(sb);
    }

    for (const auto& [diskId, volume]: Volumes) {
        TStringBuilder sb;
        sb << diskId << "\t" << volume.Print() << "\n";
        out.Write(sb);
    }
}

}   // namespace NCloud::NBlockStore
