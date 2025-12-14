#include "zero_ranges_stat.h"

#include <cloud/blockstore/libs/common/block_checksum.h>
#include <cloud/blockstore/libs/service/request.h>

#include <library/cpp/json/json_writer.h>
#include <library/cpp/json/writer/json_value.h>

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

NJson::TJsonValue TZeroRangesStat::TZeroRanges::Dump() const
{
    const size_t known = KnownRanges.Count();
    const size_t nonZero = NonZeroRanges.Count();
    const size_t zero = known - nonZero;

    NJson::TJsonValue result;
    result["Known"] = known;
    result["Zero"] = nonZero;
    result["Fraction"] = 100.0 * zero / known;
    return result;
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

NJson::TJsonValue TZeroRangesStat::TZeroRangesBySegmentSize::Dump() const
{
    NJson::TJsonValue result;
    for (size_t segmentSize = MinSegmentSize; segmentSize <= MaxSegmentSize;
         segmentSize *= 2)
    {
        if (const auto* a = BySegmentSize.FindPtr(segmentSize)) {
            auto stat = a->Dump();
            stat["SegmentSize"] = segmentSize;
            result.AppendValue(std::move(stat));
        }
    }
    return result;
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
    const TDiskInfo& diskInfo,
    const TTimeData& timeData,
    ui32 requestType,
    TBlockRange64 blockRange,
    const TReplicaChecksums& replicaChecksums,
    const TInflightData& inflightData)
{
    Y_UNUSED(timeData);
    Y_UNUSED(inflightData);

    if (requestType != static_cast<ui32>(ESysRequestType::Scrubbing)) {
        return;
    }

    bool isZeroRange = true;
    for (const auto& replicaChecksum: replicaChecksums) {
        for (ui32 checksum: replicaChecksum.GetChecksums()) {
            isZeroRange = isZeroRange && checksum == ZeroChecksum;
        }
    }

    auto& volume = Volumes[diskInfo.DiskId];
    const ui32 rangeIndex4MiB = blockRange.Start / blockRange.Size();
    volume.Set(rangeIndex4MiB, isZeroRange);
}

void TZeroRangesStat::Dump()
{
    TFileOutput out(Filename);

    NJson::TJsonValue all;
    for (const auto& [diskId, volume]: Volumes) {
        all[diskId] = volume.Dump();
    }

    NJson::WriteJson(
        &out,
        &all,
        NJson::TJsonWriterConfig{
            .FormatOutput = true,
            .SortKeys = true,
            .WriteNanAsString = true,
        });
}

}   // namespace NCloud::NBlockStore
