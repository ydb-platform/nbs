#pragma once

#include <cloud/blockstore/tools/analytics/dump-event-log/profile_log_event_handler.h>

#include <library/cpp/json/writer/json_value.h>

#include <util/generic/bitmap.h>
#include <util/generic/map.h>
#include <util/generic/string.h>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

// Analyzes the checksums that scrubbing writes when checking 4MiB ranges.
// Calculates for each disk which ranges had a checksum like a blocks filled
// with zeros and which had a different checksum. This calculates the percentage
// of disk occupancy with data.
class TZeroRangesStat: public IProfileLogEventHandler
{
    class TZeroRanges
    {
        TDynBitMap KnownRanges;
        TDynBitMap NonZeroRanges;

    public:
        void Set(ui64 rangeIndx, bool isZero);
        [[nodiscard]] NJson::TJsonValue Dump() const;
    };

    class TZeroRangesBySegmentSize
    {
        TMap<ui64, TZeroRanges> BySegmentSize;

    public:
        void Set(ui64 rangeIndx4MiB, bool isZero);
        [[nodiscard]] NJson::TJsonValue Dump() const;
    };

    const TString Filename;
    const ui32 ZeroChecksum;

    TMap<TString, TZeroRangesBySegmentSize> Volumes;

public:
    explicit TZeroRangesStat(const TString& filename);

    void ProcessRequest(
        const TDiskInfo& diskInfo,
        const TTimeData& timeData,
        ui32 requestType,
        TBlockRange64 blockRange,
        const TReplicaChecksums& replicaChecksums,
        const TInflightData& inflightData) override;

    void Finish() override;
};

}   // namespace NCloud::NBlockStore
