#pragma once

#include <cloud/blockstore/tools/analytics/dump-event-log/profile_log_event_handler.h>

#include <util/generic/bitmap.h>
#include <util/generic/map.h>
#include <util/generic/string.h>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

// Analyzes the checksums that scrubbing writes when checking 4MiB ranges.
// Calculates for each disk which ranges had a checksum like a blocks filled with
// zeros and which had a different checksum. This calculates the percentage of
// disk occupancy with data.
class TZeroRangesStat: public IProfileLogEventHandler
{
    struct TVolumeRanges
    {
        TDynBitMap KnownRanges;
        TDynBitMap ZeroRanges;
    };

    const TString Filename;
    const ui32 ZeroChecksum;

    TMap<TString, TVolumeRanges> Volumes;

public:
    explicit TZeroRangesStat(const TString& filename);
    ~TZeroRangesStat() override;

    void ProcessRequest(
        const TString& diskId,
        TInstant timestamp,
        ui32 requestType,
        TBlockRange64 blockRange,
        TDuration duration,
        const TReplicaChecksums& replicaChecksums) override;

private:
    void Dump();
};

}   // namespace NCloud::NBlockStore
