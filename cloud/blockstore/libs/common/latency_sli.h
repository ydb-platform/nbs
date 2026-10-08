#pragma once

#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/system/types.h>

#include <array>

namespace NCloud::NBlockStore {

enum class ELatencySliResult
{
    Good,
    Bad,
    Unknown
};

struct TLatencySliThreshold
{
    ui64 StartBytes = 0;
    ui64 EndBytes = 0;
    ui64 ThresholdUs = 0;
};

// Immutable after configuration. No allocation or clock reads on completion.
struct TLatencySliConfig
{
    bool Enabled = false;
    std::array<TVector<TLatencySliThreshold>, 2> Thresholds;

    // Invalid or overlapping intervals invalidate that operation's table.
    void Validate();
    TString Serialize() const;
    static TLatencySliConfig Parse(const TString& value);

    ELatencySliResult Classify(
        bool write,
        ui64 bytes,
        ui64 elapsedUs,
        ui64 quotaDelayUs,
        bool failed,
        bool validTiming = true) const
    {
        if (!validTiming || quotaDelayUs > elapsedUs) {
            return ELatencySliResult::Unknown;
        }
        for (const auto& row: Thresholds[write]) {
            if (bytes >= row.StartBytes && bytes < row.EndBytes) {
                return !failed && elapsedUs - quotaDelayUs <= row.ThresholdUs
                           ? ELatencySliResult::Good
                           : ELatencySliResult::Bad;
            }
        }
        return ELatencySliResult::Unknown;
    }
};

}   // namespace NCloud::NBlockStore
