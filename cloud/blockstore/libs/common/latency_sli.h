#pragma once

#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/system/types.h>

#include <array>
#include <memory>
#include <optional>

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

    // Invalid or overlapping intervals clear that operation's table and
    // return false; valid tables are preserved.
    bool Validate();
    TString Serialize() const;
    static std::optional<TLatencySliConfig> TryParse(const TString& value);

    ELatencySliResult Classify(
        bool write,
        ui64 bytes,
        ui64 elapsedUs,
        bool failed) const
    {
        if (failed) {
            return ELatencySliResult::Bad;
        }
        for (const auto& row: Thresholds[write]) {
            if (bytes >= row.StartBytes && bytes < row.EndBytes) {
                return elapsedUs <= row.ThresholdUs
                           ? ELatencySliResult::Good
                           : ELatencySliResult::Bad;
            }
        }
        return ELatencySliResult::Unknown;
    }
};

using TLatencySliConfigPtr = std::shared_ptr<const TLatencySliConfig>;

}   // namespace NCloud::NBlockStore
