#include "volume_params.h"

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

TRuntimeVolumeParams::TRuntimeVolumeParams(
    TVector<TRuntimeVolumeParamsValue> volumeParams)
{
    for (auto& value: volumeParams) {
        auto key = value.Key;
        VolumeParams.try_emplace(std::move(key), value);
    }
}

void TRuntimeVolumeParams::Merge(
    THashMap<TString, TRuntimeVolumeParamsValue> volumeParams)
{
    for (auto& [key, param]: volumeParams) {
        VolumeParams.insert_or_assign(std::move(key), std::move(param));
    }
}

TVector<TString> TRuntimeVolumeParams::ExtractExpiredKeys(const TInstant& now)
{
    TVector<TString> keys;
    for (const auto& [key, param]: VolumeParams) {
        if (param.ValidUntil <= now) {
            keys.emplace_back(key);
        }
    }
    for (const auto& key: keys) {
        VolumeParams.erase(key);
    }

    return keys;
}

TMaybe<TDuration> TRuntimeVolumeParams::GetNextExpirationDelay(
    const TInstant& now) const
{
    if (VolumeParams.empty()) {
        return Nothing();
    }

    const auto minTime =
        std::min_element(
            VolumeParams.begin(),
            VolumeParams.end(),
            [&](const auto& a, const auto& b)
            { return a.second.ValidUntil < b.second.ValidUntil; })
            ->second.ValidUntil;

    const auto defaultDelay = TDuration::MilliSeconds(1);
    return minTime <= now ? defaultDelay : minTime - now;
}

TDuration TRuntimeVolumeParams::GetMaxTimedOutDeviceStateDurationOverride(
    const TInstant& now) const
{
    const auto* maxTimeoutParam =
        VolumeParams.FindPtr("max-timed-out-device-state-duration");
    if (!maxTimeoutParam || maxTimeoutParam->ValidUntil <= now) {
        return {};
    }

    TDuration duration;
    if (!TDuration::TryParse(maxTimeoutParam->Value, duration)) {
        return {};
    }
    return duration;
}

}   // namespace NCloud::NBlockStore::NStorage
