#pragma once

#include <util/datetime/base.h>
#include <util/generic/hash_set.h>
#include <util/generic/maybe.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

struct TRuntimeVolumeParamsValue
{
    TString Key;
    TString Value;
    TInstant ValidUntil;
};

class TRuntimeVolumeParams
{
public:
    TRuntimeVolumeParams(TVector<TRuntimeVolumeParamsValue> volumeParams);

    void Merge(THashMap<TString, TRuntimeVolumeParamsValue> volumeParams);

    // removes and returns expired keys
    TVector<TString> ExtractExpiredKeys(const TInstant& now);
    TMaybe<TDuration> GetNextExpirationDelay(const TInstant& now) const;

    TDuration GetMaxTimedOutDeviceStateDurationOverride(
        const TInstant& now) const;

private:
    THashMap<TString, TRuntimeVolumeParamsValue> VolumeParams;
};

}   // namespace NCloud::NBlockStore::NStorage
