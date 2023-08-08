#pragma once

#include <util/datetime/base.h>
#include <util/generic/hash_set.h>
#include <util/generic/maybe.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>


namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

struct TVolumeParamsValue
{
    TString Key;
    TString Value;
    TInstant ValidUntil;
};

class TVolumeParams
{
public:
    TVolumeParams(TVector<TVolumeParamsValue> volumeParams);

    void Merge(THashMap<TString, TVolumeParamsValue> volumeParams);

    // removes and returns expired keys
    TVector<TString> ExtractExpiredKeys(const TInstant& now);
    TMaybe<TDuration> GetNextExpirationDelay(const TInstant& now) const;

    TDuration GetMaxTimedOutDeviceStateDurationOverride(const TInstant& now) const;

private:
    THashMap<TString, TVolumeParamsValue> VolumeParams;
};

}   // namespace NCloud::NBlockStore::NStorage
