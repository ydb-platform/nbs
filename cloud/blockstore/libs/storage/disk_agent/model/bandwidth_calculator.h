#pragma once

#include <util/datetime/base.h>
#include <util/generic/hash.h>
#include <util/generic/list.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

class TBandwidthCalculator
{
    const ui64 MaxTotalBandwidth;
    THashMap<TString, TInstant> DeviceLastRequest;

public:
    explicit TBandwidthCalculator(ui64 maxTotalBandwidth);
    ~TBandwidthCalculator();

    // Returns the recommended bandwidth for the next request.
    ui64 RegisterRequest(const TString& deviceUUID, TInstant now);

private:
    void ClearHistory(TInstant deadline);
    [[nodiscard]] ui64 GetRecommendedBandwidth() const;
};

}   // namespace NCloud::NBlockStore::NStorage
