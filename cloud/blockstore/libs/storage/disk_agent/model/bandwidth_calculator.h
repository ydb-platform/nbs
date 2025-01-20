#pragma once

#include <cloud/blockstore/libs/storage/disk_agent/model/public.h>

#include <util/datetime/base.h>
#include <util/generic/hash.h>
#include <util/generic/list.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

class TBandwidthCalculator
{
    const ui64 MaxTotalBandwidth;
    const ui64 MaxDeviceBandwidth;

    THashMap<TString, TInstant> DeviceLastRequest;

public:
    explicit TBandwidthCalculator(const TDiskAgentConfig& config);
    ~TBandwidthCalculator();

    // Returns the recommended bandwidth for the next request.
    ui64 RegisterRequest(const TString& deviceUUID, TInstant now);

private:
    void ClearHistory(TInstant deadline);
    [[nodiscard]] ui64 GetRecommendedBandwidth() const;
};

}   // namespace NCloud::NBlockStore::NStorage
