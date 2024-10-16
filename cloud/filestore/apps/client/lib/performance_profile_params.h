#pragma once

#include <util/system/types.h>

namespace NLastGetopt {
    class TOpts;
}   // namespace NLastGetopt

namespace NCloud::NFileStore::NClient {

////////////////////////////////////////////////////////////////////////////////

class TPerformanceProfileParams
{
private:
    bool ThrottlingEnabled = false;
    ui64 MaxReadBandwidth = 0;
    ui32 MaxReadIops = 0;
    ui64 MaxWriteBandwidth = 0;
    ui32 MaxWriteIops = 0;
    ui32 BoostTime = 0;
    ui32 BoostRefillTime = 0;
    ui32 BoostPercentage = 0;
    ui32 BurstPercentage = 0;
    ui32 DefaultPostponedRequestWeight = 0;
    ui64 MaxPostponedWeight = 0;
    ui32 MaxWriteCostMultiplier = 0;
    ui32 MaxPostponedTime = 0;
    ui32 MaxPostponedCount = 0;

public:
    TPerformanceProfileParams(NLastGetopt::TOpts& opts);

    template <typename TRequest>
    void FillRequest(TRequest& request) const
    {
        auto pp = request.MutablePerformanceProfile();
        pp->SetThrottlingEnabled(ThrottlingEnabled);
        pp->SetMaxReadBandwidth(MaxReadBandwidth);
        pp->SetMaxReadIops(MaxReadIops);
        pp->SetMaxWriteBandwidth(MaxWriteBandwidth);
        pp->SetMaxWriteIops(MaxWriteIops);
        pp->SetBoostTime(BoostTime);
        pp->SetBoostRefillTime(BoostRefillTime);
        pp->SetBoostPercentage(BoostPercentage);
        pp->SetBurstPercentage(BurstPercentage);
        pp->SetDefaultPostponedRequestWeight(DefaultPostponedRequestWeight);
        pp->SetMaxPostponedWeight(MaxPostponedWeight);
        pp->SetMaxWriteCostMultiplier(MaxWriteCostMultiplier);
        pp->SetMaxPostponedTime(MaxPostponedTime);
        pp->SetMaxPostponedCount(MaxPostponedCount);
    }
};

}   // namespace NCloud::NFileStore::NClient
