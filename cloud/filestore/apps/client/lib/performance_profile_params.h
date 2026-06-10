#pragma once

#include <util/system/types.h>

#include <util/generic/maybe.h>

namespace NLastGetopt {
    class TOpts;
}   // namespace NLastGetopt

namespace NCloud::NFileStore::NClient {

////////////////////////////////////////////////////////////////////////////////

class TPerformanceProfileParams
{
private:
    TMaybe<bool> ThrottlingEnabled;
    ui64 MaxReadBandwidth = 0;
    ui32 MaxReadIops = 0;
    ui64 MaxWriteBandwidth = 0;
    ui32 MaxWriteIops = 0;
    TMaybe<ui32> BoostTime;
    TMaybe<ui32> BoostRefillTime;
    ui32 BoostPercentage = 0;
    TMaybe<ui32> BurstPercentage;
    TMaybe<ui64> DefaultPostponedRequestWeight;
    TMaybe<ui64> MaxPostponedWeight;
    TMaybe<ui32> MaxWriteCostMultiplier;
    TMaybe<ui32> MaxPostponedTime;
    TMaybe<ui32> MaxPostponedCount;

public:
    TPerformanceProfileParams(NLastGetopt::TOpts& opts);

    template <typename TRequest>
    void FillRequest(TRequest& request) const
    {
        auto pp = request.MutablePerformanceProfile();
        if (ThrottlingEnabled.Defined()) {
            pp->SetThrottlingEnabled(*ThrottlingEnabled);
        }
        pp->SetMaxReadBandwidth(MaxReadBandwidth);
        pp->SetMaxReadIops(MaxReadIops);
        pp->SetMaxWriteBandwidth(MaxWriteBandwidth);
        pp->SetMaxWriteIops(MaxWriteIops);
        if (BoostTime.Defined()) {
            pp->SetBoostTime(*BoostTime);
        }
        if (BoostRefillTime.Defined()) {
            pp->SetBoostRefillTime(*BoostRefillTime);
        }
        pp->SetBoostPercentage(BoostPercentage);
        if (BurstPercentage.Defined()) {
            pp->SetBurstPercentage(*BurstPercentage);
        }
        if (DefaultPostponedRequestWeight.Defined()) {
            pp->SetDefaultPostponedRequestWeight(
                *DefaultPostponedRequestWeight);
        }
        if (MaxPostponedWeight.Defined()) {
            pp->SetMaxPostponedWeight(*MaxPostponedWeight);
        }
        if (MaxWriteCostMultiplier.Defined()) {
            pp->SetMaxWriteCostMultiplier(*MaxWriteCostMultiplier);
        }
        if (MaxPostponedTime.Defined()) {
            pp->SetMaxPostponedTime(*MaxPostponedTime);
        }
        if (MaxPostponedCount.Defined()) {
            pp->SetMaxPostponedCount(*MaxPostponedCount);
        }
    }
};

}   // namespace NCloud::NFileStore::NClient
