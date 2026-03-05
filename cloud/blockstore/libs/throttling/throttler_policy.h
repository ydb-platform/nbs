#pragma once

#include "public.h"

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

struct TUsedQuota
{
    THashMap<NProto::EStorageMediaKind, ui32> PerMediaKind;
    ui32 Total = 0;

    TUsedQuota() = default;

    TUsedQuota(
        const THashMap<NProto::EStorageMediaKind, TDuration>& perMediaKind,
        double burstRate)
    {
        double total = 0;
        for (auto [mediaKind, usedQuota]: perMediaKind) {
            double usedQuotaInSeconds = usedQuota.MicroSeconds() / 1e6;
            PerMediaKind[mediaKind] =
                std::round(Min(usedQuotaInSeconds / burstRate, 1.0) * 100.0);
            total += usedQuotaInSeconds;
        }

        Total = std::round(Min(total / burstRate, 1.0) * 100.0);
    }
};

////////////////////////////////////////////////////////////////////////////////

struct IThrottlerPolicy
{
    virtual ~IThrottlerPolicy() = default;

    virtual TDuration SuggestDelay(
        TInstant now,
        NCloud::NProto::EStorageMediaKind mediaKind,
        EBlockStoreRequest requestType,
        size_t byteCount) = 0;

    virtual double CalculateCurrentSpentBudgetShare(TInstant ts) const = 0;

    virtual TUsedQuota TakeUsedQuotaShare() = 0;
};

////////////////////////////////////////////////////////////////////////////////

IThrottlerPolicyPtr CreateThrottlerPolicyStub();

}   // namespace NCloud::NBlockStore
