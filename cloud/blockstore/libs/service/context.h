#pragma once

#include "public.h"

#include <cloud/storage/core/libs/common/context.h>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

struct TCallContext final: public TCallContextBase
{
private:
    TAtomic SilenceRetriableErrors = false;
    TAtomic HasUncountableRejects = false;

    // Throttler delay attributed to the original performance profile of the
    // disk, as reported by responses. Kept apart from the Postponed stage,
    // which also contains delays caused by the service itself.
    TAtomic QuotaDelayMicroSeconds = 0;
    TAtomic QuotaDelayUnknown = false;

public:
    TCallContext(ui64 requestId = 0);

    bool GetSilenceRetriableErrors() const;
    void SetSilenceRetriableErrors(bool silence);

    bool GetHasUncountableRejects() const;
    void SetHasUncountableRejects();

    TDuration GetQuotaDelay() const;
    void SetQuotaDelay(TDuration d);

    // Set when some throttler delay was reported without its quota part.
    bool GetQuotaDelayUnknown() const;

    void SetQuotaDelayUnknown()
    {
        AtomicSet(QuotaDelayUnknown, true);
    }

    // Accounts the throttler info of one response. quotaDelay is Nothing()
    // when the response did not carry it.
    void AccountThrottlerQuota(
        TMaybe<TDuration> quotaDelay,
        TDuration throttlerDelay);
};

////////////////////////////////////////////////////////////////////////////////

inline TCallContextPtr CreateCallContext(ui64 requestId = 0)
{
    return MakeIntrusive<TCallContext>(requestId);
}

////////////////////////////////////////////////////////////////////////////////

TCallContextPtr ToBlockStoreCallContext(TCallContextBasePtr callContext);

}   // namespace NCloud::NBlockStore
