#include "context.h"

#include <util/datetime/cputimer.h>
#include <util/generic/utility.h>
#include <util/system/yassert.h>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

TCallContext::TCallContext(ui64 requestId)
    : TCallContextBase(requestId)
{}

bool TCallContext::GetSilenceRetriableErrors() const
{
    return AtomicGet(SilenceRetriableErrors);
}

void TCallContext::SetSilenceRetriableErrors(bool silence)
{
    AtomicSet(SilenceRetriableErrors, silence);
}

bool TCallContext::GetHasUncountableRejects() const
{
    return AtomicGet(HasUncountableRejects);
}

void TCallContext::SetHasUncountableRejects()
{
    AtomicSet(HasUncountableRejects, true);
}

TDuration TCallContext::GetQuotaDelay() const
{
    return TDuration::MicroSeconds(AtomicGet(QuotaDelayMicroSeconds));
}

void TCallContext::SetQuotaDelay(TDuration d)
{
    AtomicSet(QuotaDelayMicroSeconds, d.MicroSeconds());
}

bool TCallContext::GetQuotaDelayUnknown() const
{
    return AtomicGet(QuotaDelayUnknown);
}

void TCallContext::AccountThrottlerQuota(
    TMaybe<TDuration> quotaDelay,
    TDuration throttlerDelay)
{
    if (quotaDelay && *quotaDelay && throttlerDelay) {
        AtomicAdd(
            QuotaDelayMicroSeconds,
            Min(*quotaDelay, throttlerDelay).MicroSeconds());
    } else if (!quotaDelay && throttlerDelay) {
        AtomicSet(QuotaDelayUnknown, true);
    }
}

TCallContextPtr ToBlockStoreCallContext(TCallContextBasePtr callContext)
{
    if (!callContext) {
        return {};
    }

    auto* concrete = dynamic_cast<TCallContext*>(callContext.Get());
    Y_ABORT_UNLESS(concrete);
    return TCallContextPtr(concrete);
}

}   // namespace NCloud::NBlockStore
