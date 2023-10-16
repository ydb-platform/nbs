#include "backoff_delay_provider.h"

namespace NCloud {

///////////////////////////////////////////////////////////////////////////////

TBackoffDelayProvider::TBackoffDelayProvider(
    TDuration initialDelay,
    TDuration maxDelay)
    : InitialDelay(initialDelay)
    , MaxDelay(maxDelay)
    , CurrentDelay(initialDelay)
{
    Y_DEBUG_ABORT_UNLESS(InitialDelay > TDuration());
    Y_DEBUG_ABORT_UNLESS(InitialDelay <= MaxDelay);
}

TDuration TBackoffDelayProvider::GetDelay() const
{
    return CurrentDelay;
}

void TBackoffDelayProvider::IncreaseDelay()
{
    CurrentDelay = Min(CurrentDelay * 2, MaxDelay);
}

void TBackoffDelayProvider::Reset()
{
    CurrentDelay = InitialDelay;
}

} // namespace NCloud
