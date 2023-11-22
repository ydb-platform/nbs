#include "backoff_delay_provider.h"

namespace NCloud {

namespace {

///////////////////////////////////////////////////////////////////////////////

constexpr auto DefaultInitialDelay = TDuration::Seconds(1);

///////////////////////////////////////////////////////////////////////////////

}   // namespace

TBackoffDelayProvider::TBackoffDelayProvider(
        TDuration initialDelay,
        TDuration maxDelay)
    : InitialDelay(initialDelay)
    , MaxDelay(Max(maxDelay, initialDelay))
    , CurrentDelay(initialDelay)
{}

TDuration TBackoffDelayProvider::GetDelay() const
{
    return CurrentDelay;
}

void TBackoffDelayProvider::IncreaseDelay()
{
    CurrentDelay =
        CurrentDelay ? Min(CurrentDelay * 2, MaxDelay) : DefaultInitialDelay;
}

void TBackoffDelayProvider::Reset()
{
    CurrentDelay = InitialDelay;
}

}   // namespace NCloud
