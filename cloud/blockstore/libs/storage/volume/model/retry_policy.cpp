#include "retry_policy.h"

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

TRetryPolicy::TRetryPolicy(TDuration increment, TDuration max)
    : TimeoutIncrement(increment)
    , TimeoutMax(max)
{}

TDuration TRetryPolicy::GetCurrentTimeout() const
{
    return CurrentTimeout;
}

TInstant TRetryPolicy::GetCurrentDeadline() const
{
    return CurrentDeadline;
}

void TRetryPolicy::Reset(TInstant now)
{
    CurrentTimeout = TDuration::Zero();
    CurrentDeadline = now;
}

void TRetryPolicy::Update(TInstant now)
{
    CurrentTimeout = Min(CurrentTimeout + TimeoutIncrement, TimeoutMax);
    CurrentDeadline = Max(CurrentDeadline, now) + CurrentTimeout;
}

}   // namespace NCloud::NBlockStore::NStorage
