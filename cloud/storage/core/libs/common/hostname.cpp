#include "hostname.h"

#include <library/cpp/retry/retry.h>

#include <util/datetime/base.h>
#include <util/system/hostname.h>

namespace NCloud {

namespace {

////////////////////////////////////////////////////////////////////////////////

const ui32 GetFqdnHostNameRetryCount = 10;
// Duration between two consecutive retries
const TDuration GetFqdnHostNameSleepDuration = TDuration::Seconds(2);

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TString GetFqdnHostNameWithRetries(
    const std::function<void(const yexception&)>& onFail)
{
    const auto hostname = DoWithRetry<TString, yexception>(
        [] () { return FQDNHostName(); },
        onFail,
        TRetryOptions(
            GetFqdnHostNameRetryCount,
            GetFqdnHostNameSleepDuration));

    Y_ENSURE(hostname);
    return *hostname;
}

}   // namespace NCloud
