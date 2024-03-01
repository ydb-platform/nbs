#pragma once

#include <util/datetime/base.h>

namespace NCloud {

///////////////////////////////////////////////////////////////////////////////

class TBackoffDelayProvider
{
private:
    const TDuration InitialDelay;
    const TDuration MaxDelay;
    TDuration CurrentDelay;

public:
    TBackoffDelayProvider(TDuration initialDelay, TDuration maxDelay);

    [[nodiscard]] TDuration GetDelay() const;
    [[nodiscard]] TDuration GetDelayAndIncrease();

    void IncreaseDelay();
    void Reset();
};

///////////////////////////////////////////////////////////////////////////////

} // namespace NCloud
