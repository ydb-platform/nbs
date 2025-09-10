#pragma once

#include <util/datetime/base.h>

namespace NCloud {

///////////////////////////////////////////////////////////////////////////////

class TBackoffDelayProvider
{
private:
    const TDuration InitialDelay;
    const TDuration MaxDelay;
    const TDuration FirstStepDelay;
    TDuration CurrentDelay;

public:
    TBackoffDelayProvider(TDuration initialDelay, TDuration maxDelay);

    TBackoffDelayProvider(
        TDuration initialDelay,
        TDuration maxDelay,
        TDuration firstStepDelay);

    [[nodiscard]] TDuration GetDelay() const;
    [[nodiscard]] TDuration GetDelayAndIncrease();

    void IncreaseDelay();
    void Reset();
};

///////////////////////////////////////////////////////////////////////////////

} // namespace NCloud
