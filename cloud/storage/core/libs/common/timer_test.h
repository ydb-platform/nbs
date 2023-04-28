#pragma once

#include "timer.h"

#include <library/cpp/deprecated/atomic/atomic.h>

namespace NCloud {

////////////////////////////////////////////////////////////////////////////////

class TTestTimer final
    : public ITimer
{
private:
    TAtomic Timestamp = 0;

public:
    TInstant Now() override;
    void AdvanceTime(TDuration delay);
};

}   // namespace NCloud
