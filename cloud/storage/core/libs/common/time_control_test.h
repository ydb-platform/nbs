#pragma once

#include "time_control.h"
#include "timer_test.h"

#include <util/generic/vector.h>

namespace NCloud {

struct TTestTimeControl: public ITimeControl
{
private:
    TTestTimer Timer;

public:
    TVector<TDuration> SleepDurations;

    void Sleep(TDuration duration) override;
    TInstant Now() override;
};

}   // namespace NCloud
