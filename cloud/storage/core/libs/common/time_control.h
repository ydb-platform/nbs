#pragma once

#include "timer.h"

#include <util/datetime/base.h>

namespace NCloud {

struct ITimeControl: public ITimer
{
    virtual void Sleep(TDuration duration) = 0;
};

using ITimeControlPtr = std::shared_ptr<ITimeControl>;

ITimeControlPtr CreateDefaultSleeper();

}   // namespace NCloud
