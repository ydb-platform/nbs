#pragma once

#include <util/datetime/base.h>

namespace NCloud {

struct ISleeper
{
    virtual ~ISleeper() = default;
    virtual void Sleep(TDuration duration) = 0;
};

using ISleeperPtr = std::shared_ptr<ISleeper>;

ISleeperPtr CreateDefaultSleeper();

}   // namespace NCloud
