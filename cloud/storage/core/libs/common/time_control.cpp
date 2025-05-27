#include "time_control.h"

#include <util/datetime/base.h>

namespace NCloud {

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TTimeControl: public ITimeControl
{
    void Sleep(TDuration duration) override
    {
        ::Sleep(duration);
    }

    TInstant Now() override
    {
        return TInstant::Now();
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

ITimeControlPtr CreateDefaultSleeper()
{
    return std::make_shared<TTimeControl>();
}

}   // namespace NCloud
