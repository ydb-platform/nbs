#include "sleeper.h"

#include <util/datetime/base.h>

namespace NCloud {

namespace {

struct TSleeper: public ISleeper
{
    void Sleep(TDuration duration) override
    {
        ::Sleep(duration);
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

ISleeperPtr CreateDefaultSleeper()
{
    return std::make_shared<TSleeper>();
}

}   // namespace NCloud
