#pragma once

#include "sleeper.h"
#include "timer_test.h"
#include "util/generic/vector.h"

#include <utility>

namespace NCloud {

struct TTestSleeper: public ISleeper
{
private:
    std::shared_ptr<TTestTimer> Timer;

public:
    TVector<TDuration> SleepDurations;

    explicit TTestSleeper(std::shared_ptr<TTestTimer> timer)
        : Timer(std::move(timer))
    {}

    void Sleep(TDuration duration) override;
};

}   // namespace NCloud
