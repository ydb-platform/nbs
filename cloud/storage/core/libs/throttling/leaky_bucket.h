#pragma once

#include "helpers.h"

#include <util/datetime/base.h>

#include <cmath>

namespace NCloud {

////////////////////////////////////////////////////////////////////////////////

class TLeakyBucket
{
private:
    const double Rate;        // budget refill rate per mcs
    const double BurstRate;   // max immediate rate (max budget)

    struct TState
    {
        TInstant LastUpdateTs;
        double TimePassed = 0;
        double Budget = 0;   // accumulated budget (immediate rate)
    };

    TState State;

public:
    TLeakyBucket(double rate, double burstRate, double initialBudget)
        : Rate(rate / 1e6)
        , BurstRate(burstRate)
    {
        State.Budget = initialBudget;
    }

public:
    double Register(TInstant ts, double update)
    {
        if (Y_LIKELY(State.LastUpdateTs.GetValue())) {
            State.TimePassed = (ts - State.LastUpdateTs).MicroSeconds();
            State.Budget += Rate * State.TimePassed;
        }

        State.LastUpdateTs = ts;

        if (State.Budget + 1e-10 >= update) {
            State.Budget =
                Min(Max(0., BurstRate - update), State.Budget - update);

            return 0;
        }

        return update - State.Budget;
    }

    void Flush()
    {
        State.Budget = 0;
    }

    void Put(double budget)
    {
        State.Budget += budget;
    }

    double Budget() const
    {
        return State.Budget;
    }

    double TimePassed() const
    {
        return State.TimePassed;
    }

    double CalculateCurrentSpentBudgetShare(TInstant ts) const
    {
        double currentBudget = State.Budget;
        if (Y_LIKELY(State.LastUpdateTs.GetValue())) {
            currentBudget = Min(
                State.Budget + Rate * (ts - State.LastUpdateTs).MicroSeconds(),
                BurstRate);
        }
        return (BurstRate - currentBudget) / BurstRate;
    }
};

////////////////////////////////////////////////////////////////////////////////

class TBoostedTimeBucket
{
private:
    double Burst;
    TLeakyBucket Standard;
    double Beta;    // boost spend rate
    double Alpha;   // Boost bucket refill rate
    TLeakyBucket Boost;

public:
    TBoostedTimeBucket(
        TDuration burstTime,
        double boostRate,
        TDuration boostTime,
        TDuration boostRefillTime,
        TDuration initialBoostTime)
        : Burst(burstTime.MicroSeconds() / 1e6)
        , Standard(1, Burst, Burst)
        , Beta(boostRate - 1)
        , Alpha(
              boostRefillTime.GetValue()
                  ? static_cast<double>(boostTime.MicroSeconds()) /
                        boostRefillTime.MicroSeconds()
                  : 0)
        , Boost(
              Alpha,
              boostTime.MicroSeconds() / 1e6,
              initialBoostTime.MicroSeconds() / 1e6)
    {}

    TBoostedTimeBucket(TDuration burstTime)
        : TBoostedTimeBucket(
              burstTime,
              0,
              TDuration::Zero(),
              TDuration::Zero(),
              TDuration::Zero())
    {}

public:
    TDuration Register(TInstant ts, TDuration update)
    {
        auto diff1 = Standard.Register(ts, update.MicroSeconds() / 1e6);
        if (diff1 == 0) {
            return TDuration::Zero();
        }

        if (Beta <= 0) {
            return SecondsToDuration(diff1);
        }

        // we should not spend all boost budget at once
        auto maxBoostableDiff1 =
            Min(diff1, Min(Burst, Standard.TimePassed() / 1e6) * Beta);

        auto diff2 = Boost.Register(ts, maxBoostableDiff1);
        // the remaining part of the update
        auto fullDiff = diff2 + diff1 - maxBoostableDiff1;
        if (fullDiff == 0) {
            Standard.Flush();
            return TDuration::Zero();
        }

        if (diff2 != 0) {
            // boost budget exhausted, will need to accumulate fullDiff at
            // standard rate, standard rate = 1 + Alpha
            // Min needed for the case when boost budget accumulation rate is
            // for some reason > than boost spending rate (this check is safer
            // than VERIFY-like stuff)
            return SecondsToDuration(fullDiff / (1 + Min(Alpha, Beta)));
        }

        const auto b = Boost.Budget();
        Standard.Put(maxBoostableDiff1);
        // b - current accumulated time
        // 1 + Alpha - accumulation rate
        // 1 + Beta - max rate
        // b + (1 + Alpha) * b / (1 + Beta) + (1 + Alpha) * ((1 + Alpha) * b /
        // (1 + Beta)) / (1 + Beta) + ... b / (1 - (1 + Alpha) / (1 + Beta)) max
        // accumulated time at max rate
        auto threshold =
            Alpha < Beta ? b / (1 - (1 + Alpha) / (1 + Beta)) : Max<double>();
        if (fullDiff < threshold) {
            // we have enough boost budget to accumulate fullDiff at max rate
            return SecondsToDuration(fullDiff / (1 + Beta));
        }

        // will have to accumulate fullDiff - threshold at standard rate
        return SecondsToDuration(
            threshold / (1 + Beta) + (fullDiff - threshold) / (1 + Alpha));
    }

    double CalculateCurrentSpentBudgetShare(TInstant ts) const
    {
        return Standard.CalculateCurrentSpentBudgetShare(ts);
    }

    TDuration GetCurrentBoostBudget() const
    {
        return TDuration::MilliSeconds(Boost.Budget() * 1'000.);
    }
};

}   // namespace NCloud
