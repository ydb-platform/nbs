#include "unspent_cost_bucket.h"

#include <util/generic/utility.h>

#include <cmath>

namespace NCloud {
namespace {

////////////////////////////////////////////////////////////////////////////////

TDuration MultiplyWithRounding(TDuration duration, double multiplier)
{
    return TDuration::MicroSeconds(
        std::round(duration.MicroSeconds() * multiplier));
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TUnspentCostBucket::TUnspentCostBucket(
    TDuration maxBudget,
    TDuration budgetRefillTime,
    double budgetSpendRate,
    double spentBudgetShare)
    : MaxBudget(Max(maxBudget, TDuration::MilliSeconds(1)))
    , BudgetRefillRate(
          budgetRefillTime.GetValue() > 0
              ? MaxBudget.MicroSeconds() /
                    static_cast<double>(budgetRefillTime.MicroSeconds())
              : 0.)
    , BudgetSpendRate(1.0 / Max(1.0, budgetSpendRate))
    , CurrentBudget(MaxBudget * (1.0 - spentBudgetShare))
{}

TDuration
TUnspentCostBucket::Register(TInstant now, TDuration cost, TDuration spent)
{
    CurrentBudget = CalculateCurrentBudget(now);
    LastUpdateTs = now;

    if (spent >= cost) {
        return TDuration::Zero();
    }

    const TDuration maxBudgetSpend = cost - spent;
    const TDuration minBudgetSpend =
        MultiplyWithRounding(cost, BudgetSpendRate) - spent;
    const TDuration neededBudgetSpend = maxBudgetSpend - minBudgetSpend;
    const TDuration coveredByBudget = Min(neededBudgetSpend, CurrentBudget);

    CurrentBudget -= coveredByBudget;

    return maxBudgetSpend - coveredByBudget;
}

double TUnspentCostBucket::CalculateCurrentSpentBudgetShare(TInstant now) const
{
    return 1.0 - CalculateCurrentBudget(now) / MaxBudget;
}

TDuration TUnspentCostBucket::GetCurrentBudget() const
{
    return CurrentBudget;
}

TDuration TUnspentCostBucket::CalculateCurrentBudget(TInstant now) const
{
    TDuration timePassed;
    if (Y_LIKELY(LastUpdateTs.GetValue())) {
        timePassed = now - LastUpdateTs;
    }
    return Min(
        MaxBudget,
        CurrentBudget + MultiplyWithRounding(timePassed, BudgetRefillRate));
}

}   // namespace NCloud
