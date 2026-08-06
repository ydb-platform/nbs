#include "unspent_cost_bucket.h"

#include <util/generic/utility.h>

#include <algorithm>
#include <cmath>

namespace NCloud {
namespace {

////////////////////////////////////////////////////////////////////////////////

TDuration MultiplyWithRounding(TDuration duration, double multiplier)
{
    return TDuration::MicroSeconds(
        std::round(duration.MicroSeconds() * multiplier));
}

TDuration CalculateDesiredBudgetSpend(
    TDuration minSpend,
    TDuration maxSpend,
    double discountFactor)
{
    return minSpend +
           MultiplyWithRounding((maxSpend - minSpend), discountFactor);
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

double TUnspentCostBucket::Smootherstep(double x)
{
    const double x3 = x * x * x;
    return ((6 * x - 15) * x + 10) * x3;
}

TDuration
TUnspentCostBucket::Register(TInstant now, TDuration cost, TDuration spent)
{
    CurrentBudget = CalculateCurrentBudget(now);
    LastUpdateTs = now;

    if (spent >= cost) {
        return TDuration::Zero();
    }

    const double budgetRatio = CurrentBudget / MaxBudget;
    const double discountFactor =
        // clamp just in case of precision issues
        Smootherstep(std::clamp(budgetRatio, 0.0, 1.0));

    const TDuration maxBudgetSpend = cost - spent;
    const TDuration minBudgetSpend =
        MultiplyWithRounding(cost, BudgetSpendRate) - spent;
    // The speed of budget consumption is higher when the budget is more full.
    const TDuration desiredBudgetSpend = CalculateDesiredBudgetSpend(
        minBudgetSpend,
        maxBudgetSpend,
        discountFactor);
    const TDuration coveredByBudget = Min(desiredBudgetSpend, CurrentBudget);

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
