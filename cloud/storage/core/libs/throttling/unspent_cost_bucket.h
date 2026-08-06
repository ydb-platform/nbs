#pragma once

#include <util/datetime/base.h>

namespace NCloud {

////////////////////////////////////////////////////////////////////////////////

// When an operation's actual time spent ("spentCost") is less than its expected
// cost ("cost"), "Register()" computes the missing delay ("cost - spentCost")
// and lets the bucket cover part of it (limited by "BudgetSpendRate"),
// consuming accumulated budget and returning the remaining delay to apply. The
// budget refills linearly with time and is capped at "MaxBudget".
class TUnspentCostBucket
{
private:
    //  The maximum amount of budget that can accumulate.
    const TDuration MaxBudget;
    //  The speed of budget refill.
    const double BudgetRefillRate;
    //  The speed of budget consumption (1.0 - disabled, 0.5 - bucket can cover
    //  up to 50% of the cost, 0.1 - up to 90% of the cost).
    const double BudgetSpendRate;

    //  The current amount of budget.
    TDuration CurrentBudget;
    //  The last time the budget was updated.
    TInstant LastUpdateTs;

public:
    TUnspentCostBucket(
        TDuration maxBudget,
        TDuration budgetRefillTime,
        double budgetSpendRate,
        double spentBudgetShare);

    ~TUnspentCostBucket() = default;

    // https://en.wikipedia.org/wiki/Smoothstep
    // Similar to sigmoid function but faster. Proof:
    // cloud/storage/core/libs/throttling/bench.
    static double Smootherstep(double x);

    TDuration Register(TInstant now, TDuration cost, TDuration spentCost);

    [[nodiscard]] double CalculateCurrentSpentBudgetShare(TInstant now) const;
    [[nodiscard]] TDuration GetCurrentBudget() const;

private:
    [[nodiscard]] TDuration CalculateCurrentBudget(TInstant now) const;
};

}   // namespace NCloud
