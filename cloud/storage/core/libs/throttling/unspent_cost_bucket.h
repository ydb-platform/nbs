#pragma once

#include <util/datetime/base.h>

namespace NCloud {

////////////////////////////////////////////////////////////////////////////////

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
        double spentShapingBudgetShare);

    ~TUnspentCostBucket() = default;

    TDuration Register(TInstant now, TDuration cost, TDuration spentCost);

    [[nodiscard]] double CalculateCurrentSpentBudgetShare(TInstant now) const;
    [[nodiscard]] TDuration GetCurrentBudget() const;

private:
    [[nodiscard]] TDuration CalculateCurrentBudget(TInstant now) const;
};

}   // namespace NCloud
