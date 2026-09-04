#include "delay_policy.h"

#include <util/system/spinlock.h>

#include <cmath>
#include <random>

namespace NCloud::NFileStore::NStorage::NFastShard {

namespace {

////////////////////////////////////////////////////////////////////////////////

class TLognormalDelayPolicy final: public IDelayPolicy
{
private:
    TAdaptiveLock Lock;
    std::mt19937_64 Engine{42U /* seed */};
    std::lognormal_distribution<double> Distribution;

public:
    TLognormalDelayPolicy(TDuration mean, TDuration stddev)
        : Distribution(MakeDistribution(mean, stddev))
    {}

    [[nodiscard]] TDuration NextDelay() override
    {
        double us = 0;
        with_lock (Lock) {
            us = Distribution(Engine);
        }

        return TDuration::MicroSeconds(static_cast<ui64>(us));
    }

private:
    static std::lognormal_distribution<double> MakeDistribution(
        TDuration mean,
        TDuration stddev)
    {
        //
        // For a lognormal variable exp(N(mu, sigma^2)) with the desired
        // mean m and standard deviation s:
        //   sigma^2 = ln(1 + (s / m)^2)
        //   mu = ln(m) - sigma^2 / 2
        //

        const double m = mean.MicroSeconds();
        const double s = stddev.MicroSeconds();
        Y_ABORT_UNLESS(m > 0, "mean must be positive");

        const double sigma2 = std::log(1 + (s / m) * (s / m));
        const double mu = std::log(m) - sigma2 / 2;
        return std::lognormal_distribution<double>(mu, std::sqrt(sigma2));
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IDelayPolicyPtr CreateLognormalDelayPolicy(TDuration mean, TDuration stddev)
{
    return std::make_shared<TLognormalDelayPolicy>(mean, stddev);
}

}   // namespace NCloud::NFileStore::NStorage::NFastShard
