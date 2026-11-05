#include "delay_policy.h"

#include <util/system/tls.h>

#include <cmath>
#include <random>
#include <thread>

namespace NCloud::NFastShard {

namespace {

////////////////////////////////////////////////////////////////////////////////

class TLognormalDelayPolicy final: public IDelayPolicy
{
private:
    struct TState
    {
        std::mt19937_64 Engine;
        std::lognormal_distribution<double> Distribution;

        TState(double mu, double sigma)
            : Distribution(mu, sigma)
        {}

        // Copied once per thread, on that thread.
        TState(const TState& other)
            : Engine(std::hash<std::thread::id>{}(std::this_thread::get_id()))
            , Distribution(other.Distribution)
        {}
    };

    // prevent contention on the shared state
    NTls::TValue<TState> State;

public:
    TLognormalDelayPolicy(TDuration mean, TDuration stddev)
        : State(MakeState(mean, stddev))
    {}

    [[nodiscard]] TDuration NextDelay() override
    {
        auto& state = State.Get();
        return TDuration::MicroSeconds(
            static_cast<ui64>(state.Distribution(state.Engine)));
    }

private:
    static TState MakeState(TDuration mean, TDuration stddev)
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
        return TState(mu, std::sqrt(sigma2));
    }
};

class TZeroDelayPolicy final: public IDelayPolicy
{
public:
    [[nodiscard]] TDuration NextDelay() override
    {
        return TDuration::Zero();
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IDelayPolicyPtr CreateLognormalDelayPolicy(TDuration mean, TDuration stddev)
{
    return std::make_shared<TLognormalDelayPolicy>(mean, stddev);
}

IDelayPolicyPtr CreateZeroDelayPolicy()
{
    return std::make_shared<TZeroDelayPolicy>();
}

}   // namespace NCloud::NFastShard
