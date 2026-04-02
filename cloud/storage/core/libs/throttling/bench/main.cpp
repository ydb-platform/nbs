#include <library/cpp/testing/benchmark/bench.h>

#include <util/generic/vector.h>
#include <util/random/random.h>
#include <util/system/compiler.h>
#include <util/system/yassert.h>

#include <cmath>

namespace {

////////////////////////////////////////////////////////////////////////////////

double Sigmoid(double x)
{
    constexpr double Steepness = -8.0;
    constexpr double Offset = 0.5;
    return 1.0 / (1.0 + std::exp(Steepness * (x - Offset)));
}

double Smoothstep(double x)
{
    const double x2 = x * x;
    return 3 * x2 - 2 * x2 * x;
}

double Smootherstep1(double x)
{
    const double x3 = x * x * x;
    return ((6 * x - 15) * x + 10) * x3;
}

double Smootherstep2(double x)
{
    const double x2 = x * x;
    const double x4 = x2 * x2;
    return (((-20 * x + 70) * x - 84) * x + 35) * x4;
}

double Smootherstep3(double x)
{
    const double x2 = x * x;
    const double x5 = x2 * x2 * x;
    return ((((70 * x - 315) * x + 540) * x - 420) * x + 126) * x5;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

thread_local TVector<double> RandomNumbers;

#define SMOOTHER_BENCH(SmoothFunc)                                        \
    Y_CPU_BENCHMARK(SmoothFunc##_Benchmark, iface)                        \
    {                                                                     \
        constexpr int RandomNumbersSize = 1024 * 1024;                    \
        if (RandomNumbers.empty()) {                                      \
            RandomNumbers.reserve(RandomNumbersSize);                     \
            for (int i = 0; i < RandomNumbersSize; i++) {                 \
                RandomNumbers.push_back(RandomNumber<double>());          \
            }                                                             \
        }                                                                 \
                                                                          \
        for (i64 i = 0; i < static_cast<i64>(iface.Iterations()); ++i) {  \
            const double x = RandomNumbers[i % RandomNumbersSize];        \
            double result = SmoothFunc(x);                                \
            NBench::DoNotOptimize(result);                                \
            Y_DEBUG_ABORT_UNLESS(                                         \
                result >= 0.0,                                            \
                "SmoothFunc(%f) = %f",                                    \
                x,                                                        \
                result);                                                  \
            constexpr double Eps = 1e-9;                                  \
            Y_DEBUG_ABORT_UNLESS(                                         \
                result <= 1.0 + Eps,                                      \
                "SmoothFunc(%f) = %f",                                    \
                x,                                                        \
                result);                                                  \
        }                                                                 \
    }                                                                     \
    // SMOOTHER_BENCH

////////////////////////////////////////////////////////////////////////////////

SMOOTHER_BENCH(Sigmoid)
SMOOTHER_BENCH(Smoothstep)
SMOOTHER_BENCH(Smootherstep1)
SMOOTHER_BENCH(Smootherstep2)
SMOOTHER_BENCH(Smootherstep3)
