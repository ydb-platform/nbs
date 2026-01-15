#include "test_scenario_base.h"

#include <cloud/blockstore/tools/testing/eternal_tests/eternal-load/lib/config.h>

namespace NCloud::NBlockStore::NTesting {

#define INIT_CONFIG_PARAMS_HELPER(config, x) \
    if ((config).GetMin##x() > 0) { \
        Min##x = (config).GetMin##x(); \
    } \
    if ((config).GetMax##x() > 0) { \
        Max##x = (config).GetMax##x(); \
    } \
    Y_ENSURE(Min##x <= Max##x, \
        "Invalid configuration: Min" #x " (" << Min##x << ") > Max" #x \
        " (" << Max##x << ")"); \
    (config).SetMin##x(Min##x); \
    (config).SetMax##x(Max##x); \

TTestScenarioBase::TTestScenarioBase(
        const TTestScenarioBaseConfig& baseConfig,
        IConfigHolderPtr configHolder,
        const TLog& log)
    : ConfigHolder(std::move(configHolder))
    , Log(log)
    , TestStartTime(Now())
    , MinReadByteCount(baseConfig.DefaultMinReadByteCount)
    , MaxReadByteCount(baseConfig.DefaultMaxReadByteCount)
    , MinWriteByteCount(baseConfig.DefaultMinWriteByteCount)
    , MaxWriteByteCount(baseConfig.DefaultMaxWriteByteCount)
    , MinRegionByteCount(baseConfig.DefaultMinRegionByteCount)
    , MaxRegionByteCount(baseConfig.DefaultMaxRegionByteCount)
{
    auto& config = ConfigHolder->GetConfig();

    if (config.HasAlternatingPhase()) {
        PhaseDuration =
            TDuration::Parse(config.GetAlternatingPhase()).SecondsFloat();
    }

    WriteProbabilityPercent = config.GetWriteRate();

    auto& fileTestConfig = *config.MutableUnalignedTest();
    INIT_CONFIG_PARAMS_HELPER(fileTestConfig, ReadByteCount);
    INIT_CONFIG_PARAMS_HELPER(fileTestConfig, WriteByteCount);
    INIT_CONFIG_PARAMS_HELPER(fileTestConfig, RegionByteCount);
}

ui32 TTestScenarioBase::GetWorkerCount() const
{
    return static_cast<ui32>(Workers.size());
}

ITestScenarioWorker& TTestScenarioBase::GetWorker(ui32 index) const
{
    return *Workers[index];
}

void TTestScenarioBase::AddWorker(std::unique_ptr<ITestScenarioWorker> worker)
{
    Workers.push_back(std::move(worker));
}

ui32 TTestScenarioBase::GetWriteProbabilityPercent(
    double secondsSinceTestStart) const
{
    if (PhaseDuration) {
        auto iter = secondsSinceTestStart / PhaseDuration.value();
        return static_cast<ui64>(iter) % 2 == 1 ? 100 - WriteProbabilityPercent
                                                : WriteProbabilityPercent;
    }
    return WriteProbabilityPercent;
}

#undef INIT_CONFIG_PARAMS_HELPER

}  // namespace NCloud::NBlockStore::NTesting
