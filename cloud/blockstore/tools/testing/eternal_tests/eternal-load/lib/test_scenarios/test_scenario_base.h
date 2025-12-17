#pragma once

#include <cloud/blockstore/tools/testing/eternal_tests/eternal-load/lib/test_executor.h>

#include <library/cpp/logger/log.h>

#include <util/datetime/base.h>

#include <optional>

namespace NCloud::NBlockStore::NTesting {

////////////////////////////////////////////////////////////////////////////////

struct TTestScenarioBaseConfig
{
    ui64 DefaultMinReadByteCount = 0;
    ui64 DefaultMaxReadByteCount = 0;
    ui64 DefaultMinWriteByteCount = 0;
    ui64 DefaultMaxWriteByteCount = 0;
    ui64 DefaultMinRegionByteCount = 0;
    ui64 DefaultMaxRegionByteCount = 0;
};

////////////////////////////////////////////////////////////////////////////////

class TTestScenarioBase: public ITestScenario
{
protected:
    IConfigHolderPtr ConfigHolder;
    const TLog Log;

    ui64 FileSize = 0;
    const TInstant TestStartTime;

    ui64 MinReadByteCount = 0;
    ui64 MaxReadByteCount = 0;
    ui64 MinWriteByteCount = 0;
    ui64 MaxWriteByteCount = 0;
    ui64 MinRegionByteCount = 0;
    ui64 MaxRegionByteCount = 0;

private:
    TVector<std::unique_ptr<ITestScenarioWorker>> Workers;

    // If set, the workers alternate between two phases:
    // - 1: WriteProbabilityPercentage = |WriteProbabilityPercentage|
    // - 2: WriteProbabilityPercentage = 100 - |WriteProbabilityPercentage|
    // The duration of each phase is |PhaseDuration| seconds
    std::optional<double> PhaseDuration;
    ui32 WriteProbabilityPercent = 0;

protected:
    TTestScenarioBase(
        const TTestScenarioBaseConfig& config,
        IConfigHolderPtr configHolder,
        const TLog& log);

public:
    ui32 GetWorkerCount() const override;
    ITestScenarioWorker& GetWorker(ui32 index) const override;

protected:
    void AddWorker(std::unique_ptr<ITestScenarioWorker> worker);
    ui32 GetWriteProbabilityPercent(double secondsSinceTestStart) const;
};

}   // namespace NCloud::NBlockStore::NTesting
