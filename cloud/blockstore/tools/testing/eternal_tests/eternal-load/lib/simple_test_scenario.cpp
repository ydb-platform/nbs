#include "simple_test_scenario.h"

#include <cloud/blockstore/tools/testing/eternal_tests/eternal-load/lib/config.h>

#include <util/generic/size_literals.h>
#include <util/random/random.h>
#include <util/system/spinlock.h>

namespace NCloud::NBlockStore::NTesting {

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr ui64 DefaultMinReadByteCount = 1_KB;
constexpr ui64 DefaultMaxReadByteCount = 16_KB;

constexpr ui64 DefaultMinWriteByteCount = 1_KB;
constexpr ui64 DefaultMaxWriteByteCount = 16_KB;

////////////////////////////////////////////////////////////////////////////////

class TSimpleTestScenario: public ITestScenario
{
private:
    class TTestWorker;

    using IService = NTesting::ITestExecutorIOService;

    ESimpleTestScenarioMode Mode;
    IConfigHolderPtr ConfigHolder;
    TLog Log;
    TVector<std::unique_ptr<ITestScenarioWorker>> Workers;

    // Used for Mode == ESimpleTestScenarioMode::Sequential
    TAdaptiveLock Lock;
    ui64 NextReadOffset = 0;
    ui64 NextWriteOffset = 0;

    // If set, the workers alternate between two phases:
    // - 1: WriteProbabilityPercentage = |WriteProbabilityPercentage|
    // - 2: WriteProbabilityPercentage = 100 - |WriteProbabilityPercentage|
    // The duration of each phase is |PhaseDuration| seconds
    std::optional<double> PhaseDuration;
    ui32 WriteProbabilityPercent = 0;

    ui64 MinReadByteCount = DefaultMinReadByteCount;
    ui64 MaxReadByteCount = DefaultMaxReadByteCount;
    ui64 MinWriteByteCount = DefaultMinWriteByteCount;
    ui64 MaxWriteByteCount = DefaultMaxWriteByteCount;

public:
    TSimpleTestScenario(
        ESimpleTestScenarioMode mode,
        IConfigHolderPtr configHolder,
        const TLog& log);

    ui32 GetWorkerCount() const override
    {
        return static_cast<ui32>(Workers.size());
    }

    ITestScenarioWorker& GetWorker(ui32 index) const override
    {
        return *Workers[index];
    }

private:
    ui32 GetWriteProbabilityPercent(double secondsSinceTestStart) const;
};

////////////////////////////////////////////////////////////////////////////////

class TSimpleTestScenario::TTestWorker: public ITestScenarioWorker
{
private:
    TSimpleTestScenario* TestScenario = nullptr;
    TVector<char> ReadBuffer;
    TVector<char> WriteBuffer;
    TLog Log;

public:
    TTestWorker(TSimpleTestScenario* testScenario, const TLog& log)
        : TestScenario(testScenario)
        , ReadBuffer(testScenario->MaxReadByteCount)
        , WriteBuffer(testScenario->MaxWriteByteCount)
        , Log(log)
    {}

    void Run(
        double secondsSinceTestStart,
        ITestExecutorIOService& service) override
    {
        const auto writeRate =
            TestScenario->GetWriteProbabilityPercent(secondsSinceTestStart);

        if (RandomNumber(100u) >= writeRate) {
            Run(TestScenario->NextReadOffset,
                TestScenario->MinReadByteCount,
                TestScenario->MaxReadByteCount,
                false,
                service);
        } else {
            Run(TestScenario->NextWriteOffset,
                TestScenario->MinWriteByteCount,
                TestScenario->MaxWriteByteCount,
                true,
                service);
        }
    }

private:
    void Run(
        ui64& nextOffset,
        ui64 minByteCount,
        ui64 maxByteCount,
        bool write,
        ITestExecutorIOService& service)
    {
        const ui64 fileSize =
            TestScenario->ConfigHolder->GetConfig().GetFileSize();

        const ui64 byteCount =
            RandomNumber(maxByteCount - minByteCount + 1) + minByteCount;

        ui64 offset = 0;

        switch (TestScenario->Mode) {
            case ESimpleTestScenarioMode::Sequential: {
                with_lock (TestScenario->Lock) {
                    if (nextOffset + byteCount > fileSize) {
                        nextOffset = 0;
                    }
                    offset = nextOffset;
                    nextOffset += byteCount;
                }
                break;
            }
            case ESimpleTestScenarioMode::Random: {
                offset = RandomNumber(fileSize - byteCount + 1);
                break;
            }
        }

        if (write) {
            service.Write(WriteBuffer.data(), byteCount, offset, [&]() {});
        } else {
            service.Read(WriteBuffer.data(), byteCount, offset, [&]() {});
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

TSimpleTestScenario::TSimpleTestScenario(
        ESimpleTestScenarioMode mode,
        IConfigHolderPtr configHolder,
        const TLog& log)
    : Mode(mode)
    , ConfigHolder(std::move(configHolder))
    , Log(log)
{
    auto& config = ConfigHolder->GetConfig();
    for (ui32 i = 0; i < config.GetIoDepth(); ++i) {
        Workers.push_back(std::make_unique<TTestWorker>(this, log));
    }

    if (config.HasAlternatingPhase()) {
        PhaseDuration =
            TDuration::Parse(config.GetAlternatingPhase()).SecondsFloat();
    }

    WriteProbabilityPercent = config.GetWriteRate();

    auto& fileTestConfig = *config.MutableUnalignedTest();

    if (fileTestConfig.GetMinReadByteCount() > 0) {
        MinReadByteCount = fileTestConfig.GetMinReadByteCount();
    }
    if (fileTestConfig.GetMaxReadByteCount() > 0) {
        MaxReadByteCount = fileTestConfig.GetMaxReadByteCount();
    }
    Y_ENSURE(
        MinReadByteCount <= MaxReadByteCount,
        "Invalid configuration: MinReadByteCount ("
            << MinReadByteCount << ") > MaxReadByteCount (" << MaxReadByteCount
            << ")");

    if (fileTestConfig.GetMinWriteByteCount() > 0) {
        MinWriteByteCount = fileTestConfig.GetMinWriteByteCount();
    }
    if (fileTestConfig.GetMaxWriteByteCount() > 0) {
        MaxWriteByteCount = fileTestConfig.GetMaxWriteByteCount();
    }
    Y_ENSURE(
        MinWriteByteCount <= MaxWriteByteCount,
        "Invalid configuration: MinWriteByteCount ("
            << MinWriteByteCount << ") > MaxWriteByteCount ("
            << MaxWriteByteCount << ")");
}

ui32 TSimpleTestScenario::GetWriteProbabilityPercent(
    double secondsSinceTestStart) const
{
    if (PhaseDuration) {
        auto iter = secondsSinceTestStart / PhaseDuration.value();
        return static_cast<ui64>(iter) % 2 == 1 ? 100 - WriteProbabilityPercent
                                                : WriteProbabilityPercent;
    }
    return WriteProbabilityPercent;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

ITestScenarioPtr CreateSimpleTestScenario(
    ESimpleTestScenarioMode mode,
    IConfigHolderPtr configHolder,
    const TLog& log)
{
    return ITestScenarioPtr(
        new TSimpleTestScenario(mode, std::move(configHolder), log));
}

}   // namespace NCloud::NBlockStore::NTesting
