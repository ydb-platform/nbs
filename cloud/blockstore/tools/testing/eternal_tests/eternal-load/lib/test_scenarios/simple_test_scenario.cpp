#include "simple_test_scenario.h"

#include "test_scenario_base.h"

#include <cloud/blockstore/tools/testing/eternal_tests/eternal-load/lib/config.h>

#include <util/generic/size_literals.h>
#include <util/random/random.h>
#include <util/system/spinlock.h>

namespace NCloud::NBlockStore::NTesting {

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr TTestScenarioBaseConfig BaseConfig = {
    .DefaultMinReadByteCount = 1_KB,
    .DefaultMaxReadByteCount = 16_KB,
    .DefaultMinWriteByteCount = 1_KB,
    .DefaultMaxWriteByteCount = 16_KB,
    .DefaultMinRegionByteCount = 0,
    .DefaultMaxRegionByteCount = 0,
};

////////////////////////////////////////////////////////////////////////////////

class TSimpleTestScenario: public TTestScenarioBase
{
private:
    class TTestWorker;

    using IService = NTesting::ITestExecutorIOService;

    ESimpleTestScenarioMode Mode;

    // Used for Mode == ESimpleTestScenarioMode::Sequential
    TAdaptiveLock Lock;
    ui64 NextReadOffset = 0;
    ui64 NextWriteOffset = 0;

public:
    TSimpleTestScenario(
        ESimpleTestScenarioMode mode,
        IConfigHolderPtr configHolder,
        const TLog& log);
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
        , WriteBuffer(testScenario->MaxWriteByteCount + (sizeof(ui64) * 2))
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
            Write(offset, byteCount, service);
        } else {
            service.Read(WriteBuffer.data(), byteCount, offset, [&]() {});
        }
    }

    void Write(ui64 offset, ui64 byteCount, ITestExecutorIOService& service)
    {
        // Simple pattern: incrementing ui64 values

        ui64 begin = AlignDown(offset, sizeof(ui64));
        ui64 end = AlignUp(offset + byteCount, sizeof(ui64));

        Y_ABORT_UNLESS(end - begin <= WriteBuffer.size());

        TMemoryOutput stream(WriteBuffer.data(), end - begin);

        for (ui64 pos = begin; pos < end; pos += sizeof(ui64)) {
            const ui64 value = pos / sizeof(ui64);
            stream.Write(&value, sizeof(ui64));
        }

        service.Write(
            WriteBuffer.data() + (offset - begin),
            byteCount,
            offset,
            [&]() {});
    }
};

////////////////////////////////////////////////////////////////////////////////

TSimpleTestScenario::TSimpleTestScenario(
        ESimpleTestScenarioMode mode,
        IConfigHolderPtr configHolder,
        const TLog& log)
    : TTestScenarioBase(BaseConfig, std::move(configHolder), log)
    , Mode(mode)
{
    auto& config = ConfigHolder->GetConfig();
    for (ui32 i = 0; i < config.GetIoDepth(); ++i) {
        AddWorker(std::make_unique<TTestWorker>(this, log));
    }
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
