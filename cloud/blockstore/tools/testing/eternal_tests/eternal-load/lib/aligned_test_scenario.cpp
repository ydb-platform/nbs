#include "aligned_test_scenario.h"

#include "config.h"

#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <library/cpp/digest/crc32c/crc32c.h>

#include <util/random/random.h>
#include <util/string/builder.h>
#include <util/system/info.h>

namespace NCloud::NBlockStore::NTesting {

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr ui64 DIRECT_IO_ALIGNMENT = 512;   // bytes

////////////////////////////////////////////////////////////////////////////////

std::pair<i64, i64> ExtendedEuclideanAlgorithm(ui64 a, ui64 b)
{
    if (a == 0) {
        return {0, 1};
    }
    auto [x1, y1] = ExtendedEuclideanAlgorithm(b % a, a);
    return {y1 - (b / a) * x1, x1};
}

////////////////////////////////////////////////////////////////////////////////

ui64 CalculateInverse(ui64 step, ui64 len)
{
    auto [x, _] = ExtendedEuclideanAlgorithm(step, len);
    x = (x + len) % len;
    return x;
}

////////////////////////////////////////////////////////////////////////////////

struct TRange
{
    TRangeConfig& Config;
    ui64 Size;
    std::shared_ptr<char[]> Buf;
    ui64 StepInversion;

    TRange(TRangeConfig& config, ui64 size)
        : Config{config}
        , Size{size}
        , Buf{static_cast<char*>(
                  std::aligned_alloc(NSystemInfo::GetPageSize(), Size)),
              std::free}
        , StepInversion{
              CalculateInverse(Config.GetStep(), Config.GetRequestCount())}
    {
        memset(Buf.get(), '1', Size);

        Y_ABORT_UNLESS(
            size % Config.GetWriteParts() == 0,
            "invalid write parts number");
        Y_ABORT_UNLESS(
            size / Config.GetWriteParts() >= sizeof(TBlockData),
            "blockdata doesn't fit write part");
        Y_ABORT_UNLESS(
            (size / Config.GetWriteParts()) % DIRECT_IO_ALIGNMENT == 0,
            "write parts has invalid alignment");
    }

    char* Data(ui64 offset = 0)
    {
        return Buf.get() + offset;
    }

    ui64 DataSize()
    {
        return Size;
    }

    std::pair<ui64, ui64> NextWrite()
    {
        ui64 blockIdx =
            Config.GetStartOffset() +
            Config.GetLastBlockIdx() * Config.GetRequestBlockCount();
        ui64 iteration = Config.GetNumberToWrite();

        Config.SetLastBlockIdx(
            (Config.GetLastBlockIdx() + Config.GetStep()) %
            Config.GetRequestCount());

        Config.SetNumberToWrite(Config.GetNumberToWrite() + 1);
        return {blockIdx, iteration};
    }

    std::pair<ui64, TMaybe<ui64>> RandomRead()
    {
        // Idea of this code is to find request number (x) which is written in
        // random block (r). To do this we need to solve equation
        // `startBlockIdx + x * step = r [mod %requestCount]`
        // which is equal to equation
        // `x = (r - startBlockIdx) * inverted_step [mod %requestCount]`.
        ui64 requestCount = Config.GetRequestCount();

        ui64 randomBlock = RandomNumber(requestCount);
        ui64 tmp = (randomBlock - Config.GetStartBlockIdx() + requestCount) %
                   requestCount;
        ui64 x = (tmp * StepInversion) % requestCount;

        TMaybe<ui64> expected = Nothing();
        if (Config.GetNumberToWrite() > x) {
            ui64 fullCycles =
                (Config.GetNumberToWrite() - x - 1) / requestCount;
            expected = x + fullCycles * requestCount;
        }

        ui64 requestBlockIdx = Config.GetStartOffset() +
                               randomBlock * Config.GetRequestBlockCount();
        return {requestBlockIdx, expected};
    }
};

////////////////////////////////////////////////////////////////////////////////

class TAlignedTestScenario: public ITestScenario
{
private:
    using IService = ITestExecutorIOService;

    const TInstant TestStartTimestamp;
    IConfigHolderPtr ConfigHolder;
    TDuration SlowRequestThreshold;
    std::optional<double> PhaseDuration;
    ui32 WriteRate;

    TVector<TRange> Ranges;
    TVector<std::unique_ptr<ITestScenarioWorker>> Workers;

    TLog Log;

    TAtomic WriteRequestsCompleted = 0;

private:
    void
    DoRequest(ui16 rangeIdx, double secondsSinceTestStart, IService& service);

    void DoWriteRequest(ui16 rangeIdx, IService& service);
    void DoReadRequest(ui16 rangeIdx, IService& service);

    void OnResponse(
        TInstant startTs,
        ui16 rangeIdx,
        TStringBuf reqType,
        IService& service);

    struct TWorker: public ITestScenarioWorker
    {
        TAlignedTestScenario* Scenario;
        ui32 Index;

        TWorker(TAlignedTestScenario* scenario, ui32 index)
            : Scenario(scenario)
            , Index(index)
        {}

        void Run(double secondsSinceTestStart, IService& service) final
        {
            Scenario->DoRequest(Index, secondsSinceTestStart, service);
        }
    };

public:
    TAlignedTestScenario(IConfigHolderPtr configHolder, const TLog& log)
        : TestStartTimestamp(Now())
        , ConfigHolder(configHolder)
        , Log(log)
    {
        auto& config = ConfigHolder->GetConfig();
        for (ui16 i = 0; i < config.GetIoDepth(); ++i) {
            auto& rangeConfig = *config.MutableRanges(i);
            Ranges.emplace_back(
                rangeConfig,
                rangeConfig.GetRequestBlockCount() * config.GetBlockSize());
            Workers.push_back(std::make_unique<TWorker>(this, i));
        }

        SlowRequestThreshold =
            TDuration::Parse(config.GetSlowRequestThreshold());

        if (config.HasAlternatingPhase()) {
            PhaseDuration =
                TDuration::Parse(config.GetAlternatingPhase()).SecondsFloat();

            Y_ENSURE(
                PhaseDuration > 0,
                "Alternating phase duration should be a positive non-zero value");
        }

        WriteRate = config.GetWriteRate();
    }

    ui32 GetWorkerCount() const final
    {
        return static_cast<ui32>(Workers.size());
    }

    ITestScenarioWorker& GetWorker(ui32 index) const final
    {
        return *Workers[index];
    }
};

////////////////////////////////////////////////////////////////////////////////

void TAlignedTestScenario::DoRequest(
    ui16 rangeIdx,
    double secondsSinceTestStart,
    IService& service)
{
    auto writeRate = WriteRate;
    if (PhaseDuration) {
        auto iter = secondsSinceTestStart / PhaseDuration.value();
        if (static_cast<ui64>(iter) % 2 == 1) {
            writeRate = 100 - writeRate;
        }
    }

    if (RandomNumber(100u) >= writeRate) {
        DoReadRequest(rangeIdx, service);
    } else {
        DoWriteRequest(rangeIdx, service);
    }
}

void TAlignedTestScenario::OnResponse(
    TInstant startTs,
    ui16 rangeIdx,
    TStringBuf reqType,
    IService& service)
{
    if (reqType == "write") {
        const i64 maxRequestCount =
            ConfigHolder->GetConfig().GetMaxWriteRequestCount();
        if (maxRequestCount &&
            AtomicIncrement(WriteRequestsCompleted) >= maxRequestCount)
        {
            service.Stop();
        }
    }

    const auto now = Now();
    const auto d = now - startTs;
    if (d > SlowRequestThreshold) {
        STORAGE_WARN(
            "Slow " << reqType << " request: "
                    << "range=" << rangeIdx << ", duration=" << d);
    }
}

void TAlignedTestScenario::DoReadRequest(ui16 rangeIdx, IService& service)
{
    auto& range = Ranges[rangeIdx];
    // https://stackoverflow.com/questions/46114214/lambda-implicit-capture-fails-with-variable-declared-from-structured-binding
    ui64 blockIdx;
    TMaybe<ui64> expected;
    std::tie(blockIdx, expected) = range.RandomRead();

    ui64 blockSize = ConfigHolder->GetConfig().GetBlockSize();

    const auto startTs = Now();

    auto readHandler =
        [this, startTs, blockIdx, rangeIdx, expected, &service]() mutable
    {
        OnResponse(startTs, rangeIdx, "read", service);

        if (!expected) {
            return;
        }

        auto& range = Ranges[rangeIdx];

        ui64 partSize = range.DataSize() / range.Config.GetWriteParts();
        for (ui64 part = 0; part < range.Config.GetWriteParts(); ++part) {
            TBlockData blockData;
            memcpy(&blockData, range.Data(part * partSize), sizeof(blockData));

            if (blockData.RequestNumber != *expected ||
                blockData.PartNumber != part)
            {
                service.Fail(
                    TStringBuilder()
                    << "[" << rangeIdx << "] Wrong data in block " << blockIdx
                    << " expected RequestNumber " << expected
                    << " actual TBlockData " << blockData);
                return;
            }
        }
    };

    service.Read(
        range.Data(),
        range.DataSize(),
        blockIdx * blockSize,
        readHandler);
}

void TAlignedTestScenario::DoWriteRequest(ui16 rangeIdx, IService& service)
{
    auto& range = Ranges[rangeIdx];

    const auto startTs = Now();
    auto [blockIdx, iteration] = range.NextWrite();
    TBlockData blockData{
        .RequestNumber = iteration,
        .BlockIndex = blockIdx,
        .RangeIdx = rangeIdx,
        .RequestTimestamp = startTs.MicroSeconds(),
        .TestTimestamp = TestStartTimestamp.MicroSeconds(),
        .TestId = ConfigHolder->GetConfig().GetTestId(),
        .Checksum = 0};

    ui64 blockSize = ConfigHolder->GetConfig().GetBlockSize();
    ui64 partSize = range.DataSize() / range.Config.GetWriteParts();
    for (ui32 part = 0; part < range.Config.GetWriteParts(); ++part) {
        blockData.PartNumber = part;
        blockData.Checksum = 0;
        blockData.Checksum = Crc32c(&blockData, sizeof(blockData));
        ui64 partOffset = part * partSize;
        memcpy(range.Data(partOffset), &blockData, sizeof(blockData));
        service.Write(
            range.Data(partOffset),
            partSize,
            blockIdx * blockSize + partOffset,
            [this, startTs, rangeIdx, &service]() mutable
            { OnResponse(startTs, rangeIdx, "write", service); });
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

ITestScenarioPtr CreateAlignedTestScenario(
    IConfigHolderPtr configHolder,
    const TLog& log)
{
    return ITestScenarioPtr(
        new TAlignedTestScenario(std::move(configHolder), log));
}

}   // namespace NCloud::NBlockStore::NTesting
