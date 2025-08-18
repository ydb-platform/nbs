#include "test_executor.h"

#include <library/cpp/aio/aio.h>
#include <library/cpp/digest/crc32c/crc32c.h>

#include <util/datetime/base.h>
#include <util/generic/yexception.h>
#include <util/random/random.h>
#include <util/string/builder.h>
#include <util/system/file.h>
#include <util/system/info.h>
#include <util/thread/lfstack.h>

namespace NCloud::NBlockStore {

namespace {

using namespace NThreading;

////////////////////////////////////////////////////////////////////////////////

constexpr ui64 DIRECT_IO_ALIGNMENT = 512; // bytes

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

struct TRange {
    TRangeConfig& Config;
    ui64 Size;
    std::shared_ptr<char[]> Buf;
    ui64 StepInversion;

    TRange(TRangeConfig& config, ui64 size)
        : Config{config}
        , Size{size}
        , Buf{static_cast<char*>(std::aligned_alloc(NSystemInfo::GetPageSize(), Size)), std::free}
        , StepInversion{CalculateInverse(Config.GetStep(), Config.GetRequestCount())}
    {
        memset(Buf.get(), '1', Size);

        Y_ABORT_UNLESS(
            size % Config.GetWriteParts() == 0,
            "invalid write parts number"
        );
        Y_ABORT_UNLESS(
            size / Config.GetWriteParts() >= sizeof(TBlockData),
            "blockdata doesn't fit write part"
        );
        Y_ABORT_UNLESS(
            (size / Config.GetWriteParts()) % DIRECT_IO_ALIGNMENT == 0,
            "write parts has invalid alignment"
        );
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
        ui64 blockIdx = Config.GetStartOffset() + Config.GetLastBlockIdx() * Config.GetRequestBlockCount();
        ui64 iteration = Config.GetNumberToWrite();

        Config.SetLastBlockIdx((Config.GetLastBlockIdx() + Config.GetStep()) % Config.GetRequestCount());
        Config.SetNumberToWrite(Config.GetNumberToWrite() + 1);
        return {blockIdx, iteration};
    }

    std::pair<ui64, TMaybe<ui64>> RandomRead()
    {
        // Idea of this code is to find request number (x) which is written in random block (r).
        // To do this we need to solve equation `startBlockIdx + x * step = r [mod %requestCount]` which is equal
        // to equation `x = (r - startBlockIdx) * inverted_step [mod %requestCount]`.
        ui64 requestCount = Config.GetRequestCount();

        ui64 randomBlock = RandomNumber(requestCount);
        ui64 tmp = (randomBlock - Config.GetStartBlockIdx() + requestCount) % requestCount;
        ui64 x = (tmp * StepInversion) % requestCount;

        TMaybe<ui64> expected = Nothing();
        if (Config.GetNumberToWrite() > x) {
            ui64 fullCycles = (Config.GetNumberToWrite() - x - 1) / requestCount;
            expected = x + fullCycles * requestCount;
        }

        ui64 requestBlockIdx = Config.GetStartOffset() + randomBlock * Config.GetRequestBlockCount();
        return {requestBlockIdx, expected};
    }
};

////////////////////////////////////////////////////////////////////////////////

class TTestExecutor final
   : public ITestExecutor
{
private:
    const TInstant TestStartTimestamp;
    IConfigHolderPtr ConfigHolder;
    TDuration SlowRequestThreshold;

    TVector<TRange> Ranges;

    TFileHandle File;
    NAsyncIO::TAsyncIOService AsyncIO;

    TLockFreeStack<ui16> RangesQueue;

    TVector<NThreading::TFuture<void>> Futures;

    TAtomic ShouldStop = 0;
    TAtomic Failed = 0;

    TLog Log;

    TAtomic WriteRequestsCompleted = 0;

private:
    void DoWriteRequest(ui16 rangeIdx);
    void DoReadRequest(ui16 rangeIdx);

    void OnResponse(TInstant startTs, ui16 rangeIdx, TStringBuf reqType);

public:
    TTestExecutor(IConfigHolderPtr configHolder, const TLog& log)
        : TestStartTimestamp(Now())
        , ConfigHolder(configHolder)
        , File(
            TString(ConfigHolder->GetConfig().GetFilePath()),
            EOpenModeFlag::DirectAligned | EOpenModeFlag::RdWr)
        , AsyncIO(0, ConfigHolder->GetConfig().GetIoDepth())
        , Futures(ConfigHolder->GetConfig().GetIoDepth())
        , Log(log)
    {
        auto& config = ConfigHolder->GetConfig();
        for (ui16 i = 0; i < config.GetIoDepth(); ++i) {
            auto& rangeConfig = *config.MutableRanges(i);
            Ranges.emplace_back(
                rangeConfig,
                rangeConfig.GetRequestBlockCount() * config.GetBlockSize());
            RangesQueue.Enqueue(i);
        }

        SlowRequestThreshold = TDuration::Parse(config.GetSlowRequestThreshold());
    }

    bool Run() override;
    void Stop() override;
};

////////////////////////////////////////////////////////////////////////////////

bool TTestExecutor::Run()
{
    STORAGE_INFO("Running load");

    AsyncIO.Start();
    TVector<ui16> buf;

    TDuration phaseDuration = TDuration::Max();
    if (ConfigHolder->GetConfig().HasAlternatingPhase()) {
        phaseDuration = TDuration::Parse(ConfigHolder->GetConfig().GetAlternatingPhase());
    }
    TInstant phaseStartTs = Now();
    ui16 writeRate = ConfigHolder->GetConfig().GetWriteRate();

    while (!AtomicGet(ShouldStop)) {
        buf.clear();
        if (phaseStartTs + phaseDuration < Now()) {
            writeRate = 100 - writeRate;
            phaseStartTs = Now();
        }
        RangesQueue.DequeueAllSingleConsumer(&buf);
        for (auto rangeIdx: buf) {
            if (RandomNumber(100u) >= writeRate) {
                DoReadRequest(rangeIdx);
            } else {
                DoWriteRequest(rangeIdx);
            }
        }
    }

    for (auto future: Futures) {
        future.GetValueSync();
    }
    AsyncIO.Stop();
    File.Close();
    STORAGE_INFO("Stopped");
    return !AtomicGet(Failed);
}

void TTestExecutor::OnResponse(
    TInstant startTs,
    ui16 rangeIdx,
    TStringBuf reqType)
{
    if (reqType == "write") {
        const i64 maxRequestCount =
            ConfigHolder->GetConfig().GetMaxWriteRequestCount();
        if (maxRequestCount &&
            AtomicIncrement(WriteRequestsCompleted) >= maxRequestCount)
        {
            Stop();
        }
    }

    const auto now = Now();
    const auto d = now - startTs;
    if (d > SlowRequestThreshold) {
        STORAGE_WARN("Slow " << reqType << " request: "
            << "range=" << rangeIdx << ", duration=" << d);
    }
}

void TTestExecutor::DoReadRequest(ui16 rangeIdx)
{
    auto& range = Ranges[rangeIdx];
    // https://stackoverflow.com/questions/46114214/lambda-implicit-capture-fails-with-variable-declared-from-structured-binding
    ui64 blockIdx;
    TMaybe<ui64> expected;
    std::tie(blockIdx, expected) = range.RandomRead();

    ui64 blockSize = ConfigHolder->GetConfig().GetBlockSize();

    const auto startTs = Now();
    auto future = AsyncIO.Read(
        File,
        range.Data(),
        range.DataSize(),
        blockIdx * blockSize);

    future.Subscribe([=, this] (const auto& f) mutable {
        OnResponse(startTs, rangeIdx, "read");

        try {
            if (f.GetValue() && f.GetValue() < range.DataSize()) {
                throw yexception() << "read less than expected: "
                    << f.GetValue() << " < " << range.DataSize();
            }
        } catch (...) {
            STORAGE_ERROR("Can't read from file: "
                << CurrentExceptionMessage());
            AtomicSet(Failed, 1);
            Stop();
            return;
        }

        if (!expected) {
            RangesQueue.Enqueue(rangeIdx);
            return;
        }

        auto& range = Ranges[rangeIdx];

        ui64 partSize = range.DataSize() / range.Config.GetWriteParts();
        for (ui64 part = 0; part < range.Config.GetWriteParts(); ++part) {
            TBlockData blockData;
            memcpy(&blockData, range.Data(part * partSize), sizeof(blockData));

            if (blockData.RequestNumber != *expected || blockData.PartNumber != part) {
                STORAGE_ERROR(
                    "[" << rangeIdx << "] Wrong data in block "
                    << blockIdx
                    << " expected RequestNumber " << expected
                    << " actual TBlockData " << blockData);
                AtomicSet(Failed, 1);
                Stop();
                return;
            }
        }
        RangesQueue.Enqueue(rangeIdx);
    });

    Futures[rangeIdx] = future.IgnoreResult();
}

void TTestExecutor::DoWriteRequest(ui16 rangeIdx)
{
    auto& range = Ranges[rangeIdx];

    const auto startTs = Now();
    auto [blockIdx, iteration] = range.NextWrite();
    TBlockData blockData {
        .RequestNumber = iteration,
        .BlockIndex = blockIdx,
        .RangeIdx = rangeIdx,
        .RequestTimestamp = startTs.MicroSeconds(),
        .TestTimestamp = TestStartTimestamp.MicroSeconds(),
        .TestId = ConfigHolder->GetConfig().GetTestId(),
        .Checksum = 0
    };

    TVector<TFuture<void>> futures;
    ui64 blockSize = ConfigHolder->GetConfig().GetBlockSize();
    ui64 partSize = range.DataSize() / range.Config.GetWriteParts();
    for (ui32 part = 0; part < range.Config.GetWriteParts(); ++part) {
        blockData.PartNumber = part;
        blockData.Checksum = 0;
        blockData.Checksum = Crc32c(&blockData, sizeof(blockData));
        ui64 partOffset = part * partSize;
        memcpy(range.Data(partOffset), &blockData, sizeof(blockData));
        auto future = AsyncIO.Write(
            File,
            range.Data(partOffset),
            partSize,
            blockIdx * blockSize + partOffset);

        future.Subscribe([=, this] (const auto& f) mutable {
            OnResponse(startTs, rangeIdx, "write");

            try {
                if (f.GetValue() && f.GetValue() < range.DataSize()) {
                    throw yexception() << "written less than expected: "
                        << f.GetValue() << " < " << partSize;
                }
            } catch (...) {
                STORAGE_ERROR("Can't write to file: "
                    << CurrentExceptionMessage());
                AtomicSet(Failed, 1);
                Stop();
                return;
            }
        });
        futures.push_back(future.IgnoreResult());
    }

    Futures[rangeIdx] = WaitAll(futures);
    Futures[rangeIdx].Subscribe([=, this] (auto) {
        RangesQueue.Enqueue(rangeIdx);
    });
}

void TTestExecutor::Stop()
{
    AtomicSet(ShouldStop, 1);
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

ITestExecutorPtr CreateTestExecutor(
    IConfigHolderPtr configHolder,
    const TLog& log)
{
    return std::make_shared<TTestExecutor>(std::move(configHolder), log);
}

}   // namespace NCloud::NBlockStore
