#include "test_executor.h"

#include <util/generic/algorithm.h>
#include <util/generic/size_literals.h>
#include <util/generic/vector.h>
#include <util/random/shuffle.h>
#include <util/stream/format.h>
#include <util/stream/output.h>
#include <util/system/byteorder.h>
#include <util/system/file.h>

#include <atomic>

namespace NCloud::NBlockStore {

namespace {

////////////////////////////////////////////////////////////////////////////////

class TTestExecutorRead final
    : public ITestExecutor
{
private:
    std::atomic_flag ShouldStop;

    TString FilePath;
    TTestExecutorConfigPtr Config;

public:
    TTestExecutorRead(TString filePath, TTestExecutorConfigPtr config)
        : FilePath(std::move(filePath))
        , Config(std::move(config))
    {}

    TTestExecutorReport Run(std::latch& waitingForStart) override;

    void Stop() override;
};

////////////////////////////////////////////////////////////////////////////////

class TTestExecutorWrite final
    : public ITestExecutor
{
private:
    std::atomic_flag ShouldStop;

    TString FilePath;
    TTestExecutorConfigPtr Config;

public:
    TTestExecutorWrite(TString filePath, TTestExecutorConfigPtr config)
        : FilePath(std::move(filePath))
        , Config(std::move(config))
    {}

    TTestExecutorReport Run(std::latch& waitingForStart) override;

    void Stop() override;
};

////////////////////////////////////////////////////////////////////////////////

TVector<ui64> GenerateOffsetsQueue(const TTestExecutorConfig& config)
{
    const ui64 blocksCount =
        (config.EndOffset - config.StartOffset) / config.BlockSize;
    TVector<ui64> offsetsQueue;

    for (ui64 i = 0UL; i < blocksCount; i += config.Step) {
        offsetsQueue.push_back(config.BlockSize * i + config.StartOffset);
    }
    if (config.TestPattern == ETestPattern::Reverse) {
        Reverse(offsetsQueue.begin(), offsetsQueue.end());
    }
    if (config.TestPattern == ETestPattern::Random) {
        Shuffle(offsetsQueue.begin(), offsetsQueue.end());
    }
    return offsetsQueue;
}

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<char[]> GenerateData(
    ui64 offset,
    ui32 blockSize,
    ETestPattern testPattern)
{
    static const ui32 multiplier = 53;
    ui32 coeff = 1;

    auto data = std::make_unique<char[]>(blockSize);
    if (testPattern == ETestPattern::CheckZero) {
        memset(data.get(), 0, blockSize);
    } else {
        for (ui32 i = 0; i < blockSize; i++) {
            data[i] = (offset + i * coeff) % 256;
            coeff = (coeff * multiplier) % 256;
        }
    }
    return data;
}

////////////////////////////////////////////////////////////////////////////////

EOpenMode GetOpenFlags(bool direct, EOpenMode flags) {
    if (direct) {
        flags |= EOpenModeFlag::DirectAligned;
        flags |= EOpenModeFlag::Sync;
    }
    return flags;
}

////////////////////////////////////////////////////////////////////////////////

struct THexDataDump
{
    TStringBuf Data;

    THexDataDump(const char* ptr, ui32 size)
        : Data {ptr, size}
    {}
};

////////////////////////////////////////////////////////////////////////////////

TTestExecutorReport TTestExecutorRead::Run(std::latch& waitingForStart)
{
    TFile file;
    TVector<ui64> offsetsQueue;

    try {
        file = TFile(
            FilePath,
            GetOpenFlags(Config->DirectIo, EOpenModeFlag::RdOnly));

        offsetsQueue = GenerateOffsetsQueue(*Config);
    } catch (...) {
        waitingForStart.count_down();
        throw;
    }

    waitingForStart.arrive_and_wait();

    auto startTime = Now();

    for (const auto offset: offsetsQueue) {
        if (ShouldStop.test()) {
            return {};
        }

        auto expectedData = GenerateData(
            offset,
            Config->BlockSize,
            Config->TestPattern);

        auto actualData = std::make_unique<char[]>(Config->BlockSize);

        std::fill_n(
            reinterpret_cast<ui32*>(actualData.get()),
            Config->BlockSize / sizeof(ui32),
            SwapBytes32(0xdeadbeef)
        );

        file.Seek(offset, sSet);
        file.Read(actualData.get(), Config->BlockSize);

        if (memcmp(actualData.get(), expectedData.get(), Config->BlockSize)) {
            ythrow yexception()
                << "Actual data differs from expected: "
                << "#offset = " << offset
                << " data = \n" << THexDataDump {actualData.get(), Config->BlockSize};
        }
    }

    auto finishTime = Now();

    return {startTime, finishTime};
}

void TTestExecutorRead::Stop()
{
    ShouldStop.test_and_set();
}

////////////////////////////////////////////////////////////////////////////////

TTestExecutorReport TTestExecutorWrite::Run(std::latch& waitingForStart)
{
    TFile file;
    TVector<ui64> offsetsQueue;

    try {
        file = TFile(
            FilePath,
            GetOpenFlags(Config->DirectIo, EOpenModeFlag::WrOnly));

        offsetsQueue = GenerateOffsetsQueue(*Config);
    } catch (...) {
        waitingForStart.count_down();
        throw;
    }

    waitingForStart.arrive_and_wait();

    auto startTime = Now();

    for (const auto offset: offsetsQueue) {
        if (ShouldStop.test()) {
            return {};
        }

        auto data = GenerateData(offset, Config->BlockSize, Config->TestPattern);

        file.Seek(offset, sSet);
        file.Write(data.get(), Config->BlockSize);
    }

    auto finishTime = Now();

    return {startTime, finishTime};
}

void TTestExecutorWrite::Stop()
{
    ShouldStop.test_and_set();
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

ITestExecutorPtr CreateTestExecutor(
    const ETestExecutorType& type,
    TString filePath,
    TTestExecutorConfigPtr testExecutorConfig)
{
    switch (type) {
        case ETestExecutorType::Read:
            return std::make_shared<TTestExecutorRead>(
                std::move(filePath),
                std::move(testExecutorConfig));
        case ETestExecutorType::Write:
            return std::make_shared<TTestExecutorWrite>(
                std::move(filePath),
                std::move(testExecutorConfig));
        default:
            ythrow yexception() << "invalid executor type";
    }
}

}   // namespace NCloud::NBlockStore

template <>
void Out<NCloud::NBlockStore::THexDataDump>(
    IOutputStream& out,
    const NCloud::NBlockStore::THexDataDump& dump)
{
    const ui32 maxLen = Min<ui32>(dump.Data.size(), 16_KB);

    for (ui32 offset = 0; offset < maxLen; offset += 16) {
        out << LeftPad(Hex(offset), 8) << " "
            << HexText(dump.Data.SubStr(offset, 16)) << '\n';
    }
    if (maxLen < dump.Data.size()) {
        out << "... truncated ...\n";
    }
}
