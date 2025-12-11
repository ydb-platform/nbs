#include "test_executor.h"

#include <util/generic/algorithm.h>
#include <util/generic/size_literals.h>
#include <util/generic/vector.h>
#include <util/random/shuffle.h>
#include <util/stream/format.h>
#include <util/stream/output.h>
#include <util/system/byteorder.h>
#include <util/system/file.h>

namespace NCloud::NBlockStore {

namespace {

////////////////////////////////////////////////////////////////////////////////

class TTestExecutorRead final: public ITestExecutor
{
private:
    TAtomic ShouldStop = 0;

    TString FilePath;
    TTestExecutorConfigPtr Config;

public:
    TTestExecutorRead(TString filePath, TTestExecutorConfigPtr config)
        : FilePath(std::move(filePath))
        , Config(std::move(config))
    {}

    TTestExecutorReport Run(
        TAtomic& waitingForStart,
        TAtomic& shouldStart) override;

    void Stop() override;
};

////////////////////////////////////////////////////////////////////////////////

class TTestExecutorWrite final: public ITestExecutor
{
private:
    TAtomic ShouldStop = 0;

    TString FilePath;
    TTestExecutorConfigPtr Config;

public:
    TTestExecutorWrite(TString filePath, TTestExecutorConfigPtr config)
        : FilePath(std::move(filePath))
        , Config(std::move(config))
    {}

    TTestExecutorReport Run(
        TAtomic& waitingForStart,
        TAtomic& shouldStart) override;

    void Stop() override;
};

////////////////////////////////////////////////////////////////////////////////

TVector<ui64> GenerateOffsetsQueue(const TTestExecutorConfig& config)
{
    const ui64 blocksCount =
        (config.EndOffset - config.StartOffset) / config.BlockSize;
    TVector<ui64> offsetsQueue;

    for (ui64 i = 0u; i < blocksCount; i += config.Step) {
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

std::unique_ptr<char[]>
GenerateData(ui64 offset, ui32 blockSize, ETestPattern testPattern)
{
    static const ui32 multiplier = 53;
    ui32 coeff = 1;

    std::unique_ptr<char[]> data(new char[blockSize]);
    if (testPattern == ETestPattern::CheckZero) {
        memset(data.get(), '\0', blockSize);
    } else {
        for (ui32 i = 0; i < blockSize; i++) {
            data[i] = (offset + i * coeff) % 256;
            coeff = (coeff * multiplier) % 256;
        }
    }
    return data;
}

////////////////////////////////////////////////////////////////////////////////

EOpenMode GetOpenFlags(bool direct)
{
    EOpenMode flags = EOpenModeFlag::RdWr;
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
        : Data{ptr, size}
    {}
};

////////////////////////////////////////////////////////////////////////////////

TTestExecutorReport TTestExecutorRead::Run(
    TAtomic& waitingForStart,
    TAtomic& shouldStart)
{
    TFile file(FilePath, GetOpenFlags(Config->DirectIo));

    const auto offsetsQueue = GenerateOffsetsQueue(*Config);

    AtomicAdd(waitingForStart, 1);
    while (AtomicGet(shouldStart) != 1 && AtomicGet(ShouldStop) == 0) {
    }

    auto startTime = Now();

    for (const auto offset: offsetsQueue) {
        if (AtomicGet(ShouldStop)) {
            return {};
        }

        auto expectedData =
            GenerateData(offset, Config->BlockSize, Config->TestPattern);
        std::unique_ptr<char[]> actualData(new char[Config->BlockSize]);

        std::fill_n(
            reinterpret_cast<ui32*>(actualData.get()),
            Config->BlockSize / sizeof(ui32),
            SwapBytes32(0xdeadbeef));

        file.Seek(offset, sSet);
        file.Read(actualData.get(), Config->BlockSize);

        if (memcmp(actualData.get(), expectedData.get(), Config->BlockSize)) {
            ythrow yexception() << "Actual data differs from expected: "
                                << "#offset = " << offset << " data = \n"
                                << THexDataDump {
                                       actualData.get(),
                                       Config -> BlockSize
                                   };
        }
    }

    auto finishTime = Now();

    return {startTime, finishTime};
}

void TTestExecutorRead::Stop()
{
    AtomicSet(ShouldStop, 1);
}

////////////////////////////////////////////////////////////////////////////////

TTestExecutorReport TTestExecutorWrite::Run(
    TAtomic& waitingForStart,
    TAtomic& shouldStart)
{
    TFile file(FilePath, GetOpenFlags(Config->DirectIo));

    const auto offsetsQueue = GenerateOffsetsQueue(*Config);

    AtomicAdd(waitingForStart, 1);
    while (AtomicGet(shouldStart) != 1 && AtomicGet(ShouldStop) == 0) {
    }

    auto startTime = Now();

    for (const auto offset: offsetsQueue) {
        if (AtomicGet(ShouldStop)) {
            return {};
        }

        auto data =
            GenerateData(offset, Config->BlockSize, Config->TestPattern);

        file.Seek(offset, sSet);
        file.Write(data.get(), Config->BlockSize);
    }

    auto finishTime = Now();

    return {startTime, finishTime};
}

void TTestExecutorWrite::Stop()
{
    AtomicSet(ShouldStop, 1);
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
