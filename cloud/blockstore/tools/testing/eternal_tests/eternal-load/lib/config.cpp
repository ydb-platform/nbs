#include "config.h"

#include <library/cpp/protobuf/json/json2proto.h>
#include <library/cpp/protobuf/json/proto2json.h>

#include <util/generic/size_literals.h>
#include <util/random/random.h>

#include <numeric>

namespace NCloud::NBlockStore {

namespace {

////////////////////////////////////////////////////////////////////////////////

ui64 RandomCoprime(ui64 x, ui64 min)
{
    if (min >= x) {
        min = 2;
    }
    for (;;) {
        ui64 r = min + RandomNumber(x - min);
        ui64 g;
        while ((g = std::gcd(r, x)) > 1) {
            r /= g;
        }
        if (r != 1) {
            return r;
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

class TConfigHolder final
    : public IConfigHolder
{
private:
    TTestConfig Config;

public:
    TConfigHolder(IInputStream* input);
    TConfigHolder(
        const TString& filePath,
        ui64 fileSize,
        ui16 ioDepth,
        ui64 blockSize,
        ui16 writeRate,
        ui64 requestBlockCount,
        ui64 writeParts,
        TString alternatingPhase,
        ui64 maxWriteRequestCount,
        ui64 minReadSize,
        ui64 maxReadSize,
        ui64 minWriteSize,
        ui64 maxWriteSize,
        ui64 minRegionSize,
        ui64 maxRegionSize);

    TTestConfig& GetConfig() override;
    void DumpConfig(const TString& filePath) override;

private:
    void GenerateMissingFields();
};

////////////////////////////////////////////////////////////////////////////////

TConfigHolder::TConfigHolder(IInputStream* input)
    : Config(NProtobufJson::Json2Proto<TTestConfig>(*input))
{
    GenerateMissingFields();
}

TConfigHolder::TConfigHolder(
        const TString& filePath,
        ui64 fileSize,
        ui16 ioDepth,
        ui64 blockSize,
        ui16 writeRate,
        ui64 requestBlockCount,
        ui64 writeParts,
        TString alternatingPhase,
        ui64 maxWriteRequestCount,
        ui64 minReadSize,
        ui64 maxReadSize,
        ui64 minWriteSize,
        ui64 maxWriteSize,
        ui64 minRegionSize,
        ui64 maxRegionSize)
{
    Config.SetFilePath(filePath);
    Config.SetFileSize(fileSize);
    Config.SetBlockSize(blockSize);
    Config.SetWriteRate(writeRate);
    Config.SetIoDepth(ioDepth);

    auto& ranges = *Config.MutableRanges();

    for (ui16 i = 0; i < ioDepth; ++i) {
        auto& range = *ranges.Add();
        range.SetRequestBlockCount(requestBlockCount);
        range.SetWriteParts(writeParts);
    }

    if (!alternatingPhase.empty()) {
        Config.SetAlternatingPhase(alternatingPhase);
    }

    if (maxWriteRequestCount) {
        Config.SetMaxWriteRequestCount(maxWriteRequestCount);
    }

    GenerateMissingFields();

    auto& fileTestConfig = *Config.MutableFileTest();
    fileTestConfig.SetMinReadSize(minReadSize);
    fileTestConfig.SetMaxReadSize(maxReadSize);
    fileTestConfig.SetMinWriteSize(minWriteSize);
    fileTestConfig.SetMaxWriteSize(maxWriteSize);
    fileTestConfig.SetMinRegionSize(minRegionSize);
    fileTestConfig.SetMaxRegionSize(maxRegionSize);
}

void TConfigHolder::GenerateMissingFields()
{
    if (!Config.HasTestId()) {
        Config.SetTestId(RandomNumber<ui64>());
    }
    if (!Config.HasRangeBlockCount()) {
        Config.SetRangeBlockCount(Config.GetFileSize() /
            (static_cast<ui64>(Config.GetIoDepth()) * Config.GetBlockSize()));
    }

    for (ui16 i = 0; i < Config.GetIoDepth(); ++i) {
        if (i >= Config.GetRanges().size()) {
            Config.MutableRanges()->Add();
        }
        auto& range = *Config.MutableRanges(i);
        if (!range.HasRequestBlockCount()) {
            range.SetRequestBlockCount(1);
        }
        range.SetRequestCount(Config.GetRangeBlockCount() / range.GetRequestBlockCount());
        range.SetStartOffset(i * Config.GetRangeBlockCount());
        if (!range.HasStep()) {
            range.SetStep(RandomCoprime(range.GetRequestCount(), 1_GB
                / (range.GetRequestBlockCount() * Config.GetBlockSize())));
        }
        if (!range.HasStartBlockIdx()) {
            range.SetStartBlockIdx(i);
        }
        if (!range.HasLastBlockIdx()) {
            range.SetLastBlockIdx(i);
            range.SetNumberToWrite(0);
        }
        if (!range.HasWriteParts()) {
            range.SetWriteParts(1);
        }
    }
}

TTestConfig& TConfigHolder::GetConfig()
{
    return Config;
}

void TConfigHolder::DumpConfig(const TString& filePath)
{
    auto openMode = EOpenModeFlag::WrOnly | EOpenModeFlag::CreateAlways;
    TFile file(filePath, openMode);

    TString config = NProtobufJson::Proto2Json(Config, {.FormatOutput = true});
    file.Write(config.data(), config.length());
}

}    //  namespace

////////////////////////////////////////////////////////////////////////////////

IConfigHolderPtr CreateTestConfig(
    const TString& filePath,
    ui64 fileSize,
    ui16 ioDepth,
    ui64 blockSize,
    ui16 writeRate,
    ui64 requestBlockCount,
    ui64 writeParts,
    TString alternatingPhase,
    ui64 maxWriteRequestCount,
    ui64 minReadSize,
    ui64 maxReadSize,
    ui64 minWriteSize,
    ui64 maxWriteSize,
    ui64 minRegionSize,
    ui64 maxRegionSize)
{
    return std::make_shared<TConfigHolder>(
        filePath,
        fileSize,
        ioDepth,
        blockSize,
        writeRate,
        requestBlockCount,
        writeParts,
        alternatingPhase,
        maxWriteRequestCount,
        minReadSize,
        maxReadSize,
        minWriteSize,
        maxWriteSize,
        minRegionSize,
        maxRegionSize);
}

IConfigHolderPtr CreateTestConfig(const TString& filePath)
{
    TFileInput input(filePath);
    return std::make_shared<TConfigHolder>(&input);
}

}   // namespace NCloud::NBlockStore
