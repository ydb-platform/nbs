#include "config.h"

#include <util/generic/size_literals.h>
#include <util/random/random.h>

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
    NJson::TJsonValue Value;
    TTestConfig Config;

public:
    TConfigHolder(NJson::TJsonValue value);
    TConfigHolder(
        const TString& filePath,
        ui64 fileSize,
        ui16 ioDepth,
        ui64 blockSize,
        ui16 writeRate,
        ui64 requestBlockCount,
        ui64 writeParts);

    TTestConfig& GetConfig() override;
    void DumpConfig(const TString& filePath) override;

private:
    void GenerateMissingFields();
};

////////////////////////////////////////////////////////////////////////////////

TConfigHolder::TConfigHolder(NJson::TJsonValue value)
    : Value(std::move(value))
    , Config(&Value)
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
        ui64 writeParts)
    : Value(NJson::TJsonValue(""))
    , Config(&Value)
{
    Config.FilePath() = filePath;
    Config.FileSize() = fileSize;
    Config.BlockSize() = blockSize;
    Config.WriteRate() = writeRate;
    Config.IoDepth() = ioDepth;

    for (ui16 i = 0; i < ioDepth; ++i) {
        Config.Ranges(i).RequestBlockCount() = requestBlockCount;
        Config.Ranges(i).WriteParts() = writeParts;
    }

    GenerateMissingFields();
}

void TConfigHolder::GenerateMissingFields()
{
    if (!Config.HasTestId()) {
        Config.TestId() = RandomNumber<ui64>();
    }
    if (!Config.HasRangeBlockCount()) {
        Config.RangeBlockCount() = Config.FileSize() /
            (static_cast<ui64>(Config.IoDepth()) * Config.BlockSize());
    }

    for (ui16 i = 0; i < Config.IoDepth(); ++i) {
        auto range = Config.Ranges()[i];
        if (!range.HasRequestBlockCount()) {
            range.RequestBlockCount() = 1;
        }
        range.RequestCount() = Config.RangeBlockCount() / range.RequestBlockCount();
        range.StartOffset() = i * Config.RangeBlockCount();
        if (!range.HasStep()) {
            range.Step() = RandomCoprime(range.RequestCount(), 1_GB
                / (range.RequestBlockCount() * Config.BlockSize()));
        }
        if (!range.HasStartBlockIdx()) {
            range.StartBlockIdx() = i;
        }
        if (!range.HasLastBlockIdx()) {
            range.LastBlockIdx() = i;
            range.NumberToWrite() = 0;
        }
        if (!range.HasWriteParts()) {
            range.WriteParts() = 1;
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

    TString config = NJson::WriteJson(Config.GetRawValue());
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
    ui64 writeParts)
{
    return std::make_shared<TConfigHolder>(
        filePath,
        fileSize,
        ioDepth,
        blockSize,
        writeRate,
        requestBlockCount,
        writeParts);
}

IConfigHolderPtr CreateTestConfig(const TString& filePath)
{
    TFileInput input(filePath);
    NJson::TJsonValue value;
    ReadJsonTree(&input, &value, true);
    return std::make_shared<TConfigHolder>(std::move(value));
}

}   // namespace NCloud::NBlockStore

