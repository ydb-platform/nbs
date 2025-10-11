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
    explicit TConfigHolder(IInputStream* input);
    explicit TConfigHolder(const TCreateTestConfigArguments& args);

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

TConfigHolder::TConfigHolder(const TCreateTestConfigArguments& args)
{
    Config.SetFilePath(args.FilePath);
    Config.SetFileSize(args.FileSize);
    Config.SetBlockSize(args.BlockSize);
    Config.SetWriteRate(args.WriteRate);
    Config.SetIoDepth(args.IoDepth);

    auto& ranges = *Config.MutableRanges();

    for (ui16 i = 0; i < args.IoDepth; ++i) {
        auto& range = *ranges.Add();
        range.SetRequestBlockCount(args.RequestBlockCount);
        range.SetWriteParts(args.WriteParts);
    }

    if (!args.AlternatingPhase.empty()) {
        Config.SetAlternatingPhase(args.AlternatingPhase);
    }

    if (args.MaxWriteRequestCount) {
        Config.SetMaxWriteRequestCount(args.MaxWriteRequestCount);
    }

    GenerateMissingFields();

    auto& fileTestConfig = *Config.MutableUnalignedTest();
    fileTestConfig.SetMinReadByteCount(args.MinReadByteCount);
    fileTestConfig.SetMaxReadByteCount(args.MaxReadByteCount);
    fileTestConfig.SetMinWriteByteCount(args.MinWriteByteCount);
    fileTestConfig.SetMaxWriteByteCount(args.MaxWriteByteCount);
    fileTestConfig.SetMinRegionByteCount(args.MinRegionByteCount);
    fileTestConfig.SetMaxRegionByteCount(args.MaxRegionByteCount);
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

IConfigHolderPtr CreateTestConfig(const TCreateTestConfigArguments& args)
{
    return std::make_shared<TConfigHolder>(args);
}

IConfigHolderPtr LoadTestConfig(const TString& filePath)
{
    TFileInput input(filePath);
    return std::make_shared<TConfigHolder>(&input);
}

}   // namespace NCloud::NBlockStore
