#include <cloud/blockstore/tools/testing/eternal_tests/eternal-load/lib/config.h>

#include <library/cpp/getopt/small/last_getopt.h>

#include <util/datetime/base.h>
#include <util/generic/size_literals.h>
#include <util/generic/vector.h>

using namespace NCloud::NBlockStore;

////////////////////////////////////////////////////////////////////////////////

constexpr ui64 MAX_MIRROR_REPLICAS = 3;
constexpr ui64 PAGE_SIZE = 4096;

////////////////////////////////////////////////////////////////////////////////

enum class EValidationMode
{
    Checksum,
    Mirror,
};

////////////////////////////////////////////////////////////////////////////////

struct TOptions
{

    TString ConfigPath;
    TString DevicePath;
    TString RangesStr;
    TString BlocksStr;
    TString LogPath;
    TVector<ui32> Ranges;
    TVector<ui64> Blocks;
    EValidationMode Mode = EValidationMode::Checksum;
    ui64 BlockSize = 4096;

    void Parse(int argc, const char** argv)
    {
        using namespace NLastGetopt;

        TOpts opts;
        opts.AddHelpOption();

        opts.AddLongOption("config-path", "path to the eternal-test config")
            .RequiredArgument("STR")
            .StoreResult(&ConfigPath);

        opts.AddLongOption("device-path", "path to the block device")
            .Required()
            .RequiredArgument("STR")
            .StoreResult(&DevicePath);

        opts.AddLongOption(
            "log-path",
            "path to the folder, where you want to save logs of validation")
            .RequiredArgument("STR")
            .StoreResult(&LogPath);

        opts.AddLongOption(
            "ranges",
            "specify range indexes for validation, separated by comma")
            .RequiredArgument("STR")
            .StoreResult(&RangesStr)
            .Handler0([this] {
                TStringBuf buf(RangesStr);
                TStringBuf arg;
                while (buf.NextTok(',', arg)) {
                    Ranges.push_back(FromString<ui32>(arg));
                }
            });

        opts.AddLongOption(
            "blocks",
            "specify individual blocks NUM or ranges NUM-NUM for validation, separated by comma")
            .RequiredArgument("STR")
            .StoreResult(&BlocksStr)
            .Handler0([this] {
                TStringBuf buf(BlocksStr);
                TStringBuf firstStr;
                while (buf.NextTok(',', firstStr)) {
                    TStringBuf lastStr = firstStr.SplitOff('-');
                    ui64 first = FromString<ui64>(firstStr);
                    ui64 last = lastStr.empty() ? first : FromString<ui64>(lastStr);
                    for (ui64 block = first; block <= last; block++) {
                        Blocks.push_back(block);
                    }
                }
            });

        opts.AddLongOption(
            "block-size",
            "specify block-size (default 4096)")
            .RequiredArgument("NUM")
            .StoreResult(&BlockSize);

        opts.AddLongOption(
            "mode",
            "specify checksum (default) or mirror validation mode")
            .Handler1T<TString>([this] (const auto& s) {
                if (s == "checksum") {
                    Mode = EValidationMode::Checksum;
                } else if (s == "mirror") {
                    Mode = EValidationMode::Mirror;
                } else {
                    Y_ABORT("validation mode must be either checksum or mirror");
                }
            });

        TOptsParseResultException(&opts, argc, argv);

        if (Mode == EValidationMode::Checksum) {
            Y_ENSURE(ConfigPath.size(), "you must specify config-path in checksum validation mode");
            Y_ENSURE(Ranges.size(), "you must specify ranges in checksum validation mode");
            Y_ENSURE(LogPath.size(), "you must specify log-path in checksum validation mode");
        }

        if (Mode == EValidationMode::Mirror) {
            Y_ENSURE(Blocks.size(), "you must specify blocks in mirror validation mode");
        }
    }
};

void ValidateRanges(TOptions options)
{
    TFile File(options.DevicePath, EOpenModeFlag::RdOnly | EOpenModeFlag::DirectAligned);

    const auto& configHolder = CreateTestConfig(options.ConfigPath);
    for (ui32 rangeIdx: options.Ranges) {
        Cout << "Start validation of " << rangeIdx << " range" << Endl;

        const auto& config = configHolder->GetConfig().GetRanges()[rangeIdx];

        ui64 len = config.GetRequestCount();
        ui64 startOffset = config.GetStartOffset();
        ui64 requestSize = config.GetRequestBlockCount() * 4_KB;

        TVector<ui64> expected(len);

        ui64 step = config.GetStep();
        ui64 curBlockIdx = config.GetLastBlockIdx();
        ui64 curNum = config.GetNumberToWrite();
        ui64 cnt = 0;
        while (cnt < len && curNum != 0) {
            curBlockIdx = (curBlockIdx + len - step) % len;
            expected[curBlockIdx] = --curNum;
            ++cnt;
        }

        TString path = options.LogPath + "range_" + ToString(rangeIdx) + ".log";
        TFileOutput out(TFile(path, EOpenModeFlag::CreateAlways | EOpenModeFlag::WrOnly));

        for (ui64 i = 0; i < len; ++i) {
            alignas(PAGE_SIZE) char buf[PAGE_SIZE];
            File.Seek((i + startOffset) * requestSize, sSet);
            File.Read(buf, PAGE_SIZE);
            TBlockData blockData = *reinterpret_cast<TBlockData*>(buf);
            if (blockData.RequestNumber != expected[i]) {
                out <<
                    "[" << rangeIdx << "] Wrong data in block "
                    << (i + startOffset)
                    << " expected " << expected[i]
                    << " actual { " << blockData.RequestNumber
                    << " " << blockData.BlockIndex
                    << " " << blockData.RangeIdx
                    << " " << TInstant::MicroSeconds(blockData.RequestTimestamp)
                    << " " << TInstant::MicroSeconds(blockData.TestTimestamp)
                    << " " << blockData.TestId
                    << " " << blockData.Checksum
                    << " }\n";
            }
        }

        Cout << "Finish validation of " << rangeIdx << " range" << Endl;
        Cout << "Log written to " << path.Quote() << Endl;
    }
}

void ValidateBlocks(TOptions options)
{
    TFile file(options.DevicePath, EOpenModeFlag::RdOnly | EOpenModeFlag::DirectAligned);

    auto compare = [](auto& x, auto& y) {
        return memcmp(&x, &y, sizeof(TBlockData));
    };

    for (ui64 blockIndex: options.Blocks) {
        std::set<TBlockData, decltype(compare)> uniqueBlocks(compare);

        for (ui64 i = 0; i < MAX_MIRROR_REPLICAS; i++) {
            alignas(PAGE_SIZE) char buf[PAGE_SIZE];
            file.Seek(blockIndex * options.BlockSize, sSet);
            file.Read(buf, PAGE_SIZE);
            uniqueBlocks.emplace(*reinterpret_cast<TBlockData*>(buf));
        }

        if (uniqueBlocks.size() == 1) {
            continue;
        }

        for (const auto& block: uniqueBlocks) {
            Cout
                << "mismatch in block data {"
                << " " << block.RequestNumber
                << " " << block.PartNumber
                << " " << block.BlockIndex
                << " " << block.RangeIdx
                << " " << TInstant::MicroSeconds(block.RequestTimestamp)
                << " " << TInstant::MicroSeconds(block.TestTimestamp)
                << " " << block.TestId
                << " " << block.Checksum
                << " }\n";
        }
    }
}

int main(int argc, const char** argv)
{
    TOptions options;
    try {
        options.Parse(argc, argv);
    } catch (...) {
        Cout << CurrentExceptionMessage() << Endl;
        return 1;
    }

    if (options.Mode == EValidationMode::Checksum) {
        ValidateRanges(std::move(options));
    } else {
        ValidateBlocks(std::move(options));
    }

    return 0;
}
