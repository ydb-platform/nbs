#include <cloud/blockstore/tools/testing/eternal_tests/eternal-load/lib/config.h>

#include <cloud/blockstore/tools/testing/eternal_tests/range-validator/lib/validate.h>

#include <library/cpp/getopt/small/last_getopt.h>

#include <util/datetime/base.h>
#include <util/generic/vector.h>

using namespace NCloud::NBlockStore;

////////////////////////////////////////////////////////////////////////////////

constexpr ui64 MaxMirrorReplicas = 3;

////////////////////////////////////////////////////////////////////////////////

enum class EValidationMode
{
    Checksum,
    Mirror,
};

////////////////////////////////////////////////////////////////////////////////

inline IOutputStream& operator<<(IOutputStream& out, const TBlockData& data)
{
    out << "{"
        << " " << data.RequestNumber
        << " " << data.PartNumber
        << " " << data.BlockIndex
        << " " << data.RangeIdx
        << " " << TInstant::MicroSeconds(data.RequestTimestamp)
        << " " << TInstant::MicroSeconds(data.TestTimestamp)
        << " " << data.TestId
        << " " << data.Checksum
        << " }";

    return out;
}

////////////////////////////////////////////////////////////////////////////////

struct TOptions
{
    TString ConfigPath;
    TString DevicePath;
    TString LogPath;
    TVector<ui32> RangeIndices;
    TVector<ui64> BlockIndices;
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
            "specify range indices for validation, separated by comma")
            .RequiredArgument("STR")
            .Handler1T<TString>([this] (const auto& s) {
                TStringBuf buf(s);
                TStringBuf arg;
                while (buf.NextTok(',', arg)) {
                    RangeIndices.push_back(FromString<ui32>(arg));
                }
            });

        opts.AddLongOption(
            "blocks",
            "specify individual block indices NUM or ranges NUM-NUM for validation, separated by comma")
            .RequiredArgument("STR")
            .Handler1T<TString>([this] (const auto& s) {
                TStringBuf buf(s);
                TStringBuf firstStr;
                while (buf.NextTok(',', firstStr)) {
                    TStringBuf lastStr = firstStr.SplitOff('-');
                    ui64 first = FromString<ui64>(firstStr);
                    ui64 last = lastStr.empty() ? first : FromString<ui64>(lastStr);
                    for (ui64 idx = first; idx <= last; idx++) {
                        BlockIndices.push_back(idx);
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
            Y_ENSURE(RangeIndices.size(), "you must specify ranges in checksum validation mode");
            Y_ENSURE(LogPath.size(), "you must specify log-path in checksum validation mode");
        }

        if (Mode == EValidationMode::Mirror) {
            Y_ENSURE(BlockIndices.size(), "you must specify blocks in mirror validation mode");
        }
    }
};

void ValidateBlocks(TFile file, ui64 blockSize, TVector<ui64> blockIndices)
{
    for (ui64 blockIndex: blockIndices) {
        std::set<TBlockData> blocks;

        for (ui64 i = 0; i < MaxMirrorReplicas; i++) {
            auto blockData = ReadBlockData(file, blockIndex * blockSize);
            blocks.emplace(blockData);
        }

        if (blockIndices.size() == 1) {
            continue;
        }

        for (const auto& block: blocks) {
            Cout << "mismatch in block data " << block << Endl;
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

    TFile file(options.DevicePath, EOpenModeFlag::RdOnly | EOpenModeFlag::DirectAligned);

    if (options.Mode == EValidationMode::Checksum) {
        auto configHolder = CreateTestConfig(options.ConfigPath);
        for (ui32 rangeIdx : options.RangeIndices) {
            Cout << "Start validation of " << rangeIdx << " range" << Endl;

            auto results = ValidateRange(
                file,
                std::move(configHolder),
                rangeIdx);

            TString path = options.LogPath + "range_" + ToString(rangeIdx) + ".log";
            TFileOutput out(TFile(path, EOpenModeFlag::CreateAlways | EOpenModeFlag::WrOnly));

            for (const auto& result : results) {
                out << "[" << rangeIdx << "]"
                    << " Wrong data in block " << result.BlockIdx
                    << " expected " << result.Expected
                    << " actual " << result.Actual
                    << Endl;
            }

            Cout << "Finish validation of " << rangeIdx << " range" << Endl;
            Cout << "Log written to " << path.Quote() << Endl;
        }
    } else {
        ValidateBlocks(
            std::move(file),
            options.BlockSize,
            std::move(options.BlockIndices));
    }

    return 0;
}
