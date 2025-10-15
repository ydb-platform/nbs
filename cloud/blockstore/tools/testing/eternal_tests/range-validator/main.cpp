#include <cloud/blockstore/tools/testing/eternal_tests/eternal-load/lib/config.h>

#include <cloud/blockstore/tools/testing/eternal_tests/range-validator/lib/validate.h>

#include <library/cpp/getopt/small/last_getopt.h>

#include <util/generic/vector.h>

using namespace NCloud::NBlockStore;

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
        auto configHolder = LoadTestConfig(options.ConfigPath);
        for (ui32 rangeIdx : options.RangeIndices) {
            Cout << "Start validation of " << rangeIdx << " range" << Endl;

            auto res = ValidateRange(file, std::move(configHolder), rangeIdx);
            Cout << "GuessedStep " << res.GuessedStep
                 << "GuessedLastBlockIdx " << res.GuessedLastBlockIdx
                 << "GuessedNumberToWrite " << res.GuessedNumberToWrite
                 << Endl;

            TString path = options.LogPath + "range_" + ToString(rangeIdx) + ".log";
            TFileOutput out(TFile(path, EOpenModeFlag::CreateAlways | EOpenModeFlag::WrOnly));

            for (const auto& block : res.InvalidBlocks) {
                out << "[" << rangeIdx << "]"
                    << " Wrong data in block " << block
                    << Endl;
            }

            Cout << "Finish validation of " << rangeIdx << " range" << Endl;
            Cout << "Log written to " << path.Quote() << Endl;
        }
    } else {
        auto res = ValidateBlocks(
            std::move(file),
            options.BlockSize,
            std::move(options.BlockIndices));

        for (const auto& block : res) {
            Cout << "mismatch in block data " << block << Endl;
        }
    }

    return 0;
}
