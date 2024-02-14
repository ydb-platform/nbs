#include <cloud/blockstore/tools/testing/eternal_tests/eternal-load/lib/config.h>

#include <library/cpp/getopt/small/last_getopt.h>

#include <util/datetime/base.h>
#include <util/generic/size_literals.h>
#include <util/generic/vector.h>

using namespace NCloud::NBlockStore;

////////////////////////////////////////////////////////////////////////////////

struct TOptions
{
    TString ConfigPath;
    TString DevicePath;
    TString RangesStr;
    TString LogPath;
    TVector<ui32> Ranges;

    void Parse(int argc, const char** argv)
    {
        using namespace NLastGetopt;

        TOpts opts;
        opts.AddHelpOption();

        opts.AddLongOption("config-path", "path to the eternal-test config")
            .Required()
            .RequiredArgument("STR")
            .StoreResult(&ConfigPath);

        opts.AddLongOption("device-path", "path to the block device")
            .Required()
            .RequiredArgument("STR")
            .StoreResult(&DevicePath);

        opts.AddLongOption(
            "log-path",
            "path to the folder, where you want to save logs of validation")
            .Required()
            .RequiredArgument("STR")
            .StoreResult(&LogPath);

        opts.AddLongOption(
            "ranges",
            "specify range indexes for validation, separated by comma")
            .Required()
            .RequiredArgument("STR")
            .StoreResult(&RangesStr)
            .Handler0([this] {
                TStringBuf buf(RangesStr);
                TStringBuf arg;
                while (buf.NextTok(',', arg)) {
                    Ranges.push_back(FromString<ui32>(arg));
                }
            });

        TOptsParseResultException(&opts, argc, argv);
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

    TFile File(options.DevicePath, EOpenModeFlag::RdWr);

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
            TBlockData blockData;
            File.Seek((i + startOffset) * requestSize, sSet);
            File.Read(&blockData, sizeof(blockData));
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

    return 0;
}
