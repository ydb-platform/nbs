#include <cloud/blockstore/tools/testing/eternal_tests/eternal-load/lib/config.h>

#include <library/cpp/getopt/small/last_getopt.h>

#include <util/datetime/base.h>
#include <util/generic/size_literals.h>
#include <util/generic/vector.h>

#include <numeric>

using namespace NCloud::NBlockStore;

////////////////////////////////////////////////////////////////////////////////

constexpr ui64 MaxMirrorReplicas = 3;
constexpr ui64 PAGE_SIZE = 4096;

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
            .Handler1T<TString>([this] (const auto& s) {
                TStringBuf buf(s);
                TStringBuf arg;
                while (buf.NextTok(',', arg)) {
                    Ranges.push_back(FromString<ui32>(arg));
                }
            });

        opts.AddLongOption(
            "blocks",
            "specify individual blocks NUM or ranges NUM-NUM for validation, separated by comma")
            .RequiredArgument("STR")
            .Handler1T<TString>([this] (const auto& s) {
                TStringBuf buf(s);
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

TBlockData ReadBlockData(TFile& file, ui64 offset)
{
    // using O_DIRECT imposes some alignment restrictions:
    //   - offset should be sector aligned
    //   - buffer should be page aligned
    //   - size should be a multiple of a page size
    size_t len = (sizeof(TBlockData) + PAGE_SIZE - 1) / PAGE_SIZE * PAGE_SIZE;
    alignas(PAGE_SIZE) char buf[len];
    file.Seek(offset, sSet);
    file.Read(buf, len);
    return *reinterpret_cast<TBlockData*>(buf);
}

void ValidateRanges(TOptions options)
{
    TFile file(options.DevicePath, EOpenModeFlag::RdOnly | EOpenModeFlag::DirectAligned);

    const auto& configHolder = CreateTestConfig(options.ConfigPath);
    for (ui32 rangeIdx: options.Ranges) {
        Cout << "Start validation of " << rangeIdx << " range" << Endl;

        const auto& config = configHolder->GetConfig().GetRanges()[rangeIdx];

        ui64 len = config.GetRequestCount();
        ui64 startOffset = config.GetStartOffset();
        ui64 requestSize = config.GetRequestBlockCount() * 4_KB;

        TVector<TBlockData> actual(len);
        for (ui64 i = 0; i < len; ++i) {
            actual[i] = ReadBlockData(file, (i + startOffset) * requestSize);
        }

        Cout << "Guessing NumberToWrite, LastBlockIdx, Step from actual data"
             << Endl;

        const auto maxIt = MaxElementBy(
            actual,
            [](const auto& d) { return d.RequestNumber; });

        auto secondMaxIt = actual.begin();
        for (auto it = actual.begin() + 1; it < actual.end(); ++it) {
            if (it != maxIt &&
                it->RequestNumber > secondMaxIt->RequestNumber
            ) {
                secondMaxIt = it;
            }
        }

        ui64 step = 0;
        if (maxIt >= secondMaxIt) {
            step = maxIt - secondMaxIt;
        } else {
            step = len - (secondMaxIt - maxIt);
        }

        ui64 curNum = maxIt->RequestNumber;
        ui64 curBlockIdx = maxIt - actual.begin();

        Cout << "Step: " << step
             << " NumberToWrite: " << curNum
             << " LastBlockIdx: " << curBlockIdx << Endl;

        Y_ENSURE(step != 0, "Step should not be zero");
        Y_ENSURE(
            std::gcd(step, len) == 1,
            "Step and RequestCount should be coprime");

        TVector<ui64> expected(len);
        ui64 cnt = 0;
        while (cnt < len && curNum != 0) {
            curBlockIdx = (curBlockIdx + len - step) % len;
            expected[curBlockIdx] = --curNum;
            ++cnt;
        }

        TString path = options.LogPath + "range_" + ToString(rangeIdx) + ".log";
        TFileOutput out(TFile(path, EOpenModeFlag::CreateAlways | EOpenModeFlag::WrOnly));

        for (ui64 i = 0; i < len; ++i) {
            if (expected[i] != actual[i].RequestNumber) {
                out << "[" << rangeIdx << "]"
                    << " Wrong data in block " << (i + startOffset)
                    << " expected " << expected[i]
                    << " actual " << actual[i]
                    << Endl;
            }
        }

        Cout << "Finish validation of " << rangeIdx << " range" << Endl;
        Cout << "Log written to " << path.Quote() << Endl;
    }
}

void ValidateBlocks(TOptions options)
{
    TFile file(options.DevicePath, EOpenModeFlag::RdOnly | EOpenModeFlag::DirectAligned);

    for (ui64 blockIndex: options.Blocks) {
        std::set<TBlockData> blocks;

        for (ui64 i = 0; i < MaxMirrorReplicas; i++) {
            auto blockData = ReadBlockData(file, blockIndex * options.BlockSize);
            blocks.emplace(blockData);
        }

        if (blocks.size() == 1) {
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

    if (options.Mode == EValidationMode::Checksum) {
        ValidateRanges(std::move(options));
    } else {
        ValidateBlocks(std::move(options));
    }

    return 0;
}
