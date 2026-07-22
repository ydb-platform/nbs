#include <library/cpp/blockcodecs/codecs.h>
#include <library/cpp/getopt/small/last_getopt.h>

#include <util/datetime/base.h>
#include <util/generic/buffer.h>
#include <util/generic/size_literals.h>
#include <util/generic/string.h>
#include <util/generic/strbuf.h>
#include <util/generic/yexception.h>
#include <util/stream/output.h>
#include <util/string/printf.h>
#include <util/system/file.h>
#include <util/system/hp_timer.h>

#include <algorithm>

namespace {

using namespace NBlockCodecs;
using namespace NLastGetopt;

////////////////////////////////////////////////////////////////////////////////

TString FormatSize(double bytes)
{
    static const char* units[] = {"B", "KiB", "MiB", "GiB", "TiB"};
    size_t u = 0;
    while (bytes >= 1024.0 && u + 1 < sizeof(units) / sizeof(units[0])) {
        bytes /= 1024.0;
        ++u;
    }
    return Sprintf("%.3f %s", bytes, units[u]);
}

TString GiBPerSec(TDuration total, double bytes)
{
    if (total == TDuration::Zero() || bytes == 0) {
        return "-";
    }
    double gib = bytes / 1_GB;
    return Sprintf("%.3f GiB/s", gib / total.SecondsFloat());
}

////////////////////////////////////////////////////////////////////////////////

struct TOptions
{
    TString Input;
    TString Codec;
    size_t Iterations = 1;
    size_t MaxBytes = 1_GB;
    bool ListCodecs = false;

    void Parse(int argc, char* argv[])
    {
        TOpts opts;
        opts.AddHelpOption();

        opts.AddLongOption('i', "input", "path to input file")
            .RequiredArgument("PATH")
            .StoreResult(&Input);

        opts.AddLongOption('c', "codec",
                "codec name (e.g. zstd_1, lz4, snappy)")
            .RequiredArgument("NAME")
            .StoreResult(&Codec);

        opts.AddLongOption('n', "iterations", "number of round-trips to run")
            .RequiredArgument("N")
            .DefaultValue(ToString(Iterations))
            .StoreResult(&Iterations);

        opts.AddLongOption('m', "max-bytes",
                "read at most this many bytes from the input")
            .RequiredArgument("N")
            .DefaultValue(ToString(MaxBytes))
            .StoreResult(&MaxBytes);

        opts.AddLongOption("list-codecs",
                "print all registered codecs and exit")
            .NoArgument()
            .SetFlag(&ListCodecs);

        TOptsParseResultException res(&opts, argc, argv);

        if (ListCodecs) {
            return;
        }
        Y_ENSURE(Input, "--input is required");
        Y_ENSURE(Codec, "--codec is required");
        Y_ENSURE(Iterations >= 1, "--iterations must be >= 1");
        Y_ENSURE(MaxBytes >= 1, "--max-bytes must be >= 1");
    }
};

////////////////////////////////////////////////////////////////////////////////

void ReadInput(const TString& path, size_t maxBytes, TBuffer& out)
{
    TFile file(path, OpenExisting | RdOnly);
    const i64 fileSize = file.GetLength();
    Y_ENSURE(fileSize >= 0, "GetLength failed on " << path);

    const size_t toRead =
        std::min(static_cast<size_t>(fileSize), maxBytes);
    out.Reserve(toRead);
    out.Resize(toRead);
    if (toRead == 0) {
        return;
    }
    file.Load(out.Data(), toRead);
}

////////////////////////////////////////////////////////////////////////////////

struct TPhaseResult
{
    TDuration Min = TDuration::Max();
    TDuration Max = TDuration::Zero();
    TDuration Total = TDuration::Zero();
    size_t Runs = 0;

    void Add(TDuration d)
    {
        Min = std::min(Min, d);
        Max = std::max(Max, d);
        Total += d;
        ++Runs;
    }

    [[nodiscard]] TDuration Avg() const
    {
        return Runs == 0 ? TDuration::Zero() : Total / Runs;
    }
};

////////////////////////////////////////////////////////////////////////////////

int PrintCodecList()
{
    for (const auto& name: NBlockCodecs::ListAllCodecs()) {
        Cout << name << Endl;
    }
    return 0;
}

////////////////////////////////////////////////////////////////////////////////

int Run(const TOptions& opts)
{
    const ICodec* codec = nullptr;
    try {
        codec = Codec(opts.Codec);
    } catch (const TNotFound&) {
        Cerr << "unknown codec: " << opts.Codec << Endl
             << "run with --list-codecs to see all registered codecs" << Endl;
        return 1;
    }

    Cerr << "reading " << opts.Input << " (up to "
         << FormatSize(opts.MaxBytes) << ") ..." << Endl;
    TBuffer inputBuf;
    ReadInput(opts.Input, opts.MaxBytes, inputBuf);
    const TStringBuf input(inputBuf.Data(), inputBuf.Size());
    const size_t originalSize = input.size();
    Cerr << "read " << FormatSize(originalSize) << " ("
         << originalSize << " bytes)" << Endl;

    //
    // Pre-size compress and decompress destination buffers so no
    // allocation happens inside the timed loop. TBuffer::Reserve/Resize
    // does no zero-init, unlike TString::ReserveAndResize.
    //

    const size_t maxCompressed = codec->MaxCompressedLength(input);
    TBuffer compressedBuf;
    compressedBuf.Resize(maxCompressed);

    TBuffer decompressedBuf;
    decompressedBuf.Resize(originalSize);

    TPhaseResult compressPhase;
    TPhaseResult decompressPhase;
    size_t compressedSize = 0;

    for (size_t i = 0; i < opts.Iterations; ++i) {
        //
        // Compress phase.
        //

        THPTimer timer;
        const size_t sz = codec->Compress(input, compressedBuf.Data());
        compressPhase.Add(TDuration::Seconds(timer.Passed()));

        if (i == 0) {
            compressedSize = sz;
        } else if (sz != compressedSize) {
            Cerr << "compressed size differs across iterations: "
                 << compressedSize << " vs " << sz << Endl;
        }

        //
        // Decompress phase.
        //

        const TStringBuf compressedView(compressedBuf.Data(), compressedSize);
        timer.Reset();
        const size_t dsz =
            codec->Decompress(compressedView, decompressedBuf.Data());
        decompressPhase.Add(TDuration::Seconds(timer.Passed()));

        if (dsz != originalSize) {
            Cerr << "decompressed size mismatch: expected " << originalSize
                 << ", got " << dsz << Endl;
            return 1;
        }

        //
        // Validation phase.
        //

        const TStringBuf out(decompressedBuf.Data(), dsz);
        if (out != input) {
            Cerr << "round-trip mismatch: decompressed data differs "
                    "from original" << Endl;
            return 1;
        }
    }

    //
    // Report.
    //

    const double orig = static_cast<double>(originalSize);
    const double comp = static_cast<double>(compressedSize);
    const double compressionRatio = compressedSize ? orig / comp : 0.0;

    Cout << "codec           : " << codec->Name() << Endl;
    Cout << "iterations      : " << opts.Iterations << Endl;
    Cout << "original size   : " << FormatSize(originalSize)
         << " (" << originalSize << " bytes)" << Endl;
    Cout << "compressed size : " << FormatSize(compressedSize)
         << " (" << compressedSize << " bytes)" << Endl;
    Cout << Sprintf("ratio (o/c)     : %.3fx", compressionRatio) << Endl;
    Cout << Endl;

    auto report = [](const char* label, const TPhaseResult& p,
                     double originalBytes, double compressedBytes)
    {
        Cout << label << Endl;
        Cout << "  runs               : " << p.Runs << Endl;
        Cout << "  total time         : " << p.Total << Endl;
        Cout << "  avg time           : " << p.Avg() << Endl;
        Cout << "  min time           : " << p.Min << Endl;
        Cout << "  max time           : " << p.Max << Endl;
        Cout << "  ms per GiB (comp)  : "
             << GiBPerSec(p.Avg(), compressedBytes) << Endl;
        Cout << "  throughput (orig)  : "
             << GiBPerSec(p.Avg(), originalBytes) << Endl;
        Cout << "  throughput (comp)  : "
             << GiBPerSec(p.Avg(), compressedBytes) << Endl;
    };

    report("compress:", compressPhase, originalSize, compressedSize);
    Cout << Endl;
    report("decompress:", decompressPhase, originalSize, compressedSize);

    return 0;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

int main(int argc, char* argv[])
{
    TOptions opts;
    try {
        opts.Parse(argc, argv);
    } catch (...) {
        Cerr << CurrentExceptionMessage() << Endl;
        return 1;
    }

    try {
        return opts.ListCodecs ? PrintCodecList() : Run(opts);
    } catch (...) {
        Cerr << "error: " << CurrentExceptionMessage() << Endl;
        return 1;
    }
}
