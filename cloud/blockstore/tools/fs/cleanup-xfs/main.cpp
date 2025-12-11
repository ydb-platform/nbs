#include "cleanup.h"
#include "parser.h"

#include <cloud/storage/core/libs/common/format.h>

#include <library/cpp/getopt/small/last_getopt.h>

#include <util/system/file.h>
#include <util/system/shellcommand.h>

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TOptions
{
    TString FsPath;
    TString ToolPath = "./xfs_db";

    bool DryRun = false;
    bool Verbose = false;

    int Threads = 4;

    void Parse(int argc, char** argv)
    {
        using namespace NLastGetopt;

        TOpts opts;
        opts.AddHelpOption();

        opts.AddLongOption('d', "device-path")
            .Required()
            .RequiredArgument("PATH")
            .StoreResult(&FsPath);

        opts.AddLongOption("xfs_db-path")
            .DefaultValue(ToolPath)
            .RequiredArgument("PATH")
            .StoreResult(&ToolPath);

        opts.AddLongOption("threads")
            .RequiredArgument("NUM")
            .DefaultValue(Threads)
            .StoreResult(&Threads);

        opts.AddLongOption("dry-run").NoArgument().StoreTrue(&DryRun);

        opts.AddLongOption('v', "verbose").NoArgument().StoreTrue(&Verbose);

        TOptsParseResultException res(&opts, argc, argv);
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TDummyDev: IDev
{
    void Pwrite(const void* buffer, ui32 byteCount, i64 offset) const override
    {
        Y_UNUSED(buffer);
        Y_UNUSED(byteCount);
        Y_UNUSED(offset);
    }

    void Pload(void* buffer, ui32 byteCount, i64 offset) const override
    {
        Y_UNUSED(buffer);
        Y_UNUSED(byteCount);
        Y_UNUSED(offset);
    }
};

struct TDev: IDev
{
    TFile File;

    explicit TDev(const TString& path)
        : File(path, OpenExisting | RdWr | Sync | DirectAligned)
    {}

    void Pwrite(const void* buffer, ui32 byteCount, i64 offset) const override
    {
        File.Pwrite(buffer, byteCount, offset);
    }

    void Pload(void* buffer, ui32 byteCount, i64 offset) const override
    {
        File.Pload(buffer, byteCount, offset);
    }
};

////////////////////////////////////////////////////////////////////////////////

class TApp
{
private:
    const TOptions& Options;

public:
    explicit TApp(const TOptions& options)
        : Options(options)
    {}

    int Run()
    {
        auto sb = GetSuperBlock();

        if (Options.Verbose) {
            Cout << "Super block: { " << sb.BlockSize << ", " << sb.GroupCount
                 << ", " << sb.BlocksPerGroup << ", " << sb.SectorSize << " }"
                 << Endl;
        }

        auto freesp = GetFreeSpace();

        if (Options.Verbose) {
            Cout << "Free space:\n";
            for (auto& [x, y, z]: freesp) {
                Cout << "    " << x << " " << y << " " << z << "\n";
            }
            Cout << Endl;
        }

        auto dev = CreateDev();

        Cleanup(*dev, sb, freesp, Options.Threads, Options.Verbose);

        return 0;
    }

private:
    template <typename... Args>
    TString ExecDB(Args&&... args)
    {
        TShellCommand cmd(Options.ToolPath, {args..., Options.FsPath});

        auto output = cmd.Run().Wait().GetOutput();

        if (Options.Verbose) {
            Cout << "xfs_db:\n" << output << Endl;
        }

        auto ec = cmd.GetExitCode();
        Y_ENSURE(!ec.Empty());
        Y_ENSURE(ec == 0, "xfs_db: " << *ec);

        return output;
    }

    TSuperBlock GetSuperBlock()
    {
        auto output = ExecDB("-c", "sb 0", "-c", "p");

        TStringInput stream(output);

        return ParseSuperBlock(stream);
    }

    TVector<TFreeList> GetFreeSpace()
    {
        auto output = ExecDB("-c", "freesp -d -h1");

        TStringInput stream(output);

        return ParseFreeSpace(stream);
    }

    std::unique_ptr<IDev> CreateDev()
    {
        if (Options.DryRun) {
            return std::make_unique<TDummyDev>();
        }

        return std::make_unique<TDev>(Options.FsPath);
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

int main(int argc, char** argv)
{
    try {
        TOptions options;
        options.Parse(argc, argv);

        TApp app(options);

        return app.Run();

    } catch (...) {
        Cerr << CurrentExceptionMessage() << Endl;
        return 1;
    }
}
