#include <cloud/blockstore/libs/diagnostics/block_digest.h>

#include <library/cpp/getopt/small/last_getopt.h>

#include <util/generic/size_literals.h>
#include <util/stream/file.h>

namespace {

using namespace NCloud;
using namespace NBlockStore;
using namespace NLastGetopt;

////////////////////////////////////////////////////////////////////////////////

struct TOptions
{
    TString DataPath;
    ui32 BlockSize = 0;

    void Parse(int argc, char** argv)
    {
        TOpts opts;
        opts.AddHelpOption();

        opts.AddLongOption("data-path", "path to the file with data")
            .RequiredArgument()
            .Required()
            .StoreResult(&DataPath);

        opts.AddLongOption("block-size", "split data into blocks of this size")
            .RequiredArgument()
            .DefaultValue(4_KB)
            .StoreResult(&BlockSize);

        TOptsParseResultException res(&opts, argc, argv);
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

int main(int argc, char** argv)
{
    TOptions opts;
    opts.Parse(argc, argv);

    auto data = TIFStream(opts.DataPath).ReadAll();
    if (data.size() % opts.BlockSize != 0) {
        Cerr << "data size is not divisible by block size" << Endl;
        return 1;
    }

    ui32 i = 0;
    while (i < data.size()) {
        auto csum = ComputeDefaultDigest(TBlockDataRef(
            data.begin() + i,
            opts.BlockSize));

        Cout << i << "\t" << csum << Endl;

        i += opts.BlockSize;
    }

    return 0;
}
