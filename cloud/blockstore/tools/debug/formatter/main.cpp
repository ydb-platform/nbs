#include <cloud/storage/core/libs/common/error.h>

#include <library/cpp/getopt/small/last_getopt.h>

namespace {

using namespace NCloud;
using namespace NLastGetopt;

////////////////////////////////////////////////////////////////////////////////

struct TOptions
{
    i64 ResultCode;
    ui32 Flags;
    TString ResultCodeStr;

    void Parse(int argc, char** argv)
    {
        TOpts opts;
        opts.AddHelpOption();

        opts.AddLongOption("result-code", "result code")
            .RequiredArgument()
            .DefaultValue(-1)
            .StoreResult(&ResultCode);

        opts.AddLongOption("flags", "error flags")
            .RequiredArgument()
            .DefaultValue(0)
            .StoreResult(&Flags);

        opts.AddLongOption("result-code-str", "well known code string")
            .RequiredArgument()
            .StoreResult(&ResultCodeStr);

        TOptsParseResultException res(&opts, argc, argv);
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

int main(int argc, char** argv)
{
    TOptions opts;
    opts.Parse(argc, argv);

    if (opts.ResultCode >= 0) {
        Cout << FormatError(MakeError(opts.ResultCode, "", opts.Flags)) << Endl;
    } else if (opts.ResultCodeStr) {
        EWellKnownResultCodes code = FromString(opts.ResultCodeStr);
        Cout << static_cast<ui32>(code) << Endl;
    } else {
        Cerr << "bad parameter combination" << Endl;
    }

    return 0;
}
