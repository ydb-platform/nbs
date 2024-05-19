#include "options.h"

namespace NCloud::NBlockStore::NServer {

using namespace NLastGetopt;

////////////////////////////////////////////////////////////////////////////////

TOptions::TOptions()
{
    Opts.AddLongOption("root-certs-file")
        .RequiredArgument("FILE")
        .StoreResult(&RootCertsFile);

    Opts.AddLongOption("key-file")
        .RequiredArgument("FILE")
        .StoreResult(&KeyFile);

    Opts.AddLongOption("cert-file")
        .RequiredArgument("FILE")
        .StoreResult(&CertFile);
}

void TOptions::Parse(int argc, char** argv)
{
    auto res = std::make_unique<TOptsParseResultException>(&Opts, argc, argv);
    if (res->FindLongOptParseResult("verbose") != nullptr && !VerboseLevel) {
        VerboseLevel = "debug";
    }
}

}   // namespace NCloud::NBlockStore::NServer
