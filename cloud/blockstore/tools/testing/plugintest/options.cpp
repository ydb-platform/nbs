#include "options.h"

#include <library/cpp/getopt/small/last_getopt.h>

namespace NCloud::NBlockStore {

using namespace NLastGetopt;

////////////////////////////////////////////////////////////////////////////////

void TOptions::Parse(int argc, char** argv)
{
    TOpts opts;
    opts.AddHelpOption();

    opts.AddLongOption("plugin-options", "plugin options")
        .Required()
        .RequiredArgument("STR")
        .StoreResult(&PluginOptions);

    opts.AddLongOption("plugin-lib", "plugin lib path")
        .Required()
        .RequiredArgument("FILE")
        .StoreResult(&PluginLibPath);

    opts.AddLongOption("test-config", "test configuration")
        .Required()
        .RequiredArgument("FILE")
        .StoreResult(&TestConfig);

    opts.AddLongOption("host-major", "host major version")
        .RequiredArgument("INT")
        .StoreResult(&HostMajor);

    opts.AddLongOption("host-minor", "host minor version")
        .RequiredArgument("INT")
        .StoreResult(&HostMinor);

    opts.AddLongOption("endpoint-folder", "path to endpoint folder")
        .RequiredArgument("DIR")
        .StoreResult(&EndpointFolder);

    opts.AddLongOption("run-count", "number of test runs")
        .RequiredArgument("INT")
        .StoreResult(&RunCount);

    TOptsParseResultException(&opts, argc, argv);
}

}   // namespace NCloud::NBlockStore
