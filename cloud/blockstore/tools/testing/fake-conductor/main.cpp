#include <cloud/blockstore/libs/discovery/test_conductor.h>

#include <library/cpp/getopt/small/last_getopt.h>

#include <util/generic/string.h>
#include <util/generic/vector.h>

////////////////////////////////////////////////////////////////////////////////

int main(int argc, char** argv)
{
    using namespace NCloud::NBlockStore;

    ui16 port;
    TVector<TString> instances;

    {
        using namespace NLastGetopt;

        TOpts opts;
        opts.AddHelpOption();

        opts.AddLongOption("port", "http port")
            .Required()
            .RequiredArgument("PORT")
            .StoreResult(&port);

        opts.AddLongOption("instance", "group/instance")
            .AppendTo(&instances)
            .RequiredArgument("GROUP/INSTANCE");

        TOptsParseResultException(&opts, argc, argv);
    }

    NDiscovery::TFakeConductor conductor(port);
    TVector<NDiscovery::THostInfo> hostInfo;
    for (const auto& instance: instances) {
        TStringBuf group, host;
        TStringBuf(instance).Split('/', group, host);
        hostInfo.push_back({TString{group}, TString{host}});
    }
    conductor.SetHostInfo(std::move(hostInfo));
    conductor.Start();

    return 0;
}
