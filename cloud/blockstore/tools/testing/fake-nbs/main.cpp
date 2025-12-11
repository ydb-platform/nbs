#include <cloud/blockstore/libs/discovery/test_server.h>

#include <library/cpp/getopt/small/last_getopt.h>

////////////////////////////////////////////////////////////////////////////////

int main(int argc, char** argv)
{
    using namespace NCloud::NBlockStore;

    ui16 port;
    ui16 securePort;
    TString rootCertsFile;
    TString certFile;
    TString keyFile;

    {
        using namespace NLastGetopt;

        TOpts opts;
        opts.AddHelpOption();

        opts.AddLongOption("port", "grpc port")
            .Required()
            .RequiredArgument("PORT")
            .StoreResult(&port);

        opts.AddLongOption("secure-port", "secure grpc port")
            .RequiredArgument("PORT")
            .DefaultValue(0)
            .StoreResult(&securePort);

        opts.AddLongOption("root-certs-file", "root certificates file")
            .RequiredArgument("PATH")
            .StoreResult(&rootCertsFile);

        opts.AddLongOption("private-key-file", "private key file")
            .RequiredArgument("PATH")
            .StoreResult(&keyFile);

        opts.AddLongOption("cert-file", "certificate file")
            .RequiredArgument("PATH")
            .StoreResult(&certFile);

        TOptsParseResultException(&opts, argc, argv);
    }

    NDiscovery::TFakeBlockStoreServer
        server(port, securePort, rootCertsFile, keyFile, certFile);

    server.Start();

    return 0;
}
