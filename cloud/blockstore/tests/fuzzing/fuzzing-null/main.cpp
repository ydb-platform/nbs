#include <cloud/blockstore/libs/daemon/local/bootstrap.h>
#include <cloud/blockstore/libs/service/device_handler.h>
#include <cloud/blockstore/tests/fuzzing/common/starter.h>

////////////////////////////////////////////////////////////////////////////////

extern "C"
int LLVMFuzzerTestOneInput(const ui8* data, size_t size)
{
    using namespace NCloud::NBlockStore;

    NServer::TBootstrapLocal bootstrap(CreateDefaultDeviceHandlerFactory());

    TVector<TString> options {
        "blockstore-server",
        "--service", "null",
        "--server-file", "./nbs-server.txt"
    };

    auto* starter = NFuzzing::TStarter::GetStarter(
        &bootstrap,
        std::move(options)
    );
    return starter ? starter->Run(data, size) : 0;
}
