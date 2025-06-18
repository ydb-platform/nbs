#include <cloud/blockstore/libs/daemon/local/bootstrap.h>
#include <cloud/blockstore/libs/service/device_handler.h>

#include <cloud/storage/core/libs/daemon/app.h>

////////////////////////////////////////////////////////////////////////////////

int main(int argc, char** argv)
{
    using namespace NCloud::NBlockStore;

    NServer::TBootstrapLocal bootstrap(CreateDefaultDeviceHandlerFactory());
    return NCloud::DoMain(bootstrap, argc, argv);
}
