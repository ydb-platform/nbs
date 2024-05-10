#include <cloud/blockstore/libs/endpoint_proxy/bootstrap.h>

#include <cloud/storage/core/libs/daemon/app.h>

////////////////////////////////////////////////////////////////////////////////

int main(int argc, char** argv)
{
    using namespace NCloud::NBlockStore;

    NServer::TBootstrap bootstrap;
    return NCloud::DoMain(bootstrap, argc, argv);
}
