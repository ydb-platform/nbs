#include <cloud/blockstore/libs/daemon/local/bootstrap.h>
#include <cloud/blockstore/libs/service/device_handler.h>

#include <cloud/storage/core/libs/daemon/app.h>

////////////////////////////////////////////////////////////////////////////////
void dummy_use(void* ptr)
{
    asm volatile("" : : "r"(ptr) : "memory");   // Prevent optimization
}
int main(int argc, char** argv)
{
    void* leak = malloc(100);
    int* leak2 = new int[50];
    dummy_use(leak);
    dummy_use(leak2);

    using namespace NCloud::NBlockStore;

    NServer::TBootstrapLocal bootstrap(CreateDefaultDeviceHandlerFactory());
    return NCloud::DoMain(bootstrap, argc, argv);
}
