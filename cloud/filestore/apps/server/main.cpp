#include <cloud/filestore/libs/daemon/server/bootstrap.h>
#include <cloud/storage/core/libs/daemon/app.h>

#include <contrib/ydb/core/driver_lib/run/factories.h>
#include <contrib/ydb/core/security/ticket_parser.h>

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
    using namespace NCloud::NFileStore;

    auto moduleFactories = std::make_shared<NKikimr::TModuleFactories>();
    moduleFactories->CreateTicketParser = NKikimr::CreateTicketParser;

    NDaemon::TBootstrapServer bootstrap(std::move(moduleFactories));
    return NCloud::DoMain(bootstrap, argc, argv);
}
