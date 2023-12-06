#include <cloud/filestore/libs/daemon/server/bootstrap.h>
#include <cloud/storage/core/libs/daemon/app.h>

#include <ydb/core/driver_lib/run/factories.h>
#include <ydb/core/security/ticket_parser.h>

////////////////////////////////////////////////////////////////////////////////

int main(int argc, char** argv)
{
    using namespace NCloud::NFileStore;

    auto moduleFactories = std::make_shared<NKikimr::TModuleFactories>();
    moduleFactories->CreateTicketParser = NKikimr::CreateTicketParser;

    NDaemon::TBootstrapServer bootstrap(std::move(moduleFactories));
    return NCloud::DoMain(bootstrap, argc, argv);
}
