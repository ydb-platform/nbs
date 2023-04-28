#include <cloud/blockstore/libs/disk_agent/bootstrap.h>

#include <cloud/storage/core/libs/daemon/app.h>

#include <ydb/core/driver_lib/run/factories.h>
#include <ydb/core/security/ticket_parser.h>

#include <util/generic/yexception.h>

////////////////////////////////////////////////////////////////////////////////

int main(int argc, char** argv)
{
    using namespace NCloud::NBlockStore;

    auto moduleFactories = std::make_shared<NKikimr::TModuleFactories>();
    moduleFactories->CreateTicketParser = NKikimr::CreateTicketParser;

    NServer::TBootstrap bootstrap(std::move(moduleFactories));
    return NCloud::DoMain(bootstrap, argc, argv);
}
