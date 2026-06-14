#include <cloud/filestore/libs/daemon/vhost/bootstrap.h>
#include <cloud/filestore/libs/vfs_fuse/loop.h>

#include <cloud/storage/core/libs/daemon/app.h>

#include <contrib/ydb/core/driver_lib/run/factories.h>
#include <contrib/ydb/core/security/ticket_parser.h>

////////////////////////////////////////////////////////////////////////////////

int main(int argc, char** argv)
{
    using namespace NCloud::NFileStore;

    auto moduleFactories = std::make_shared<NKikimr::TModuleFactories>();
    moduleFactories->CreateTicketParser = NKikimr::CreateTicketParser;
    moduleFactories->SchemeOperationFactory.reset(
        NKikimr::NSchemeShard::DefaultOperationFactory());

    auto vhostFactories = std::make_shared<NDaemon::TVhostModuleFactories>();
    vhostFactories->LoopFactory = NFuse::CreateFuseLoopFactory;

    NDaemon::TBootstrapVhost bootstrap(std::move(moduleFactories), std::move(vhostFactories));
    return NCloud::DoMain(bootstrap, argc, argv);
}
