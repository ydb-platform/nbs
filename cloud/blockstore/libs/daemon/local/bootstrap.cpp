#include "bootstrap.h"

#include "config_initializer.h"
#include "options.h"

#include <cloud/storage/core/libs/diagnostics/trace_serializer.h>
#include <cloud/blockstore/libs/sharding/iface/sharding.h>

namespace NCloud::NBlockStore::NServer {

////////////////////////////////////////////////////////////////////////////////

TBootstrapLocal::TBootstrapLocal(IDeviceHandlerFactoryPtr deviceHandlerFactory)
    : TBootstrapBase(std::move(deviceHandlerFactory))
{}

TBootstrapLocal::~TBootstrapLocal()
{}

TConfigInitializerCommonPtr TBootstrapLocal::InitConfigs(int argc, char** argv)
{
    auto options = std::make_shared<TOptionsLocal>();
    options->Parse(argc, argv);
    Configs = std::make_shared<TConfigInitializerLocal>(std::move(options));
    return Configs;
}

void TBootstrapLocal::InitSpdk()
{
    // do nothing
}

void TBootstrapLocal::InitRdmaClient()
{
    // do nothing
}

void TBootstrapLocal::InitRdmaServer()
{
    // do nothing
}

void TBootstrapLocal::InitKikimrService()
{
    Y_ABORT("Not implemented");
}

void TBootstrapLocal::InitAuthService()
{
    // do nothing
}

void TBootstrapLocal::WarmupBSGroupConnections()
{
    // do nothing
}

void TBootstrapLocal::InitRdmaRequestServer()
{
    // do nothing
}

ITraceSerializerPtr TBootstrapLocal::GetTraceSerializer()
{
    return CreateTraceSerializerStub();
}

void TBootstrapLocal::SetupShardingManager()
{
    ShardingManager = NSharding::CreateShardingManagerStub();
}

TProgramShouldContinue& TBootstrapLocal::GetShouldContinue()
{
    return ShouldContinue;
}

}   // namespace NCloud::NBlockStore::NServer
