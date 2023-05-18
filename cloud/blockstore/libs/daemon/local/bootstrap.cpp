#include "bootstrap.h"

#include "config_initializer.h"
#include "options.h"

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

void TBootstrapLocal::InitKikimrService()
{
    Y_FAIL("Not implemented");
}

void TBootstrapLocal::InitAuthService()
{
    // do nothing
}

TProgramShouldContinue& TBootstrapLocal::GetShouldContinue()
{
    return ShouldContinue;
}

}   // namespace NCloud::NBlockStore::NServer
