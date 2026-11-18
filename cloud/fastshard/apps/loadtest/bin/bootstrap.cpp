#include "bootstrap.h"

#include "options.h"

#include <cloud/fastshard/libs/bootstrap/core.h>
#include <cloud/fastshard/libs/sn/client/client.h>

namespace NCloud::NFastShard::NLoadTest {

////////////////////////////////////////////////////////////////////////////////

TBootstrap::TBootstrap(TOptionsPtr options)
    : Options(std::move(options))
{}

TBootstrap::~TBootstrap() = default;

void TBootstrap::Init()
{
    if (Options->Verbose) {
        EnableDebugLogging();
    }

    ClientMetrics = std::make_shared<TDurableClientMetrics>();

    Client = CreateDurableClient(
        CreateStorageNodeClient(Options->Host, Options->Port),
        {.RetryTimeout = Options->RetryTimeout},
        ClientMetrics);
}

void TBootstrap::Start()
{
    NFastShard::Init();
    RuntimeStarted = true;
}

void TBootstrap::Stop()
{
    Client.reset();

    if (RuntimeStarted) {
        Destroy();
        RuntimeStarted = false;
    }
}

}   // namespace NCloud::NFastShard::NLoadTest
