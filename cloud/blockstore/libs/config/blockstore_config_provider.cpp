#include "blockstore_config_provider.h"

#include "blockstore_config_provider_private.h"

#include <util/system/yassert.h>

#include <memory>
#include <utility>

namespace NCloud::NBlockStore {

namespace {

////////////////////////////////////////////////////////////////////////////////

IBlockstoreConfigProviderPtr& BlockstoreConfigProvider()
{
    static IBlockstoreConfigProviderPtr provider;
    return provider;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IBlockstoreConfigConstPtr GetCurrentBlockstoreConfig()
{
    const auto& provider = BlockstoreConfigProvider();
    Y_ABORT_UNLESS(
        provider,
        "Blockstore configuration provider is not initialized");
    return provider->Get();
}

TBlockstoreConfigHolderPtr InitializeBlockstoreConfigProvider(
    IBlockstoreConfigPtr initialConfig)
{
    auto& provider = BlockstoreConfigProvider();
    Y_ABORT_UNLESS(
        !provider,
        "Blockstore configuration provider cannot be reset or rebound");
    Y_ABORT_UNLESS(
        initialConfig,
        "Initial Blockstore configuration must not be null");

    auto holder =
        std::make_shared<TBlockstoreConfigHolder>(std::move(initialConfig));
    provider = holder;
    return holder;
}

void ResetBlockstoreConfigProvider()
{
    auto& provider = BlockstoreConfigProvider();
    provider.reset();
}

}   // namespace NCloud::NBlockStore
