#include "blockstore_config_provider.h"

#include "blockstore_config_provider_private.h"

#include <util/system/yassert.h>

#include <memory>
#include <utility>

namespace NCloud::NBlockStore {

namespace {

////////////////////////////////////////////////////////////////////////////////

TBlockstoreConfigHolderPtr& BlockstoreConfigHolder()
{
    static TBlockstoreConfigHolderPtr holder;
    return holder;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IBlockstoreConfigConstPtr GetCurrentBlockstoreConfig()
{
    const auto& holder = BlockstoreConfigHolder();
    Y_ABORT_UNLESS(
        holder,
        "Blockstore configuration provider is not initialized");
    return holder->Get();
}

TBlockstoreConfigHolderPtr InitializeBlockstoreConfigProvider(
    IBlockstoreConfigPtr initialConfig)
{
    auto& holder = BlockstoreConfigHolder();
    Y_ABORT_UNLESS(
        !holder,
        "Blockstore configuration provider cannot be reset or rebound");
    Y_ABORT_UNLESS(
        initialConfig,
        "Initial Blockstore configuration must not be null");

    holder = std::make_shared<TBlockstoreConfigHolder>(std::move(initialConfig));
    return holder;
}

}   // namespace NCloud::NBlockStore
