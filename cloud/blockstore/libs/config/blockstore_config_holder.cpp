#include "blockstore_config_holder.h"

#include <util/system/yassert.h>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

TBlockstoreConfigHolder::TBlockstoreConfigHolder(
    IBlockstoreConfigPtr initialConfig)
    : Current(initialConfig)
{
    Y_ABORT_UNLESS(initialConfig);
}

IBlockstoreConfigConstPtr TBlockstoreConfigHolder::Get() const
{
    auto config = Current.AtomicLoad();
    Y_ABORT_UNLESS(config);
    return config;
}

void TBlockstoreConfigHolder::Set(IBlockstoreConfigPtr config)
{
    Y_ABORT_UNLESS(config);
    Current.AtomicStore(config);
}

}   // namespace NCloud::NBlockStore
