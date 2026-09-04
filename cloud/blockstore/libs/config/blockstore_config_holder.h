/*******************************************************************************

The atomic publication point for Blockstore configuration snapshots. Construct
the holder with a non-null initial configuration. A single writer publishes
replacements; readers load once per logical operation and retain the result.

*******************************************************************************/

#pragma once

#include "blockstore_config.h"

#include <library/cpp/threading/hot_swap/hot_swap.h>

#include <memory>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

// The owner of the current Blockstore configuration publication point.
class TBlockstoreConfigHolder final
{
public:
    explicit TBlockstoreConfigHolder(IBlockstoreConfigPtr initialConfig);

    TBlockstoreConfigHolder(const TBlockstoreConfigHolder&) = delete;
    TBlockstoreConfigHolder& operator=(const TBlockstoreConfigHolder&) = delete;

    [[nodiscard]] IBlockstoreConfigConstPtr Get() const;

    // Publish a non-null configuration atomically. Only one writer may call
    // Set().
    void Set(IBlockstoreConfigPtr config);

private:
    // The non-null configuration currently visible to new readers.
    THotSwap<IBlockstoreConfig> Current;
};

using TBlockstoreConfigHolderPtr = std::shared_ptr<TBlockstoreConfigHolder>;

}   // namespace NCloud::NBlockStore
