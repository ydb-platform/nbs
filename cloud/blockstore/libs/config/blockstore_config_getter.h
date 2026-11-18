/*******************************************************************************

Read-only access to published Blockstore configuration snapshots.

*******************************************************************************/

#pragma once

#include "blockstore_config.h"

#include <memory>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

// Read-only access to current Blockstore configuration snapshots.
//
// A snapshot is a configuration object corresponding to one publication.
// The implementation owns the publication point.
//
// Call Get() once per logical operation and use the returned configuration
// throughout that operation. Later Get() calls may return a newer one.
// Previously returned snapshots remain valid after later publications.
struct IBlockstoreConfigGetter
{
    virtual ~IBlockstoreConfigGetter() = default;

    // Return an owning, non-null snapshot of the latest publication atomically.
    [[nodiscard]] virtual IBlockstoreConfigPtr Get() const = 0;
};

using IBlockstoreConfigGetterPtr = std::shared_ptr<IBlockstoreConfigGetter>;

}   // namespace NCloud::NBlockStore
