/*******************************************************************************

Read-only access to published Blockstore configuration snapshots. A snapshot
is a configuration object corresponding to one publication. Get() returns the
latest snapshot from the supplied source; later calls may return a newer one.

*******************************************************************************/

#pragma once

#include "blockstore_config.h"

#include <memory>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

// Read-only access to current Blockstore configuration snapshots. Keep an
// owning getter pointer and retain one snapshot per logical operation.
// The implementation owns the publication point; previously returned
// snapshots remain valid after later publications.
struct IBlockstoreConfigGetter
{
    virtual ~IBlockstoreConfigGetter() = default;

    // Return an owning, non-null snapshot of the latest publication atomically.
    [[nodiscard]] virtual IBlockstoreConfigConstPtr Get() const = 0;
};

using IBlockstoreConfigGetterPtr = std::shared_ptr<IBlockstoreConfigGetter>;

}   // namespace NCloud::NBlockStore
