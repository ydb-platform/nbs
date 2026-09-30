/*******************************************************************************

Process-wide read access to the current Blockstore configuration. Bootstrap
must initialize the provider in one thread before starting any readers. Each
call returns a snapshot that remains valid but may stop being current after a
later publication.

*******************************************************************************/

#pragma once

#include "blockstore_config.h"

#include <memory>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

// Read-only access to current Blockstore configuration snapshots. Keep an
// owning provider pointer obtained from bootstrap or ConfigsManager and retain
// one snapshot per logical operation. The implementation owns the publication
// point; previously returned snapshots remain valid after later publications.
struct IBlockstoreConfigProvider
{
    virtual ~IBlockstoreConfigProvider() = default;

    // Return an owning, non-null snapshot of the latest publication atomically.
    [[nodiscard]] virtual IBlockstoreConfigConstPtr Get() const = 0;
};

using IBlockstoreConfigProviderPtr = std::shared_ptr<IBlockstoreConfigProvider>;

////////////////////////////////////////////////////////////////////////////////

// Return the Blockstore configuration published at the time of the call.
[[nodiscard]] IBlockstoreConfigConstPtr GetCurrentBlockstoreConfig();

}   // namespace NCloud::NBlockStore
