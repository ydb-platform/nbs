/*******************************************************************************

Process-wide read access to the current Blockstore configuration. Bootstrap
must initialize the provider in one thread before starting any readers. Each
call returns a snapshot that remains valid but may stop being current after a
later publication.

*******************************************************************************/

#pragma once

#include "blockstore_config.h"

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

// Return the Blockstore configuration published at the time of the call.
[[nodiscard]] IBlockstoreConfigConstPtr GetCurrentBlockstoreConfig();

}   // namespace NCloud::NBlockStore
