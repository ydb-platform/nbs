/*******************************************************************************

Bootstrap-only initialization for the process-wide Blockstore configuration
provider. Initialize it exactly once in one thread before starting any readers,
then pass the returned holder to the single runtime writer. Reset it only after
all readers and the writer have stopped. Rebinding while initialized is
forbidden.

*******************************************************************************/

#pragma once

#include "blockstore_config_holder.h"

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

// Initialize the provider with a non-null configuration and return its writer
// handle. Abort if the provider has already been initialized.
[[nodiscard]] TBlockstoreConfigHolderPtr InitializeBlockstoreConfigProvider(
    IBlockstoreConfigPtr initialConfig);

// Reset the provider during test teardown.
// Do nothing if the provider is not initialized.
void ResetBlockstoreConfigProvider();

}   // namespace NCloud::NBlockStore
