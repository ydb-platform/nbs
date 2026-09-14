/*******************************************************************************

Helpers for manipulating Blockstore configuration.

*******************************************************************************/

#pragma once

#include <cloud/blockstore/config/blockstore.pb.h>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

// Remove static-only fields from NProto::TBlockstoreConfig.
void RemoveStaticOnlyBlockstoreFields(NProto::TBlockstoreConfig& config);

}   // namespace NCloud::NBlockStore
