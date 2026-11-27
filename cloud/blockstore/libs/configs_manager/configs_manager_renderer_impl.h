/*******************************************************************************

Effective configuration assembly for the ConfigsManager monitoring page.
Internal helpers shared by the renderer and its unit tests.

*******************************************************************************/

#pragma once

#include <cloud/blockstore/config/blockstore.pb.h>
#include <cloud/blockstore/libs/config/blockstore_config.h>

namespace NCloud::NBlockStore::NPrivate {

// Build every configuration section for the Effective column, including live
// ICB overrides from the StorageConfig effective proto.
NProto::TBlockstoreConfig BuildEffectiveConfig(
    const IBlockstoreConfig& currentConfig);

}   // namespace NCloud::NBlockStore::NPrivate
