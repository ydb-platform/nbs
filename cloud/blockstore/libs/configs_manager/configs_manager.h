/*******************************************************************************

The Blockstore dynamic configuration actor API.
The actor receives PrivateDatabaseConfig through ConfigsDispatcher, applies
it to the static configuration saved before CMS, and publishes the result.
Rejected updates leave the published configuration unchanged.
Created via CreateConfigsManager().

*******************************************************************************/

#pragma once

#include "public.h"

#include <cloud/blockstore/libs/config/blockstore_config.h>
#include <cloud/blockstore/libs/config/blockstore_config_holder.h>

#include <contrib/ydb/library/actors/core/actor.h>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

// ConfigsManager arguments. Pass them to CreateConfigsManager() after the
// startup configuration has been published in ConfigHolder.
struct TConfigsManagerArgs
{
    // The non-null holder where accepted configurations are published.
    TBlockstoreConfigHolderPtr ConfigHolder;

    // Static configuration saved before CMS, including CLI overrides. Every
    // accepted PrivateDatabaseConfig is applied to these unchanged values.
    NProto::TBlockstoreConfig StaticConfig;

    // Parsed PrivateDatabaseConfig applied at startup; empty without overrides.
    NProto::TBlockstoreConfig InitialDynamicConfig;

    // The non-null ICB controls reused by every storage adapter.
    NStorage::TStorageConfigControlsPtr StorageConfigControls;

    // The dispatcher override; zero selects the node-local service.
    // Required for tests
    NActors::TActorId ConfigsDispatcherId;
};

// Create the single-writer dynamic configuration actor.
NActors::IActor* CreateConfigsManager(TConfigsManagerArgs args);

}   // namespace NCloud::NBlockStore
