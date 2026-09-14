/*******************************************************************************

The BlockStore dynamic configuration actor API.
The actor receives private_database_config through ConfigsDispatcher, applies
it to the local configuration saved before CMS, and publishes the result before
acknowledging the notification. Rejected updates leave the published
configuration unchanged.

*******************************************************************************/

#pragma once

#include "public.h"

#include <cloud/blockstore/libs/config/blockstore_config.h>
#include <cloud/blockstore/libs/config/blockstore_config_holder.h>

#include <contrib/ydb/core/config/init/init.h>
#include <contrib/ydb/library/actors/core/actor.h>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

// The result of the latest ConfigsManager update attempt exposed in monitoring.
enum class EConfigsManagerUpdateStatus : ui32
{
    Startup = 0,
    Applied = 1,
    Rejected = 2,
};

// Inputs for ConfigsManager. Pass them to CreateConfigsManager() after the
// startup configuration has been published in BlockstoreConfigHolder.
struct TConfigsManagerArgs
{
    // The non-null holder where accepted configurations are published.
    TBlockstoreConfigHolderPtr BlockstoreConfigHolder;

    // Local configuration saved before CMS, including CLI overrides. Every
    // accepted private config is applied to these unchanged values.
    NProto::TBlockstoreConfig StaticConfig;

    // Parsed private_database_config applied at startup, if present.
    NProto::TBlockstoreConfig InitialDynamicConfig;

    // The presence flag for InitialDynamicConfig, including valid empty input.
    bool InitialDynamicConfigPresent = false;

    // The non-null ICB controls reused by every storage adapter.
    NStorage::TStorageConfigControlsPtr StorageConfigControls;

    // The dispatcher override for tests; zero selects the node-local service.
    NActors::TActorId ConfigsDispatcherId;
};

// Create a parser that returns TBlockstoreConfig on success or
// NCloud::NProto::TError with a safe description on invalid private YAML.
NKikimr::NConfig::TOpaqueConfigParser CreateBlockstoreOpaqueConfigParser();

// Remove DynamicYamlConfigurationEnabled from private config to preserve
// the configuration mode selected at startup.
void RemoveStaticOnlyBlockstoreFields(NProto::TBlockstoreConfig* config);

// Create the single-writer dynamic configuration actor.
NActors::IActor* CreateConfigsManager(TConfigsManagerArgs args);

}   // namespace NCloud::NBlockStore
