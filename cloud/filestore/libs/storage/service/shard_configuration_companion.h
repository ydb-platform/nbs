#pragma once

#include "shard_request_companion.h"

#include <cloud/filestore/private/api/protos/tablet.pb.h>

#include <cloud/storage/core/libs/common/error.h>

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

// Configures the shards of a filesystem and then its main tablet with the same
// flags, shard list and topology. The owner reports the responses.
class TShardConfigurationCompanion
{
private:
    NProtoPrivate::TConfigureShardsRequest MainFileStoreRequest;
    ui32 ExistingShardCount = 0;

    TShardRequestCompanion NewShardConfiguration;
    TShardRequestCompanion ExistingShardConfiguration;

public:
    explicit TShardConfigurationCompanion(ui32 maxRequestsInFlight);

    // Takes the request of the main filesystem, the shards get theirs from it.
    // The shards of the request starting at existingShardCount are new.
    void InitializeShardConfiguration(
        NProtoPrivate::TConfigureShardsRequest request,
        ui32 existingShardCount);

    // The new shards are configured before the existing ones.
    void StartShardConfiguration(const NActors::TActorContext& ctx);
    NProto::TError OnShardConfigured(
        const NActors::TActorContext& ctx,
        ui32 shardIndex);
    bool HasShardsToConfigure() const;

    void ConfigureMainFileStore(const NActors::TActorContext& ctx) const;

private:
    void ConfigureShard(const NActors::TActorContext& ctx, ui32 shardIndex);
};

}   // namespace NCloud::NFileStore::NStorage
