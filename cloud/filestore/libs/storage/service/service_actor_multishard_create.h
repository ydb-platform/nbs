#pragma once

#include <cloud/filestore/libs/storage/core/public.h>
#include <cloud/filestore/private/api/protos/tablet.pb.h>
#include <cloud/filestore/public/api/protos/fs.pb.h>

#include <cloud/storage/core/libs/actors/public.h>
#include <cloud/storage/core/libs/common/error.h>

#include <contrib/ydb/core/protos/filestore_config.pb.h>
#include <contrib/ydb/library/actors/core/actorid.h>

#include <util/generic/string.h>
#include <util/generic/vector.h>

#include <optional>

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

struct TMultiShardCreateConfig
{
    struct TShardConfig
    {
        NKikimrFileStore::TConfig Config;
        NProtoPrivate::TShardTopology::EShardType Type = {};
        ui32 RoleMask = 0;
        std::optional<NProto::EFastShardStorageKind> FastShardStorageKind;
    };

    TString FileSystemId;
    ui32 ExistingShardCount = 0;
    TVector<TShardConfig> ShardsToCreate;

    std::optional<NProtoPrivate::TFileSystemShardCreationState> State;
};

auto InitShardCreationState(
    const NProtoPrivate::TFileSystemShardCreationState& saved,
    const TMultiShardCreateConfig& config)
    -> TResultOrError<NProtoPrivate::TFileSystemShardCreationState>;

// Creates the shards and allocates the devices of the fast ones.
// Returns the topology of the created shards.
NActors::IActorPtr CreateMultiShardCreateActor(
    TStorageConfigPtr storageConfig,
    NActors::TActorId owner,
    TMultiShardCreateConfig config);

}   // namespace NCloud::NFileStore::NStorage
