#pragma once

#include <cloud/filestore/libs/storage/core/public.h>
#include <cloud/filestore/private/api/protos/tablet.pb.h>

#include <cloud/storage/core/libs/actors/public.h>

#include <contrib/ydb/library/actors/core/actorid.h>

#include <util/generic/string.h>

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

struct TMultiShardConfigureConfig
{
    TString FileSystemId;
    NProtoPrivate::TShardTopology Topology;

    ui32 ExistingShardCount = 0;
    bool DirectoryCreationInShardsEnabled = false;
    bool ForceDirectoryCreationInShards = false;
    bool StrictFileSystemSizeEnforcementEnabled = false;
};

// Configures the shards, then the filesystem. Replies
// TEvServicePrivate::TEvMultiShardConfigureResponse.
NActors::IActorPtr CreateMultiShardConfigureActor(
    TStorageConfigPtr storageConfig,
    NActors::TActorId owner,
    TMultiShardConfigureConfig config);

}   // namespace NCloud::NFileStore::NStorage
