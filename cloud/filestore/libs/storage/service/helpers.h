#pragma once

#include "public.h"

namespace NKikimrFileStore {
    class TConfig;
}

namespace NCloud::NFileStore::NProto {
    class TFileStore;
    class TFileStorePerformanceProfile;
}

namespace NCloud::NFileStore::NProtoPrivate {
    class TGetFileSystemTopologyResponse;
    class TShardTopology;
}

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

void Convert(
    const NKikimrFileStore::TConfig& config,
    NProto::TFileStore& fileStore);

// Get topology or generate it from shard id list.
NProtoPrivate::TShardTopology GetShardTopology(
    const NProtoPrivate::TGetFileSystemTopologyResponse& response);

}   // namespace NCloud::NFileStore::NStorage
