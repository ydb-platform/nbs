#pragma once

#include <cloud/filestore/libs/storage/fastshard/sn/quorum/storage_group.h>

#include <cloud/filestore/private/api/protos/tablet.pb.h>

namespace NCloud::NFileStore::NStorage::NFastShard {

////////////////////////////////////////////////////////////////////////////////

struct IStorageGroupFactory
{
    virtual ~IStorageGroupFactory() = default;
    virtual IStorageGroupPtr MakeStorageGroup(
        const NProtoPrivate::TPersistentFastShardConfig& config,
        ui64 generation) = 0;
};

}   // namespace NCloud::NFileStore::NStorage::NFastShard
