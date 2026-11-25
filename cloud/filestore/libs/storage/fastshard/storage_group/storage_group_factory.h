#pragma once

#include "public.h"

#include <cloud/filestore/private/api/protos/tablet.pb.h>

#include <cloud/fastshard/libs/storage_group/storage_group.h>

namespace NCloud::NFileStore::NStorage::NFastShard {

using NCloud::NFastShard::IStorageGroupPtr;

////////////////////////////////////////////////////////////////////////////////

struct IStorageGroupFactory
{
    virtual ~IStorageGroupFactory() = default;
    virtual IStorageGroupPtr MakeStorageGroup(
        const NProtoPrivate::TPersistentFastShardConfig& config,
        ui64 generation) = 0;
};

}   // namespace NCloud::NFileStore::NStorage::NFastShard
