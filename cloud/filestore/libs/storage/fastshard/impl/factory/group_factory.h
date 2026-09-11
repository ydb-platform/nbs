#pragma once

#include "public.h"

#include <cloud/filestore/libs/storage/fastshard/sn/factory/group_factory.h>

#include <cloud/filestore/private/api/protos/tablet.pb.h>

namespace NCloud::NFileStore::NStorage::NFastShard {

/**
 * An unrecognised type falls back to E_SG_MIRROR.
 */
IStorageGroupFactoryPtr CreateStorageGroupFactory();

}   // namespace NCloud::NFileStore::NStorage::NFastShard
