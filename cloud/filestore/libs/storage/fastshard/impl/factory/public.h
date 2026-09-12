#pragma once

#include <util/system/defaults.h>

#include <memory>

namespace NCloud::NFileStore::NStorage::NFastShard {

////////////////////////////////////////////////////////////////////////////////

struct IStorageGroupFactory;
using IStorageGroupFactoryPtr = std::shared_ptr<IStorageGroupFactory>;

}   // namespace NCloud::NFileStore::NStorage::NFastShard
