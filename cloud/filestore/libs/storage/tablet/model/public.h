#pragma once

#include <util/system/defaults.h>

#include <memory>

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

struct IBlockIterator;
using IBlockIteratorPtr = std::shared_ptr<IBlockIterator>;

struct IBlockLocation2RangeIndex;
using IBlockLocation2RangeIndexPtr = std::shared_ptr<IBlockLocation2RangeIndex>;

class IShardBalancer;
using IShardBalancerPtr = std::shared_ptr<IShardBalancer>;

}   // namespace NCloud::NFileStore::NStorage
