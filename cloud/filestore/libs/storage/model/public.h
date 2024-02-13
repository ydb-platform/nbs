#pragma once

#include <util/system/defaults.h>

#include <memory>

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

struct IBlockBuffer;
using IBlockBufferPtr = std::shared_ptr<IBlockBuffer>;

}   // namespace NCloud::NFileStore::NStorage
