#pragma once

#include <util/system/defaults.h>

#include <memory>

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

struct IBlockBuffer;
using IBlockBufferPtr = std::shared_ptr<IBlockBuffer>;
using IConstBlockBufferPtr = std::shared_ptr<const IBlockBuffer>;

}   // namespace NCloud::NFileStore::NStorage
