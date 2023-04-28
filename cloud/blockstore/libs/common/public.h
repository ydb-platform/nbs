#pragma once

#include <cloud/storage/core/libs/common/public.h>

#include <util/generic/ptr.h>
#include <util/generic/size_literals.h>

#include <memory>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

constexpr ui32 DefaultBlockSize = 4_KB;

////////////////////////////////////////////////////////////////////////////////

struct ICachingAllocator;
using ICachingAllocatorPtr = std::shared_ptr<ICachingAllocator>;

}   // namespace NCloud::NBlockStore
