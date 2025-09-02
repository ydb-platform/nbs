#pragma once

#include <cloud/storage/core/libs/common/public.h>

#include <util/generic/ptr.h>
#include <util/generic/size_literals.h>

#include <memory>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

constexpr ui32 DefaultBlockSize = 4_KB;
constexpr ui32 DefaultLocalSSDBlockSize = 512_B;
constexpr ui32 MaxBlockSize = 128_KB;

// Keep the value less than MaxBufferSize in
// cloud/blockstore/libs/rdma/iface/client.h
constexpr ui32 MaxSubRequestSize = 4_MB;

////////////////////////////////////////////////////////////////////////////////

struct ICachingAllocator;
using ICachingAllocatorPtr = std::shared_ptr<ICachingAllocator>;

}   // namespace NCloud::NBlockStore
