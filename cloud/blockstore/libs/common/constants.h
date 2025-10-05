#pragma once

#include <util/generic/fwd.h>
#include <util/generic/size_literals.h>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////
constexpr ui32 DefaultLocalSSDBlockSize = 512_B;

// The maximum possible volume block size.
constexpr ui32 MaxBlockSize = 128_KB;

// Keep the value less than MaxBufferSize in
// cloud/blockstore/libs/rdma/iface/client.h
constexpr ui32 MaxSubRequestSize = 4_MB;

// Volume tag that indicates that data integrity violation has been detected at
// least once.
constexpr TStringBuf DataIntegrityViolationDetectedTagName =
    "data-integrity-violation-detected";

// Volume tag that enables copying user's buffer to an intermediate buffer.
constexpr TStringBuf IntermediateWriteBufferTagName =
    "use-intermediate-write-buffer";

// If the tag is set, it means that this disk is being copied and the name of
// the disk from where the copy is being made should be taken from the tag.
constexpr TStringBuf SourceDiskIdTagName = "source-disk-id";

constexpr TStringBuf UseFastPathTagName = "use-fastpath";

}   // namespace NCloud::NBlockStore
