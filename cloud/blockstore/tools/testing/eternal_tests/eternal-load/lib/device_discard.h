#pragma once

#include <cloud/storage/core/libs/common/error.h>

#include <util/system/file.h>
#include <util/system/types.h>

namespace NCloud::NBlockStore::NTesting {

////////////////////////////////////////////////////////////////////////////////

// Issues BLKDISCARD for [offset, offset + length) on a block device.
// Returns an error if the file handle is not a block device or discard fails.
NProto::TError DiscardDeviceRange(
    TFileHandle& file,
    ui64 offset,
    ui64 length);

}   // namespace NCloud::NBlockStore::NTesting
