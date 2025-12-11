#pragma once

#include "public.h"

#include <cloud/storage/core/libs/common/error.h>

class TFileHandle;

namespace NCloud::NBlockStore::NServer {

////////////////////////////////////////////////////////////////////////////////
// Zero specific file extent.
// Calls platform-specific fast discard method

NProto::TError
ZeroFileExtent(TFileHandle& fileHandle, i64 offset, i64 length) noexcept;

}   // namespace NCloud::NBlockStore::NServer
