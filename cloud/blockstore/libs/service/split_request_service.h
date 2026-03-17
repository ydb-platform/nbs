#pragma once

#include "public.h"

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

// A service for splitting requests by stripe boundaries or DiskRegistry-based
// disk devices.
IBlockStorePtr CreateSplitRequestService(IBlockStorePtr service);

////////////////////////////////////////////////////////////////////////////////

}   // namespace NCloud::NBlockStore
