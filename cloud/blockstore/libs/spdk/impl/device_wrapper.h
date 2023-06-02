#pragma once

#include "public.h"

#include <util/generic/string.h>

namespace NCloud::NBlockStore::NSpdk {

////////////////////////////////////////////////////////////////////////////////

int RegisterDeviceWrapper(
    ISpdkDevicePtr device,
    TString name,
    ui64 blocksCount,
    ui32 blockSize);

}   // namespace NCloud::NBlockStore::NSpdk
