#pragma once

#include <cloud/blockstore/libs/service/public.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

IStoragePtr CreateHashTableStorage(ui32 blockSize, ui64 blockCount);

}   // namespace NCloud::NBlockStore::NStorage
