#pragma once

#include <cloud/blockstore/libs/service/storage.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

IStoragePtr CreateBrokenStorage();

}   // namespace NCloud::NBlockStore::NStorage
