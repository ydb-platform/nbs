#pragma once

#include "public.h"

#include <cloud/blockstore/libs/service/public.h>

namespace NCloud::NBlockStore::NServer {

////////////////////////////////////////////////////////////////////////////////

IStorageProviderPtr CreateNullStorageProvider();

IStoragePtr CreateNullStorage();

}   // namespace NCloud::NBlockStore::NServer
