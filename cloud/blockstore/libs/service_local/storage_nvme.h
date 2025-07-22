#pragma once

#include "public.h"

#include <cloud/blockstore/libs/service/public.h>

namespace NCloud::NBlockStore::NServer {

////////////////////////////////////////////////////////////////////////////////

IStorageProviderPtr CreateNvmeStorageProvider(IStorageProviderPtr fallback);

}   // namespace NCloud::NBlockStore::NServer
