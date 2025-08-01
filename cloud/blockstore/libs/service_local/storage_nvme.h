#pragma once

#include "public.h"

#include <cloud/blockstore/libs/nvme/public.h>
#include <cloud/blockstore/libs/service/public.h>

#include <cloud/storage/core/libs/diagnostics/public.h>

namespace NCloud::NBlockStore::NServer {

////////////////////////////////////////////////////////////////////////////////

IStorageProviderPtr CreateNvmeStorageProvider(
    ILoggingServicePtr logging,
    IStorageProviderPtr fallback,
    NNvme::INvmeManagerPtr nvmeManager);

}   // namespace NCloud::NBlockStore::NServer
