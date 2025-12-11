#pragma once

#include "public.h"

#include <cloud/blockstore/libs/service/public.h>

#include <cloud/storage/core/libs/diagnostics/public.h>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

IBlockStorePtr CreateMultipleEncryptionService(
    IBlockStorePtr service,
    ILoggingServicePtr logging,
    IEncryptionClientFactoryPtr encryptionClientFactory);

}   // namespace NCloud::NBlockStore
