#pragma once

#include "public.h"

#include <cloud/blockstore/libs/encryption/public.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

namespace NCloud::NBlockStore::NVHostServer {

////////////////////////////////////////////////////////////////////////////////

IBackendPtr CreateAioBackend(
    IEncryptorPtr encryptor,
    ILoggingServicePtr logging);

}   // namespace NCloud::NBlockStore::NVHostServer
