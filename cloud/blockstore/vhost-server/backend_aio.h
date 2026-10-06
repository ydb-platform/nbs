#pragma once

#include "public.h"

#include <cloud/blockstore/libs/encryption/public.h>
#include <cloud/storage/core/libs/diagnostics/io_depth_tracker.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

namespace NCloud::NBlockStore::NVHostServer {

////////////////////////////////////////////////////////////////////////////////

IBackendPtr CreateAioBackend(
    IEncryptorPtr encryptor,
    ILoggingServicePtr logging,
    ui64 threadPoolSize,
    TIoDepthClock ioDepthClock = {});

}   // namespace NCloud::NBlockStore::NVHostServer
