#pragma once

#include "public.h"

#include <cloud/blockstore/libs/encryption/public.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <library/cpp/threading/future/core/future.h>

namespace NCloud::NBlockStore::NVHostServer {

////////////////////////////////////////////////////////////////////////////////

IBackendPtr CreateAioBackend(
    NThreading::TFuture<IEncryptorPtr> encryptor,
    ILoggingServicePtr logging);

}   // namespace NCloud::NBlockStore::NVHostServer
