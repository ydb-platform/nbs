#pragma once

#include <cloud/blockstore/libs/logbroker/iface/credentials_provider.h>
#include <cloud/blockstore/libs/logbroker/iface/public.h>

#include <cloud/storage/core/libs/diagnostics/public.h>

namespace NCloud::NBlockStore::NLogbroker {

////////////////////////////////////////////////////////////////////////////////

IServicePtr CreateTopicAPIService(
    TLogbrokerConfigPtr config,
    ILoggingServicePtr logging,
    std::shared_ptr<NYdbICredentialsProviderFactory>
        credentialsProviderFactory);

IServicePtr CreateTopicAPIService(
    TLogbrokerConfigPtr config,
    ILoggingServicePtr logging);

}   // namespace NCloud::NBlockStore::NLogbroker
