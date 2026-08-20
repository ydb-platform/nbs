#pragma once

#include <cloud/blockstore/libs/logbroker/iface/credentials_provider.h>
#include <cloud/blockstore/libs/logbroker/iface/public.h>

#include <cloud/storage/core/libs/diagnostics/public.h>

#include <functional>

namespace NYdb::inline V3::NTopic {
class IWriteSession;
}   // namespace NYdb::inline V3::NTopic

namespace NCloud::NBlockStore::NLogbroker {

////////////////////////////////////////////////////////////////////////////////

using TWriteSessionFactory =
    std::function<std::shared_ptr<NYdb::NTopic::IWriteSession>()>;

IServicePtr CreateTopicAPIService(
    TLogbrokerConfigPtr config,
    ILoggingServicePtr logging,
    std::shared_ptr<NYdbICredentialsProviderFactory> credentialsProviderFactory,
    TWriteSessionFactory writeSessionFactory);

IServicePtr CreateTopicAPIService(
    TLogbrokerConfigPtr config,
    ILoggingServicePtr logging,
    std::shared_ptr<NYdbICredentialsProviderFactory>
        credentialsProviderFactory);

IServicePtr CreateTopicAPIService(
    TLogbrokerConfigPtr config,
    ILoggingServicePtr logging);

}   // namespace NCloud::NBlockStore::NLogbroker
