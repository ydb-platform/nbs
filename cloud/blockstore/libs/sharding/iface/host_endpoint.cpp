#include "host_endpoint.h"
#include "remote_storage.h"

#include <cloud/blockstore/libs/client/config.h>

namespace NCloud::NBlockStore::NSharding {

////////////////////////////////////////////////////////////////////////////////

THostEndpoint::THostEndpoint(
        const NClient::TClientAppConfigPtr& clientConfig,
        const TString& fqdn,
        IBlockStorePtr controlService,
        IBlockStorePtr storageService)
    : LogTag(BuildLogTag(clientConfig, fqdn))
    , Service(std::move(controlService))
    , Storage(CreateRemoteStorage(std::move(storageService)))
{}

TString THostEndpoint::BuildLogTag(
    const NClient::TClientAppConfigPtr& clientConfig,
    const TString& fqdn)
{
    return TStringBuilder()
        << "[h:" << fqdn << "]"
        << "[i:" << clientConfig->GetInstanceId() << "]"
        << "[c:" << clientConfig->GetClientId() << "]";
}

}   // namespace NCloud::NBlockStore::NSharding
