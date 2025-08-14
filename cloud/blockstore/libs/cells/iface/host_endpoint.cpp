#include "host_endpoint.h"
#include "remote_storage.h"

#include <cloud/blockstore/libs/client/config.h>

namespace NCloud::NBlockStore::NCells {

namespace {

////////////////////////////////////////////////////////////////////////////////

TString BuildLogTag(
    const NClient::TClientAppConfigPtr& clientConfig,
    const TString& fqdn)
{
    return TStringBuilder() << "[h:" << fqdn << "]"
                            << "[i:" << clientConfig->GetInstanceId() << "]"
                            << "[c:" << clientConfig->GetClientId() << "]";
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TCellHostEndpoint::TCellHostEndpoint(
        const NClient::TClientAppConfigPtr& clientConfig,
        const TString& fqdn,
        IBlockStorePtr controlService,
        IBlockStorePtr dataService)
    : LogTag(BuildLogTag(clientConfig, fqdn))
    , Service(std::move(controlService))
    , Storage(CreateRemoteStorage(std::move(dataService)))
{}

}   // namespace NCloud::NBlockStore::NCells
