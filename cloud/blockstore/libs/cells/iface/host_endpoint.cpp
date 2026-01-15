#include "host_endpoint.h"

#include <cloud/blockstore/libs/client/config.h>
#include <cloud/blockstore/libs/service/storage.h>

namespace NCloud::NBlockStore::NCells {

////////////////////////////////////////////////////////////////////////////////

TCellHostEndpoint::TCellHostEndpoint(
    const NClient::TClientAppConfigPtr& clientConfig,
    const TString& fqdn,
    IBlockStorePtr controlService,
    IStoragePtr dataStorage)
    : LogTag(BuildLogTag(clientConfig, fqdn))
    , Service(std::move(controlService))
    , Storage(std::move(dataStorage))
{}

TString TCellHostEndpoint::BuildLogTag(
    const NClient::TClientAppConfigPtr& clientConfig,
    const TString& fqdn)
{
    return TStringBuilder() << "[h:" << fqdn << "]"
                            << "[i:" << clientConfig->GetInstanceId() << "]"
                            << "[c:" << clientConfig->GetClientId() << "]";
}

}   // namespace NCloud::NBlockStore::NCells
