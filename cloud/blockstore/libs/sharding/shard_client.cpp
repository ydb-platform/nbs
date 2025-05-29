#include "shard_client.h"

#include <cloud/blockstore/libs/client/config.h>

namespace NCloud::NBlockStore::NSharding {

////////////////////////////////////////////////////////////////////////////////

TString TShardClient::BuildLogTag(
    const NClient::TClientAppConfigPtr& clientConfig,
    const TString& fqdn)
{
    return TStringBuilder()
        << "[h:" << fqdn << "]"
        << "[i:" << clientConfig->GetInstanceId() << "]"
        << "[c:" << clientConfig->GetClientId() << "]";
}

}   // namespace NCloud::NBlockStore::NSharding
