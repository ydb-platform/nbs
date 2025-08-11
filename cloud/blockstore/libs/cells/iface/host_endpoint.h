#pragma once

#include <cloud/blockstore/libs/client/public.h>
#include <cloud/blockstore/libs/service/public.h>

#include <util/generic/string.h>

namespace NCloud::NBlockStore::NCells {

////////////////////////////////////////////////////////////////////////////////

class TCellHostEndpoint
{
private:
    const TString LogTag;
    const IBlockStorePtr Service;
    const IStoragePtr Storage;

    static TString BuildLogTag(
        const NClient::TClientAppConfigPtr& clientConfig,
        const TString& fqdn);

public:
    TCellHostEndpoint() = default;
    TCellHostEndpoint(
        const NClient::TClientAppConfigPtr& clientConfig,
        const TString& fqdn,
        IBlockStorePtr controlService,
        IBlockStorePtr dataService);

    const TString& GetLogTag() const
    {
        return LogTag;
    }

    [[nodiscard]] IBlockStorePtr GetService() const
    {
        return Service;
    }

    [[nodiscard]] IStoragePtr GetStorage() const
    {
        return Storage;
    }
};

using TCellHostEndpoints = TVector<TCellHostEndpoint>;

using TCellHostEndpointsByCellId = THashMap<TString, TCellHostEndpoints>;

}   // namespace NCloud::NBlockStore::NCells
