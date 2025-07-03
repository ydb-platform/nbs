#pragma once

#include <cloud/blockstore/libs/client/public.h>
#include <cloud/blockstore/libs/service/public.h>

#include <util/generic/string.h>

namespace NCloud::NBlockStore::NCells {

////////////////////////////////////////////////////////////////////////////////

class THostEndpoint
{
private:
    const TString LogTag;
    const IBlockStorePtr Service;
    const IStoragePtr Storage;

    static TString BuildLogTag(
        const NClient::TClientAppConfigPtr& clientConfig,
        const TString& fqdn);

public:
    THostEndpoint() = default;
    THostEndpoint(
        const NClient::TClientAppConfigPtr& clientConfig,
        const TString& fqdn,
        IBlockStorePtr controlService,
        IBlockStorePtr storageService);

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

using TCellEndpoints = TVector<THostEndpoint>;
using TCellsEndpoints = THashMap<TString, TCellEndpoints>;

}   // namespace NCloud::NBlockStore::NCells
