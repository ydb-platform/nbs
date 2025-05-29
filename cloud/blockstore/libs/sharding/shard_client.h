#pragma once

#include "public.h"

#include <cloud/blockstore/libs/client/public.h>
#include <cloud/blockstore/libs/service/public.h>

#include <util/generic/string.h>

namespace NCloud::NBlockStore::NSharding {

////////////////////////////////////////////////////////////////////////////////

class TShardClient
{
private:
    const TString LogTag;
    const IBlockStorePtr Service;
    const IStoragePtr Storage;

    static TString BuildLogTag(
        const NClient::TClientAppConfigPtr& clientConfig,
        const TString& fqdn);

public:
    TShardClient() = default;
    TShardClient(
            const NClient::TClientAppConfigPtr& clientConfig,
            const TString& fqdn,
            IBlockStorePtr service,
            IStoragePtr storage)
        : LogTag(BuildLogTag(clientConfig, fqdn))
        , Service(std::move(service))
        , Storage(std::move(storage))
    {}

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

using TShardClients = THashMap<TString, TVector<TShardClient>>;

}   // namespace NCloud::NBlockStore::NSharding
