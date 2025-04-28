#pragma once

#include "public.h"

#include <cloud/blockstore/config/server.pb.h>
#include <cloud/blockstore/libs/server/public.h>
#include <cloud/blockstore/libs/service/service.h>

namespace NCloud::NBlockStore::NServer {

////////////////////////////////////////////////////////////////////////////////

struct IRemoteStorageProvider
{
    virtual ~IRemoteStorageProvider() = default;

    virtual IBlockStorePtr CreateStorage(const TString& shardId) = 0;
};

////////////////////////////////////////////////////////////////////////////////

using TTransportFactory = std::function<IBlockStorePtr>();

////////////////////////////////////////////////////////////////////////////////

IRemoteStorageProviderPtr CreateRemoteStorageProvider(
    TServerAppConfigPtr config,
    TVector<std::pair<NProto::EShardDataTransport, NProto::TTransportFactory>> factories
);


}   // namespace NCloud::NBlockStore::NServer
