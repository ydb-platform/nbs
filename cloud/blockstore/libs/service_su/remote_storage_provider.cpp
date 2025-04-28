#include "remote_storage_provider.h"

namespace NCloud::NBlockStore::NServer {

using namespace NThreading;

using namespace NCloud::NBlockStore::NStorage;

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TRemoteEndpoint
{
    IBlockStorePtr Service;
    IBlockStorePtr Storage;
}

////////////////////////////////////////////////////////////////////////////////

struct TRemoteStorageProvider
    : public IRemoteStorageProvider
{
    TServerAppConfigPtr Config;
    THashMap<TString, TVector<std::pair<IBlockStorePtr, IBlockStorePtr>>> StorageCache;

    TRemoteStorageProvider(
        TServerAppConfigPtr config,
        TVector<std::pair<NCloud::NProto::EShardDataTransport, NProto::TTransportFactory>> factories)
        : Config(std::move(config))
    {

    }


    IBlockStorePtr CreateStorage(const TString& shardId) override
    {
        if
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

}   // namespace NCloud::NBlockStore::NServer
