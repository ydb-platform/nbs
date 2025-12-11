#include "client_storage_factory.h"

namespace NCloud::NBlockStore::NServer {

using namespace NCloud::NStorage::NServer;

////////////////////////////////////////////////////////////////////////////////

struct TClientStorageFactoryStub: public IClientStorageFactory
{
    NStorage::NServer::IClientStoragePtr CreateClientStorage(
        IBlockStorePtr service) override
    {
        Y_UNUSED(service);
        return CreateClientStorageStub();
    }
};

////////////////////////////////////////////////////////////////////////////////

IClientStorageFactoryPtr CreateClientStorageFactoryStub()
{
    return std::make_shared<TClientStorageFactoryStub>();
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NCloud::NBlockStore::NServer
