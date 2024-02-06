#pragma once

#include "public.h"

#include <cloud/blockstore/libs/service/public.h>

#include <cloud/storage/core/libs/uds/client_storage.h>

namespace NCloud::NBlockStore::NServer {

////////////////////////////////////////////////////////////////////////////////

struct IClientStorageFactory
{
    virtual ~IClientStorageFactory() = default;

    virtual NStorage::NServer::IClientStoragePtr CreateClientStorage(
        IBlockStorePtr service) = 0;
};

////////////////////////////////////////////////////////////////////////////////

IClientStorageFactoryPtr CreateClientStorageFactoryStub();

////////////////////////////////////////////////////////////////////////////////

}   // namespace NCloud::NBlockStore::NServer
