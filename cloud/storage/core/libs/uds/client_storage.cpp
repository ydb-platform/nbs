#include "client_storage.h"

namespace NCloud::NStorage::NServer {

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TClientStorageStub final: public IClientStorage
{
    void AddClient(
        const TSocketHolder& socket,
        NCloud::NProto::ERequestSource source) override
    {
        Y_UNUSED(socket);
        Y_UNUSED(source);
    }

    void RemoveClient(const TSocketHolder& socket) override
    {
        Y_UNUSED(socket);
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IClientStoragePtr CreateClientStorageStub()
{
    return std::make_shared<TClientStorageStub>();
}

}   // namespace NCloud::NStorage::NServer
