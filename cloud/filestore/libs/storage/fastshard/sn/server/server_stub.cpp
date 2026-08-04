#include "server.h"

namespace NCloud::NFileStore::NStorage::NFastShard {

namespace {

////////////////////////////////////////////////////////////////////////////////

class TServer: public IServer
{
public:
    TServer(ui16 port, IStorageNodePtr storage)
    {
        Y_UNUSED(port);
        Y_UNUSED(storage);
    }

    void Start() override
    {}

    void Stop() override
    {}
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IServerPtr CreateServer(ui16 port, IStorageNodePtr storage)
{
    return std::make_unique<TServer>(port, std::move(storage));
}

}   // namespace NCloud::NFileStore::NStorage::NFastShard
