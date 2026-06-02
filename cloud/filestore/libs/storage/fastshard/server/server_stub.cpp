#include "server.h"

namespace NCloud::NFileStore::NStorage::NFastShard {

namespace {

////////////////////////////////////////////////////////////////////////////////

class TServer: public IServer
{
public:
    explicit TServer(ui16 port)
    {
        Y_UNUSED(port);
    }

    void Start() override
    {
    }

    void Stop() override
    {
    }

    void RegisterShard(
        const TString& fileSystemId,
        IFileSystemShardPtr shard) override
    {
        Y_UNUSED(fileSystemId);
        Y_UNUSED(shard);
    }

    void UnregisterShard(const TString& fileSystemId) override
    {
        Y_UNUSED(fileSystemId);
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IServerPtr CreateServer(ui16 port)
{
    return std::make_unique<TServer>(port);
}

}   // namespace NCloud::NFileStore::NStorage::NFastShard
