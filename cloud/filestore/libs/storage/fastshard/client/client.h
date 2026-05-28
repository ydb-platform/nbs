#pragma once

#include <cloud/filestore/libs/storage/fastshard/server/protos/fastshard.pb.h>

#include <util/system/types.h>

namespace NCloud::NFileStore::NStorage::NFastShard {

////////////////////////////////////////////////////////////////////////////////

// Simple blocking TCP client for the fastshard server.
// Must be called from a silk fiber context — uses ThreadModeScope internally
// so blocking socket calls don't stall the scheduler.
class TClient
{
public:
    explicit TClient(ui16 port);

    NProtoSrv::TResponse Send(const NProtoSrv::TRequest& req);

private:
    int Connect();
    static void RecvAll(int fd, void* buf, size_t len);
    static void SendAll(int fd, const void* buf, size_t len);

    ui16 Port;
};

}   // namespace NCloud::NFileStore::NStorage::NFastShard
