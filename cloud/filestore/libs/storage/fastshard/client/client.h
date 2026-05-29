#pragma once

#include <cloud/filestore/libs/storage/fastshard/server/protos/fastshard.pb.h>

#include <util/system/types.h>

namespace NCloud::NFileStore::NStorage::NFastShard {

////////////////////////////////////////////////////////////////////////////////

// Async TCP client for the fastshard server.
// Uses silk's non-blocking poll + io_uring under the hood.
// Must be called from a silk fiber context.
class TClient
{
public:
    explicit TClient(ui16 port);

    NProtoSrv::TResponse Send(const NProtoSrv::TRequest& req);

private:
    int Connect();

    ui16 Port;
};

}   // namespace NCloud::NFileStore::NStorage::NFastShard
