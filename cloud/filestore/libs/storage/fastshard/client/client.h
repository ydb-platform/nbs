#pragma once

#include <cloud/filestore/libs/storage/fastshard/server/protos/fastshard.pb.h>

#include <util/system/types.h>

namespace NCloud::NFileStore::NStorage::NFastShard {

////////////////////////////////////////////////////////////////////////////////

class IEndpoint
{
public:
    virtual ~IEndpoint() = default;

    virtual NProtoSrv::TResponse Send(const NProtoSrv::TRequest& req) = 0;
};

// Async TCP client for the fastshard server.
// Uses silk's non-blocking poll + io_uring under the hood.
// Must be called from a silk fiber context.
class TClient
{
public:
    std::shared_ptr<IEndpoint> Connect(const TString& host, ui16 port);
};

}   // namespace NCloud::NFileStore::NStorage::NFastShard
