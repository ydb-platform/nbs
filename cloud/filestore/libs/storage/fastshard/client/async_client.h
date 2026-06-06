#pragma once

#include "client.h"

#include <cloud/filestore/libs/storage/fastshard/server/protos/fastshard.pb.h>

#include <library/cpp/threading/future/future.h>

#include <util/generic/string.h>
#include <util/system/types.h>

#include <memory>

namespace NCloud::NFileStore::NStorage::NFastShard {

////////////////////////////////////////////////////////////////////////////////

class IAsyncEndpoint
{
public:
    virtual ~IAsyncEndpoint() = default;

    // Spawn a fiber to execute the round-trip and return its result.
    // Concurrent calls on the same endpoint are not supported; callers
    // must serialize sends.
    virtual NThreading::TFuture<NProtoSrv::TResponse> Send(
        NProtoSrv::TRequest req) = 0;
};

using IAsyncEndpointPtr = std::unique_ptr<IAsyncEndpoint>;

// TFuture-based wrapper around TClient. May be called from any thread.
// Requires silk::FiberScheduler to be initialized.
class TAsyncClient
{
public:
    TAsyncClient();

public:
    // Returns a future that resolves to a connected endpoint on success,
    // or to nullptr if the connection could not be established.
    NThreading::TFuture<IAsyncEndpointPtr> Connect(
        const TString& host,
        ui16 port);

private:
    std::shared_ptr<TClient> Client;
};

}   // namespace NCloud::NFileStore::NStorage::NFastShard
