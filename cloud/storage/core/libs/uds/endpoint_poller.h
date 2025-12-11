#pragma once

#include "client_storage.h"

#include <cloud/blockstore/libs/service/public.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/startable.h>
#include <cloud/storage/core/protos/request_source.pb.h>

#include <util/generic/string.h>

namespace NCloud::NStorage::NServer {

////////////////////////////////////////////////////////////////////////////////

class TEndpointPoller
{
private:
    class TImpl;
    std::unique_ptr<TImpl> Impl;

public:
    TEndpointPoller();
    ~TEndpointPoller();

    void Start();
    void Stop();

    NProto::TError StartListenEndpoint(
        const TString& unixSocketPath,
        ui32 backlog,
        int accessMode,
        bool multiClient,
        NProto::ERequestSource source,
        IClientStoragePtr clientStorage);

    NProto::TError StopListenEndpoint(const TString& unixSocketPath);
};

}   // namespace NCloud::NStorage::NServer
