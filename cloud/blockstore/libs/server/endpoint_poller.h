#pragma once

#include "public.h"

#include <cloud/blockstore/public/api/protos/headers.pb.h>

#include <cloud/blockstore/libs/service/public.h>
#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/startable.h>

#include <util/generic/string.h>

namespace NCloud::NBlockStore::NServer {

////////////////////////////////////////////////////////////////////////////////

class TEndpointPoller
{
private:
    class TImpl;
    std::unique_ptr<TImpl> Impl;

public:
    TEndpointPoller(IClientAcceptorPtr clientAcceptor);
    ~TEndpointPoller();

    void Start();
    void Stop();

    NProto::TError StartListenEndpoint(
        const TString& unixSocketPath,
        ui32 backlog,
        bool multiClient,
        NProto::ERequestSource source,
        IBlockStorePtr service);

    NProto::TError StopListenEndpoint(const TString& unixSocketPath);
};

}   // namespace NCloud::NBlockStore::NServer
