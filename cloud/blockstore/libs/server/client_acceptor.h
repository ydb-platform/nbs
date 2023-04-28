#pragma once

#include "public.h"

#include <cloud/blockstore/public/api/protos/headers.pb.h>

#include <cloud/blockstore/libs/service/public.h>

#include <util/network/socket.h>

namespace NCloud::NBlockStore::NServer {

////////////////////////////////////////////////////////////////////////////////

struct IClientAcceptor
{
    virtual ~IClientAcceptor() = default;

    virtual void Accept(
        const TSocketHolder& clientSocket,
        IBlockStorePtr service,
        NProto::ERequestSource source) = 0;

    virtual void Remove(const TSocketHolder& clientSocket) = 0;
};

////////////////////////////////////////////////////////////////////////////////

IClientAcceptorPtr CreateClientAcceptorStub();

}   // namespace NCloud::NBlockStore::NServer
