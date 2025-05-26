#pragma once

#include <cloud/blockstore/libs/service/public.h>

#include <cloud/storage/core/libs/common/error.h>

#include <util/generic/vector.h>

namespace NCloud::NBlockStore {

namespace NProto {

class TWriteDeviceBlocksRequest;

}   // namespace NProto

namespace NStorage {

////////////////////////////////////////////////////////////////////////////////

// The response to the TMultiAgentWriteRequest, which is not transmitted through
// the actor system and does not depend on the proto.
struct TMultiAgentWriteResponsePrivate
{

    NProto::TError Error;
    TVector<NProto::TError> ReplicationResponses;

    TMultiAgentWriteResponsePrivate() = default;

    TMultiAgentWriteResponsePrivate(TErrorResponse&& error)
        : Error(std::move(error))
    {}

    static bool HasError()
    {
        return true;
    }

    const NProto::TError& GetError() const
    {
        return Error;
    }
};

class IMultiAgentWriteHandler
{
public:
    virtual ~IMultiAgentWriteHandler() = default;

    virtual NThreading::TFuture<TMultiAgentWriteResponsePrivate>
    PerformMultiAgentWrite(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TWriteDeviceBlocksRequest> request) = 0;
};

using IMultiAgentWriteHandlerPtr = std::shared_ptr<IMultiAgentWriteHandler>;

////////////////////////////////////////////////////////////////////////////////

}   // namespace NStorage
}   // namespace NCloud::NBlockStore
