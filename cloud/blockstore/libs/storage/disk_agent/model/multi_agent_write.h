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

    NProto::TError Error;
    TVector<NProto::TError> ReplicationResponses;
};

class IMultiagentWriteHandler
{
public:
    virtual ~IMultiagentWriteHandler() = default;

    virtual NThreading::TFuture<TMultiAgentWriteResponsePrivate>
    PerformMultiAgentWrite(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TWriteDeviceBlocksRequest> request) = 0;
};

////////////////////////////////////////////////////////////////////////////////

}   // namespace NStorage
}   // namespace NCloud::NBlockStore
