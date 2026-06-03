#include "side_channel.h"

namespace NCloud::NFileStore {

namespace {

////////////////////////////////////////////////////////////////////////////////

class TTCPSideChannel: public ISideChannel
{
    bool ExecuteRequest(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TReadDataRequest> request,
        NThreading::TPromise<NProto::TReadDataResponse> response) override
    {
        Y_UNUSED(callContext, request, response);

        // TODO(#5894): impl

        return false;
    }

    virtual bool ExecuteRequest(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TWriteDataRequest> request,
        NThreading::TPromise<NProto::TWriteDataResponse> response) override
    {
        Y_UNUSED(callContext, request, response);

        // TODO(#5894): impl

        return false;
    }

    void Update(const NProto::TBackendInfo& backendInfo) override
    {
        Y_UNUSED(backendInfo);

        // TODO(#5894): impl
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

ISideChannelPtr CreateTCPSideChannel()
{
    return std::make_shared<TTCPSideChannel>();
}

}   // namespace NCloud::NFileStore
