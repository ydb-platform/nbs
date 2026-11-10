#include "data_endpoint_factory_stub.h"

#include "data_endpoint_factory.h"

namespace NCloud::NBlockStore::NIcV2 {

namespace {

////////////////////////////////////////////////////////////////////////////////

class TDataEndpointFactoryStub final: public IDataEndpointFactory
{
public:
    TResultOrError<IBlockStorePtr> CreateDataEndpoint(
        const TString& cellId,
        const TString& host,
        IHostConnectionHandlerPtr handler) override
    {
        Y_UNUSED(cellId);
        Y_UNUSED(host);
        Y_UNUSED(handler);

        return MakeError(
            E_NOT_IMPLEMENTED,
            "IC V2 data transport is unavailable");
    }

    THostChannelStatus GetDataEndpointStatus(
        const IBlockStorePtr& endpoint) override
    {
        Y_UNUSED(endpoint);

        return {.State = EHostChannelState::Disconnected};
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IDataEndpointFactoryPtr CreateDataEndpointFactoryStub()
{
    return std::make_shared<TDataEndpointFactoryStub>();
}

}   // namespace NCloud::NBlockStore::NIcV2
