#include "endpoint_events.h"

namespace NCloud::NBlockStore::NServer {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TEndpointEventProxy: public IEndpointEventProxy
{
private:
    IEndpointEventHandlerPtr Handler;

public:
    TFuture<NProto::TError> SwitchEndpointIfNeeded(
        const TString& diskId,
        const TString& reason) override;
    void Register(IEndpointEventHandlerPtr listener) override;
};

////////////////////////////////////////////////////////////////////////////////

TFuture<NProto::TError> TEndpointEventProxy::SwitchEndpointIfNeeded(
    const TString& diskId,
    const TString& reason)
{
    if (Handler) {
        return Handler->SwitchEndpointIfNeeded(diskId, reason);
    }

    return MakeFuture(MakeError(S_OK));
}

void TEndpointEventProxy::Register(IEndpointEventHandlerPtr listener)
{
    Handler = std::move(listener);
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IEndpointEventProxyPtr CreateEndpointEventProxy()
{
    return std::make_shared<TEndpointEventProxy>();
}

}   // namespace NCloud::NBlockStore::NServer
