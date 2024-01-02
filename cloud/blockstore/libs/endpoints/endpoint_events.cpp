#include "endpoint_events.h"

namespace NCloud::NBlockStore::NServer {

namespace {

////////////////////////////////////////////////////////////////////////////////

class TEndpointEventProxy: public IEndpointEventProxy
{
private:
    IEndpointEventHandlerPtr Handler;

public:
    void OnVolumeConnectionEstablished(const TString& diskId) override;
    void Register(IEndpointEventHandlerPtr listener) override;
};

////////////////////////////////////////////////////////////////////////////////

void TEndpointEventProxy::OnVolumeConnectionEstablished(const TString& diskId)
{
    if (Handler) {
        Handler->OnVolumeConnectionEstablished(diskId);
    }
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
