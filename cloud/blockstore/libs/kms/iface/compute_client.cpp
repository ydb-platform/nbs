#include "compute_client.h"

namespace NCloud::NBlockStore {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TComputeClientStub: public IComputeClient
{
public:
    void Start() override
    {}

    void Stop() override
    {}

    TFuture<TResponse> CreateTokenForDEK(
        const TString& diskId,
        const TString& taskId,
        const TString& authToken) override
    {
        Y_UNUSED(diskId);
        Y_UNUSED(taskId);
        Y_UNUSED(authToken);
        return MakeFuture<TResponse>(TErrorResponse(
            E_NOT_IMPLEMENTED,
            "ComputeClientStub can't create token"));
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IComputeClientPtr CreateComputeClientStub()
{
    return std::make_shared<TComputeClientStub>();
}

}   // namespace NCloud::NBlockStore
