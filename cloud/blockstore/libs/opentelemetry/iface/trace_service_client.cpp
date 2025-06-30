#include "trace_service_client.h"

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

class TTraceServiceClientStub: public ITraceServiceClient
{
public:
    void Start() override
    {}

    void Stop() override
    {}

    NThreading::TFuture<TResponse> Export(
        TRequest traces,
        const TString& authToken) override
    {
        Y_UNUSED(traces);
        Y_UNUSED(authToken);
        return NThreading::MakeFuture<TResponse>(TErrorResponse(
            E_NOT_IMPLEMENTED,
            "TraceServiceClientStub can't export traces"));
    }
};

ITraceServiceClientPtr CreateTraceServiceClientStub()
{
    return std::make_shared<TTraceServiceClientStub>();
}
}   // namespace NCloud::NBlockStore
