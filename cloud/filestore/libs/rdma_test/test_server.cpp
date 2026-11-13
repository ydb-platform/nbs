#include "test_server.h"

namespace NCloud::NFileStore::NStorage {

using namespace NCloud::NStorage::NRdma;

////////////////////////////////////////////////////////////////////////////////

class TTestRdmaServer::TEndpoint final
    : public IServerEndpoint
{
private:
    const TResponseCallback OnResponse;
    const TErrorCallback OnError;

public:
    TEndpoint(TResponseCallback onResponse, TErrorCallback onError)
        : OnResponse(std::move(onResponse))
        , OnError(std::move(onError))
    {}

    void SendResponse(void* context, size_t responseBytes) override
    {
        OnResponse(context, responseBytes);
    }

    void SendError(void* context, ui32 error, TStringBuf message) override
    {
        OnError(context, error, message);
    }
};

////////////////////////////////////////////////////////////////////////////////

TTestRdmaServer::TTestRdmaServer(
        TResponseCallback onResponse,
        TErrorCallback onError)
    : Endpoint(std::make_shared<TEndpoint>(
          std::move(onResponse),
          std::move(onError)))
{}

IServerEndpointPtr TTestRdmaServer::StartEndpoint(
    TString host,
    ui32 port,
    IServerHandlerPtr handler)
{
    Host = std::move(host);
    Port = port;
    Handler = std::move(handler);
    return Endpoint;
}

void TTestRdmaServer::Start()
{}

void TTestRdmaServer::Stop()
{}

void TTestRdmaServer::DumpHtml(IOutputStream& out) const
{
    Y_UNUSED(out);
}

IServerHandlerPtr TTestRdmaServer::GetHandler() const
{
    return Handler;
}

const TString& TTestRdmaServer::GetHost() const
{
    return Host;
}

ui32 TTestRdmaServer::GetPort() const
{
    return Port;
}

}   // namespace NCloud::NFileStore::NStorage
