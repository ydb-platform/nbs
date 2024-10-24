#include "simple_http_server.h"

namespace NCloud::NStorage {

////////////////////////////////////////////////////////////////////////////////

TSimpleHttpServer::TRequest::TRequest(TString response)
    : Response(std::move(response))
{}

TSimpleHttpServer::TRequest::~TRequest() = default;

bool TSimpleHttpServer::TRequest::Reply(void* /*threadSpecificResource*/)
{
    if (!ProcessHeaders()) {
        return true;
    }

    Output() << "HTTP/1.1 200 Ok\r\n"
             << "Content-Length: " << Response.size() << "\r\n"
             << "Content-Type: text/html\r\n\r\n"
             << Response;
    Output().Finish();

    return true;
}

////////////////////////////////////////////////////////////////////////////////

TSimpleHttpServer::TSimpleHttpServer(TString host, ui16 port, TString response)
    : Response(std::move(response))
{
    THttpServer::TOptions options;
    options.Host = std::move(host);
    options.Port = port;
    options.nThreads = 1;
    options.KeepAliveEnabled = false;
    options.RejectExcessConnections = true;
    Server = std::make_unique<THttpServer>(this, std::move(options));
}

TSimpleHttpServer::~TSimpleHttpServer()
{
    Server->Stop();
    Server.reset();
}

bool TSimpleHttpServer::Start()
{
    return Server->Start();
}

void TSimpleHttpServer::Stop()
{
    Server->Stop();
}

TClientRequest* TSimpleHttpServer::CreateClient()
{
    return new TSimpleHttpServer::TRequest(Response);
}

}   // namespace NCloud::NStorage
