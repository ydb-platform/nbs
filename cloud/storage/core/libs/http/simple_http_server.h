#pragma once

#include <library/cpp/http/server/http.h>
#include <library/cpp/http/server/http_ex.h>

namespace NCloud::NStorage {

////////////////////////////////////////////////////////////////////////////////

// The most simple HTTP server. Listens to the port and responds with "Response"
// with code 200.
class TSimpleHttpServer: public THttpServer::ICallBack
{
    class TRequest: public THttpClientRequestEx
    {
    private:
        TString Response;

    public:
        explicit TRequest(TString response);
        ~TRequest() override;

        bool Reply(void* threadSpecificResource) override;
    };

private:
    TString Response;
    std::unique_ptr<THttpServer> Server;

public:
    TSimpleHttpServer(TString host, ui16 port, TString response);
    ~TSimpleHttpServer() override;

    bool Start();
    void Stop();

    TClientRequest* CreateClient() override;
};

}   // namespace NCloud::NStorage
