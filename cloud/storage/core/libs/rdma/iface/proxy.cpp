#include "proxy.h"

namespace NCloud::NStorage::NRdma {

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TClientEndpoint
{
    IClientEndpointPtr Endpoint;

    TClientEndpoint(IClientEndpointPtr endpoint)
        : Endpoint(std::move(endpoint))
    {}

    ~TClientEndpoint()
    {
        Endpoint->Stop();
    }
};

class TProxy
    : public IProxy
    , public IClientEndpointHandler
    , public std::enable_shared_from_this<TProxy>
{
private:
    IClientPtr Client;
    IClientEndpointHandlerPtr Handler;
    THashMap<std::pair<TString, ui32>, TClientEndpoint> Endpoints;
    ui32 Connected = 0;
    bool Unavailable = false;

public:
    TProxy(
        IClientPtr client,
        IClientEndpointHandlerPtr handler,
        TVector<std::pair<TString, ui32>> endpoints)
        : Client(std::move(client))
        , Handler(std::move(handler))
    {
        for (auto& [host, port]: endpoints) {
            auto result = Client->StartEndpoint(host, port, shared_from_this());
            if (HasError(result.GetError())) {
                STORAGE_THROW_SERVICE_ERROR(result.GetError());
            }
            Endpoints.try_emplace(
                std::pair(host, port),
                TClientEndpoint(result.ExtractResult()));
        }
    }

    IClientEndpointPtr GetEndpoint(const TString& host, ui32 port) override
    {
        if (auto it = Endpoints.find(std::pair(host, port));
            it != Endpoints.end())
        {
            return it->second.Endpoint;
        }
        return nullptr;
    }

    bool IsAlignedDataEnabled() const override
    {
        return Client->IsAlignedDataEnabled();
    }

    void HandleConnected() override
    {
        if (++Connected == Endpoints.size()) {
            Handler->HandleConnected();
            Unavailable = false;
        }
    }

    void HandleUnavailable() override
    {
        if (!Unavailable) {
            Handler->HandleUnavailable();
            Unavailable = true;
        }
    }

    void HandleDisconnected() override
    {
        --Connected;
        HandleUnavailable();
    }
};

////////////////////////////////////////////////////////////////////////////////

}   // namespace

IProxyPtr CreateProxy(
    IClientPtr client,
    IClientEndpointHandlerPtr handler,
    TVector<std::pair<TString, ui32>> endpoints)
{
    return std::make_shared<TProxy>(
        std::move(client),
        std::move(handler),
        std::move(endpoints));
}

}   // namespace NCloud::NStorage::NRdma
