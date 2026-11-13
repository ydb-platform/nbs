#pragma once

#include <cloud/storage/core/libs/rdma/iface/server.h>

#include <util/generic/string.h>

#include <functional>
#include <memory>

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

// In-process stand-in for the shared RDMA server: keeps the handler passed to
// StartEndpoint() and reports the endpoint replies through callbacks.
class TTestRdmaServer final
    : public NCloud::NStorage::NRdma::IServer
{
public:
    using TResponseCallback =
        std::function<void(void* context, size_t responseBytes)>;
    using TErrorCallback =
        std::function<void(void* context, ui32 error, TStringBuf message)>;

private:
    class TEndpoint;

    std::shared_ptr<TEndpoint> Endpoint;
    NCloud::NStorage::NRdma::IServerHandlerPtr Handler;
    TString Host;
    ui32 Port = 0;

public:
    TTestRdmaServer(TResponseCallback onResponse, TErrorCallback onError);

    NCloud::NStorage::NRdma::IServerEndpointPtr StartEndpoint(
        TString host,
        ui32 port,
        NCloud::NStorage::NRdma::IServerHandlerPtr handler) override;

    void Start() override;
    void Stop() override;
    void DumpHtml(IOutputStream& out) const override;

    NCloud::NStorage::NRdma::IServerHandlerPtr GetHandler() const;
    const TString& GetHost() const;
    ui32 GetPort() const;
};

}   // namespace NCloud::NFileStore::NStorage
