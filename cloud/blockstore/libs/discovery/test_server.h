#pragma once

#include <util/generic/string.h>

#include <memory>

namespace NCloud::NBlockStore::NDiscovery {

////////////////////////////////////////////////////////////////////////////////

class TFakeBlockStoreServer
{
public:
    explicit TFakeBlockStoreServer(
        ui16 port,
        ui16 securePort = 0,
        const TString& rootCertsFile = {},
        const TString& keyFile = {},
        const TString& certFile = {});
    ~TFakeBlockStoreServer();

public:
    void ForkStart();
    void Start();
    ui16 Port() const;
    ui16 SecurePort() const;
    void SetLastByteCount(ui64 count);
    void SetErrorMessage(TString error);
    void SetDropPingRequests(bool drop);

private:
    struct TImpl;
    std::unique_ptr<TImpl> Impl;
};

}   // namespace NCloud::NBlockStore::NDiscovery
