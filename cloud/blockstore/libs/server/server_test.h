#pragma once

#include "public.h"

#include "config.h"

#include <cloud/blockstore/libs/client/config.h>
#include <cloud/blockstore/libs/client/public.h>
#include <cloud/blockstore/libs/common/public.h>
#include <cloud/blockstore/libs/diagnostics/public.h>
#include <cloud/blockstore/libs/service/public.h>
#include <cloud/storage/core/libs/grpc/init.h>

#include <library/cpp/logger/log.h>

namespace NCloud::NBlockStore::NServer {

////////////////////////////////////////////////////////////////////////////////

struct TTestContext
{
    ITimerPtr Timer;
    ISchedulerPtr Scheduler;
    ILoggingServicePtr Logging;
    IMonitoringServicePtr Monitoring;
    IProfileLogPtr ProfileLog;
    IRequestStatsPtr RequestStats;
    IVolumeStatsPtr VolumeStats;
    TString CellId;
};

////////////////////////////////////////////////////////////////////////////////

class TTestServerBuilder final
{
private:
    TTestContext TestContext;
    NProto::TServerAppConfig ServerAppConfig;

public:
    explicit TTestServerBuilder(TTestContext testContext);

    TTestServerBuilder& SetPort(ui16 port);

    TTestServerBuilder& SetDataPort(ui16 port);

    TTestServerBuilder& SetSecureEndpoint(
        ui16 port,
        const TString& rootCertsFileName,
        const TString& certFileName,
        const TString& certPrivateKeyFileName);

    TTestServerBuilder& AddCert(
        const TString& certFileName,
        const TString& certPrivateKeyFileName);

    TTestServerBuilder& SetUnixSocketPath(const TString& unixSocketPath);

    TTestServerBuilder& SetVolumeStats(IVolumeStatsPtr volumeStats);

    TTestServerBuilder& SetCellId(TString cellId);

    IServerPtr BuildServer(
        IBlockStorePtr service,
        IBlockStorePtr udsService = nullptr);
};

////////////////////////////////////////////////////////////////////////////////

class TTestClientBuilder final
{
private:
    TTestContext TestContext;
    NProto::TClientAppConfig ClientAppConfig;

public:
    explicit TTestClientBuilder(TTestContext testContext);

    TTestClientBuilder& SetPort(ui16 port);

    TTestClientBuilder& SetDataPort(ui16 port);

    TTestClientBuilder& SetClientId(const TString& clientId);

    TTestClientBuilder& SetSecureEndpoint(
        ui16 port,
        const TString& rootCertsFileName,
        const TString& authToken);

    TTestClientBuilder& SetCertificate(
        const TString& certsFileName,
        const TString& certPrivateKeyFileName);

    TTestClientBuilder& SetUnixSocketPath(const TString& unixSocketPath);

    TTestClientBuilder& SetVolumeStats(IVolumeStatsPtr volumeStats);

    NClient::IClientPtr BuildClient();
};

////////////////////////////////////////////////////////////////////////////////

class TTestFactory final
    : public TTestContext
{
private:
    TGrpcInitializer GrpcInitializer;

public:
    TTestFactory();

    TTestServerBuilder CreateServerBuilder();

    TTestClientBuilder CreateClientBuilder();

    IBlockStorePtr CreateDurableClient(IBlockStorePtr client);
};

}   // namespace NCloud::NBlockStore::NServer
