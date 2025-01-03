#include "bootstrap.h"

#include "options.h"

#include <cloud/blockstore/config/server.pb.h>

#include <cloud/blockstore/libs/client/client.h>
#include <cloud/blockstore/libs/diagnostics/server_stats.h>
#include <cloud/blockstore/libs/nbd/client.h>
#include <cloud/blockstore/libs/nbd/client_handler.h>
#include <cloud/blockstore/libs/nbd/error_handler.h>
#include <cloud/blockstore/libs/nbd/server.h>
#include <cloud/blockstore/libs/nbd/server_handler.h>
#include <cloud/blockstore/libs/service/context.h>
#include <cloud/blockstore/libs/service/device_handler.h>
#include <cloud/blockstore/libs/service/service_null.h>
#include <cloud/blockstore/libs/service/storage.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>
#include <cloud/storage/core/libs/diagnostics/monitoring.h>

#include <util/datetime/base.h>

namespace NCloud::NBlockStore::NBD {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

static const TDuration WaitTimeout = TDuration::Seconds(10);

TNetworkAddress CreateListenAddress(const TOptions& options)
{
    if (options.ListenUnixSocketPath) {
        return TNetworkAddress(TUnixSocketPath(options.ListenUnixSocketPath));
    } else if (options.ListenAddress) {
        return TNetworkAddress(options.ListenAddress, options.ListenPort);
    } else {
        return TNetworkAddress(options.ListenPort);
    }
}

////////////////////////////////////////////////////////////////////////////////

class TServiceWrapper final
    : public IStorage
{
private:
    const IBlockStorePtr Service;

public:
    TServiceWrapper(IBlockStorePtr service)
        : Service(std::move(service))
    {}

    TFuture<NProto::TZeroBlocksResponse> ZeroBlocks(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TZeroBlocksRequest> request) override
    {
        return Service->ZeroBlocks(
            std::move(callContext),
            std::move(request));
    }

    TFuture<NProto::TReadBlocksLocalResponse> ReadBlocksLocal(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TReadBlocksLocalRequest> request) override
    {
        return Service->ReadBlocksLocal(
            std::move(callContext),
            std::move(request));
    }

    TFuture<NProto::TWriteBlocksLocalResponse> WriteBlocksLocal(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TWriteBlocksLocalRequest> request) override
    {
        return Service->WriteBlocksLocal(
            std::move(callContext),
            std::move(request));
    }

    NThreading::TFuture<NProto::TError> EraseDevice(
        NProto::EDeviceEraseMethod method) override
    {
        Y_UNUSED(method);
        return MakeFuture(MakeError(E_NOT_IMPLEMENTED));
    }

    TStorageBuffer AllocateBuffer(size_t bytesCount) override
    {
        Y_UNUSED(bytesCount);
        return nullptr;
    }

    void ReportIOError() override
    {}
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TBootstrap::TBootstrap(TOptionsPtr options)
    : Options(std::move(options))
    , ListenAddress(CreateListenAddress(*Options))
{}

TBootstrap::~TBootstrap()
{}

void TBootstrap::Init()
{
    TLogSettings logSettings;
    logSettings.FiltrationLevel = Options->FiltrationLevel;

    Logging = CreateLoggingService("console", logSettings);
    Monitoring = CreateMonitoringServiceStub();

    NProto::TNullServiceConfig config;
    config.SetDiskBlockSize(Options->BlockSize);
    config.SetDiskBlocksCount(Options->BlocksCount);

    Service = CreateNullService(config);

    TStorageOptions options;
    options.BlockSize = Options->BlockSize;
    options.BlocksCount = Options->BlocksCount;

    ServerHandlerFactory = CreateServerHandlerFactory(
        CreateDefaultDeviceHandlerFactory(),
        Logging,
        std::make_shared<TServiceWrapper>(Service),
        CreateServerStatsStub(),
        CreateErrorHandlerStub(),
        options);

    TServerConfig serverConfig {
        .ThreadsCount = 1,  // there will be just one endpoint
        .LimiterEnabled = Options->LimiterEnabled,
        .MaxInFlightBytesPerThread = Options->MaxInFlightBytes,
        .Affinity = {}
    };

    Server = CreateServer(Logging, serverConfig);

    ClientHandler = CreateClientHandler(
        Logging,
        Options->StructuredReply);

    Client = CreateClient(
        Logging,
        2   // threadsCount
    );

    ClientEndpoint = Client->CreateEndpoint(
        ListenAddress,
        ClientHandler,
        Service);
}

void TBootstrap::Start()
{
    if (Logging) {
        Logging->Start();
    }

    if (Monitoring) {
        Monitoring->Start();
    }

    if (Service) {
        Service->Start();
    }

    if (Server) {
        Server->Start();

        Server->StartEndpoint(ListenAddress, ServerHandlerFactory)
            .Wait(WaitTimeout);
    }

    if (Client) {
        Client->Start();
    }

    if (ClientEndpoint) {
        ClientEndpoint->Start();
    }
}

void TBootstrap::Stop()
{
    if (ClientEndpoint) {
        ClientEndpoint->Stop();
    }

    if (Client) {
        Client->Stop();
    }

    if (Server) {
        Server->Stop();
    }

    if (Service) {
        Service->Stop();
    }

    if (Monitoring) {
        Monitoring->Stop();
    }

    if (Logging) {
        Logging->Stop();
    }
}

}   // namespace NCloud::NBlockStore::NBD
