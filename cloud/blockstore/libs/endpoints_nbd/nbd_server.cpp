#include "nbd_server.h"

#include <cloud/blockstore/libs/client/session.h>
#include <cloud/blockstore/libs/diagnostics/server_stats.h>
#include <cloud/blockstore/libs/endpoints/endpoint_listener.h>
#include <cloud/blockstore/libs/nbd/error_handler.h>
#include <cloud/blockstore/libs/nbd/server.h>
#include <cloud/blockstore/libs/nbd/server_handler.h>
#include <cloud/blockstore/libs/service/device_handler.h>
#include <cloud/storage/core/libs/common/media.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

namespace NCloud::NBlockStore::NServer {

using namespace NMonitoring;
using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TNbdEndpointListener final
    : public IEndpointListener
{
private:
    const NBD::IServerPtr Server;
    const ILoggingServicePtr Logging;
    const IServerStatsPtr ServerStats;
    const NProto::TChecksumFlags ChecksumFlags;

public:
    TNbdEndpointListener(
            NBD::IServerPtr server,
            ILoggingServicePtr logging,
            IServerStatsPtr serverStats,
            NProto::TChecksumFlags checksumFlags)
        : Server(std::move(server))
        , Logging(std::move(logging))
        , ServerStats(std::move(serverStats))
        , ChecksumFlags(std::move(checksumFlags))
    {}

    TFuture<NProto::TError> StartEndpoint(
        const NProto::TStartEndpointRequest& request,
        const NProto::TVolume& volume,
        NClient::ISessionPtr session) override
    {
        NBD::TStorageOptions options;
        options.DiskId = request.GetDiskId();
        options.ClientId = request.GetClientId();
        options.BlockSize = volume.GetBlockSize();
        options.BlocksCount = volume.GetBlocksCount();
        options.UnalignedRequestsDisabled = request.GetUnalignedRequestsDisabled();
        options.SendMinBlockSize = request.GetSendNbdMinBlockSize();
        options.CheckBufferModificationDuringWriting =
            ChecksumFlags.GetCheckBufferModificationForMirrorDisk() &&
            IsReliableDiskRegistryMediaKind(volume.GetStorageMediaKind());
        options.IsReliableMediaKind =
            IsReliableMediaKind(volume.GetStorageMediaKind());
        options.MaxZeroBlocksSubRequestSize =
            request.GetMaxZeroBlocksSubRequestSize();

        auto requestFactory = CreateServerHandlerFactory(
            CreateDefaultDeviceHandlerFactory(),
            Logging,
            std::move(session),
            ServerStats,
            NBD::CreateErrorHandlerStub(),
            options);

        auto address = TNetworkAddress(
            TUnixSocketPath(request.GetUnixSocketPath()));

        return Server->StartEndpoint(address, std::move(requestFactory));
    }

    TFuture<NProto::TError> AlterEndpoint(
        const NProto::TStartEndpointRequest& request,
        const NProto::TVolume& volume,
        NClient::ISessionPtr session) override
    {
        Y_UNUSED(request, volume, session);

        return MakeFuture<NProto::TError>();
    }

    TFuture<NProto::TError> StopEndpoint(const TString& socketPath) override
    {
        auto address = TNetworkAddress(TUnixSocketPath(socketPath));

        return Server->StopEndpoint(address);
    }

    NProto::TError RefreshEndpoint(
        const TString& socketPath,
        const NProto::TVolume& volume) override
    {
        Y_UNUSED(socketPath);
        Y_UNUSED(volume);
        return {};
    }

    TFuture<NProto::TError> SwitchEndpoint(
        const NProto::TStartEndpointRequest& request,
        const NProto::TVolume& volume,
        NClient::ISessionPtr session) override
    {
        Y_UNUSED(request);
        Y_UNUSED(volume);
        Y_UNUSED(session);
        return MakeFuture(MakeError(E_NOT_IMPLEMENTED));
    }

};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IEndpointListenerPtr CreateNbdEndpointListener(
    NBD::IServerPtr server,
    ILoggingServicePtr logging,
    IServerStatsPtr serverStats,
    NProto::TChecksumFlags checksumFlags)
{
    return std::make_shared<TNbdEndpointListener>(
        std::move(server),
        std::move(logging),
        std::move(serverStats),
        std::move(checksumFlags));
}

}   // namespace NCloud::NBlockStore::NServer
