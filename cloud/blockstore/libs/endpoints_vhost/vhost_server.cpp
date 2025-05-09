#include "vhost_server.h"

#include <cloud/blockstore/libs/client/session.h>
#include <cloud/blockstore/libs/endpoints/endpoint_listener.h>
#include <cloud/blockstore/libs/vhost/server.h>
#include <cloud/storage/core/libs/common/media.h>

namespace NCloud::NBlockStore::NServer {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TVhostEndpointListener final
    : public IEndpointListener
{
private:
    const NVhost::IServerPtr Server;
    const NProto::TChecksumFlags ChecksumFlags;
    const bool VhostDiscardEnabled;
    const ui64 MaxZeroBlocksSubRequestSize;

public:
    TVhostEndpointListener(
            NVhost::IServerPtr server,
            NProto::TChecksumFlags checksumFlags,
            bool vhostDiscardEnabled,
            ui64 maxZeroBlocksSubRequestSize)
        : Server(std::move(server))
        , ChecksumFlags(std::move(checksumFlags))
        , VhostDiscardEnabled(vhostDiscardEnabled)
        , MaxZeroBlocksSubRequestSize(maxZeroBlocksSubRequestSize)
    {}

    TFuture<NProto::TError> StartEndpoint(
        const NProto::TStartEndpointRequest& request,
        const NProto::TVolume& volume,
        NClient::ISessionPtr session) override
    {
        NVhost::TStorageOptions options;
        options.DeviceName = request.GetDeviceName();
        options.DiskId = request.GetDiskId();
        options.ClientId = request.GetClientId();
        options.BlockSize = volume.GetBlockSize();
        options.BlocksCount = volume.GetBlocksCount();
        options.VhostQueuesCount = request.GetVhostQueuesCount();
        options.UnalignedRequestsDisabled = request.GetUnalignedRequestsDisabled();
        options.CheckBufferModificationDuringWriting =
            ChecksumFlags.GetCheckBufferModificationForMirrorDisk() &&
            IsReliableDiskRegistryMediaKind(volume.GetStorageMediaKind());
        options.IsReliableMediaKind =
            IsReliableMediaKind(volume.GetStorageMediaKind());
        options.DiscardEnabled =
            VhostDiscardEnabled &&
            !IsDiskRegistryMediaKind(volume.GetStorageMediaKind());
        options.MaxZeroBlocksSubRequestSize = MaxZeroBlocksSubRequestSize;

        return Server->StartEndpoint(
            request.GetUnixSocketPath(),
            std::move(session),
            options);
    }

    TFuture<NProto::TError> AlterEndpoint(
        const NProto::TStartEndpointRequest& request,
        const NProto::TVolume& volume,
        NClient::ISessionPtr session) override
    {
        Y_UNUSED(request, volume, session);

        return MakeFuture<NProto::TError>();
    }

    TFuture<NProto::TError> StopEndpoint(
        const TString& socketPath) override
    {
        return Server->StopEndpoint(socketPath);
    }

    NProto::TError RefreshEndpoint(
        const TString& socketPath,
        const NProto::TVolume& volume) override
    {
        return Server->UpdateEndpoint(socketPath, volume.GetBlocksCount());
    }

    TFuture<NProto::TError> SwitchEndpoint(
        const NProto::TStartEndpointRequest& request,
        const NProto::TVolume& volume,
        NClient::ISessionPtr session) override
    {
        Y_UNUSED(request);
        Y_UNUSED(volume);
        Y_UNUSED(session);
        return MakeFuture<NProto::TError>();
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IEndpointListenerPtr CreateVhostEndpointListener(
    NVhost::IServerPtr server,
    const NProto::TChecksumFlags& checksumFlags,
    bool vhostDiscardEnabled,
    ui64 maxZeroBlocksSubRequestSize)
{
    return std::make_shared<TVhostEndpointListener>(
        std::move(server),
        checksumFlags,
        vhostDiscardEnabled,
        maxZeroBlocksSubRequestSize);
}

}   // namespace NCloud::NBlockStore::NServer
