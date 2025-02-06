#include "vhost_server.h"

#include <cloud/blockstore/libs/client/session.h>
#include <cloud/blockstore/libs/endpoints/endpoint_listener.h>
#include <cloud/blockstore/libs/vhost/server.h>
#include <cloud/storage/core/libs/common/media.h>

namespace NCloud::NBlockStore::NServer {

using namespace NThreading;

namespace {

constexpr ui32 ProtoFlag(int value)
{
    return value ? 1 << (value - 1) : value;
}

constexpr bool HasFlag(ui32 flags, ui32 value)
{
    return flags & ProtoFlag(value);
}

////////////////////////////////////////////////////////////////////////////////

class TVhostEndpointListener final
    : public IEndpointListener
{
private:
    const NVhost::IServerPtr Server;
    const NProto::EChecksumFlags ChecksumFlags;

public:
    TVhostEndpointListener(
            NVhost::IServerPtr server,
            NProto::EChecksumFlags checksumFlags)
        : Server(std::move(server))
        , ChecksumFlags(checksumFlags)
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
            HasFlag(
                ChecksumFlags,
                NProto::CHECKSUM_FLAGS_CHECK_FOR_MIRROR) &&
            IsReliableDiskRegistryMediaKind(volume.GetStorageMediaKind());

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
    NProto::EChecksumFlags checksumFlags)
{
    return std::make_shared<TVhostEndpointListener>(
        std::move(server),
        checksumFlags);
}

}   // namespace NCloud::NBlockStore::NServer
