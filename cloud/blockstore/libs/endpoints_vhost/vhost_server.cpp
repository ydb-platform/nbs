#include "vhost_server.h"

#include <cloud/blockstore/libs/client/session.h>
#include <cloud/blockstore/libs/common/constants.h>
#include <cloud/blockstore/libs/endpoints/endpoint_listener.h>
#include <cloud/blockstore/libs/service/request_helpers.h>
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
    const TVhostEndpointThreadCounts ThreadCounts;
    const bool VhostDiscardEnabled;
    const bool VhostWriteZeroesEnabled;
    const bool DropDiscardRequests;
    const ui32 MaxZeroBlocksSubRequestSize;
    const ui32 OptimalIoSize;

public:
    TVhostEndpointListener(
            NVhost::IServerPtr server,
            NProto::TChecksumFlags checksumFlags,
            const TVhostEndpointThreadCounts& threadCounts,
            bool vhostDiscardEnabled,
            bool vhostWriteZeroesEnabled,
            bool dropDiscardRequests,
            ui32 maxZeroBlocksSubRequestSize,
            ui32 optimalIoSize)
        : Server(std::move(server))
        , ChecksumFlags(std::move(checksumFlags))
        , ThreadCounts(threadCounts)
        , VhostDiscardEnabled(vhostDiscardEnabled)
        , VhostWriteZeroesEnabled(vhostWriteZeroesEnabled)
        , DropDiscardRequests(dropDiscardRequests)
        , MaxZeroBlocksSubRequestSize(maxZeroBlocksSubRequestSize)
        , OptimalIoSize(optimalIoSize)
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
        options.VhostQueuesCount = Max<ui32>(1, request.GetVhostQueuesCount());
        options.ThreadCount = GetVhostEndpointThreadCount(
            ThreadCounts,
            volume.GetStorageMediaKind());
        options.UnalignedRequestsDisabled = request.GetUnalignedRequestsDisabled();
        options.StorageMediaKind = volume.GetStorageMediaKind();
        options.DiscardEnabled =
            ShouldEnableVhostDiscardForVolume(VhostDiscardEnabled, volume);
        options.WriteZeroesEnabled =
            VhostWriteZeroesEnabled && !IsDiskRegistryMediaKind(volume.GetStorageMediaKind());
        options.DropDiscardRequests =
            ShouldDropDiscardRequestsForVolume(DropDiscardRequests, volume);
        options.MaxZeroBlocksSubRequestSize = MaxZeroBlocksSubRequestSize;
        options.OptimalIoSize = OptimalIoSize;
        options.ReadOnly = !IsReadWriteMode(request.GetVolumeAccessMode());

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

bool ShouldEnableVhostDiscardForVolume(
    bool vhostDiscardEnabled,
    const NProto::TVolume& volume)
{
    return (vhostDiscardEnabled || volume.GetVhostDiscardEnabled()) &&
           !IsDiskRegistryMediaKind(volume.GetStorageMediaKind());
}

bool ShouldDropDiscardRequestsForVolume(
    bool dropDiscardRequests,
    const NProto::TVolume& volume)
{
    // It is not safe to use ZeroBlocks as the implementation of discard
    // for disk registry based disks.
    return dropDiscardRequests ||
           volume.GetTags().contains(DropDiscardRequestsTagName) ||
           IsDiskRegistryMediaKind(volume.GetStorageMediaKind());
}

ui32 GetVhostEndpointThreadCount(
    const TVhostEndpointThreadCounts& threadCounts,
    NCloud::NProto::EStorageMediaKind mediaKind)
{
    switch (mediaKind) {
        case NCloud::NProto::STORAGE_MEDIA_SSD:
            return threadCounts.SSD;
        case NCloud::NProto::STORAGE_MEDIA_DEFAULT:
        case NCloud::NProto::STORAGE_MEDIA_HYBRID:
        case NCloud::NProto::STORAGE_MEDIA_HDD:
            return threadCounts.HDD;
        case NCloud::NProto::STORAGE_MEDIA_SSD_NONREPLICATED:
        case NCloud::NProto::STORAGE_MEDIA_HDD_NONREPLICATED:
            return threadCounts.NonReplicated;
        case NCloud::NProto::STORAGE_MEDIA_SSD_MIRROR2:
            return threadCounts.Mirror2;
        case NCloud::NProto::STORAGE_MEDIA_SSD_MIRROR3:
            return threadCounts.Mirror3;
        default:
            return 0;
    }
}

IEndpointListenerPtr CreateVhostEndpointListener(
    NVhost::IServerPtr server,
    const NProto::TChecksumFlags& checksumFlags,
    const TVhostEndpointThreadCounts& threadCounts,
    bool vhostDiscardEnabled,
    bool vhostWriteZeroesEnabled,
    bool dropDiscardRequests,
    ui32 maxZeroBlocksSubRequestSize,
    ui32 optimalIoSize)
{
    return std::make_shared<TVhostEndpointListener>(
        std::move(server),
        checksumFlags,
        threadCounts,
        vhostDiscardEnabled,
        vhostWriteZeroesEnabled,
        dropDiscardRequests,
        maxZeroBlocksSubRequestSize,
        optimalIoSize);
}

}   // namespace NCloud::NBlockStore::NServer
