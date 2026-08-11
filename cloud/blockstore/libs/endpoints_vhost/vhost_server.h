#pragma once

#include "public.h"

#include <cloud/blockstore/config/server.pb.h>
#include <cloud/blockstore/libs/endpoints/public.h>
#include <cloud/blockstore/libs/vhost/public.h>
#include <cloud/blockstore/public/api/protos/volume.pb.h>

#include <cloud/storage/core/protos/media.pb.h>

namespace NCloud::NBlockStore::NServer {

////////////////////////////////////////////////////////////////////////////////

// Number of vhost server threads used to serve a single endpoint, per media
// kind. Zero means a single thread.
struct TVhostEndpointThreadCounts
{
    ui32 SSD = 0;
    ui32 HDD = 0;
    ui32 NonReplicated = 0;
    ui32 Mirror2 = 0;
    ui32 Mirror3 = 0;
};

////////////////////////////////////////////////////////////////////////////////

bool ShouldEnableVhostDiscardForVolume(
    bool vhostDiscardEnabled,
    const NProto::TVolume& volume);

bool ShouldDropDiscardRequestsForVolume(
    bool dropDiscardRequests,
    const NProto::TVolume& volume);

ui32 GetVhostEndpointThreadCount(
    const TVhostEndpointThreadCounts& threadCounts,
    NCloud::NProto::EStorageMediaKind mediaKind);

IEndpointListenerPtr CreateVhostEndpointListener(
    NVhost::IServerPtr server,
    const NProto::TChecksumFlags& checksumFlags,
    const TVhostEndpointThreadCounts& threadCounts,
    bool vhostDiscardEnabled,
    bool vhostWriteZeroesEnabled,
    bool dropDiscardRequests,
    ui32 maxZeroBlocksSubRequestSize,
    ui32 optimalIoSize);

}   // namespace NCloud::NBlockStore::NServer
