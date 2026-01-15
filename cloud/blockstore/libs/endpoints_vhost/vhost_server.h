#pragma once

#include "public.h"

#include <cloud/blockstore/libs/endpoints/public.h>
#include <cloud/blockstore/libs/vhost/public.h>
#include <cloud/blockstore/config/server.pb.h>
#include <cloud/blockstore/public/api/protos/volume.pb.h>

namespace NCloud::NBlockStore::NServer {

////////////////////////////////////////////////////////////////////////////////

bool ShouldEnableVhostDiscardForVolume(
    bool vhostDiscardEnabled,
    const NProto::TVolume& volume);

IEndpointListenerPtr CreateVhostEndpointListener(
    NVhost::IServerPtr server,
    const NProto::TChecksumFlags& checksumFlags,
    bool vhostDiscardEnabled,
    ui32 maxZeroBlocksSubRequestSize,
    ui32 optimalIoSize);

}   // namespace NCloud::NBlockStore::NServer
