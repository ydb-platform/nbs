#pragma once

#include "public.h"

#include <cloud/blockstore/libs/rdma/iface/public.h>

namespace NCloud::NBlockStore::NClient {

////////////////////////////////////////////////////////////////////////////////

struct TBlockStoreProtocol
{
    enum EMessageType
    {
        ReadBlocksRequest = 1,
        ReadBlocksResponse = 2,

        WriteBlocksRequest = 3,
        WriteBlocksResponse = 4,

        ZeroBlocksRequest = 5,
        ZeroBlocksResponse = 6,

        PingRequest = 7,
        PingResponse = 8,

        EvMountVolumeRequest = 9,
        EvMountVolumeResponse = 10,

        EvUnmountVolumeRequest = 11,
        EvUnmountVolumeResponse = 12,
    };

    static NRdma::TProtoMessageSerializer* Serializer();
};

}   // namespace NCloud::NBlockStore::NClient
