#pragma once

#include <cloud/blockstore/libs/service/request.h>

#include <cloud/storage/core/libs/rdma/iface/public.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

struct TBlockStoreServerProtocol
{
    enum EMessageType
    {
        EvReadBlocksRequest = 1,
        EvReadBlocksResponse = 2,

        EvWriteBlocksRequest = 3,
        EvWriteBlocksResponse = 4,

        EvZeroBlocksRequest = 5,
        EvZeroBlocksResponse = 6,

        EvPingRequest = 7,
        EvPingResponse = 8,

        EvMountVolumeRequest = 9,
        EvMountVolumeResponse = 10,

        EvUnmountVolumeRequest = 11,
        EvUnmountVolumeResponse = 12,
    };

    static NRdma::TProtoMessageSerializer* Serializer();
};

}   // namespace NCloud::NBlockStore::NStorage
