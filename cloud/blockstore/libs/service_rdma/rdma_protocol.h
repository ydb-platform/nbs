#pragma once

#include <cloud/blockstore/libs/rdma/iface/public.h>
#include <cloud/blockstore/libs/service/request.h>

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
    };

    static NRdma::TProtoMessageSerializer* Serializer();
};

}   // namespace NCloud::NBlockStore::NStorage
