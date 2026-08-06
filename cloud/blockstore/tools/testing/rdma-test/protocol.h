#pragma once

#include "private.h"

#include <cloud/storage/core/libs/rdma/iface/public.h>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

struct TBlockStoreProtocol
{
    enum EMessageType
    {
        ReadBlocksRequest = 1,
        ReadBlocksResponse = 2,

        WriteBlocksRequest = 3,
        WriteBlocksResponse = 4,
    };

    static NCloud::NStorage::NRdma::TProtoMessageSerializer* Serializer();
};

}   // namespace NCloud::NBlockStore
