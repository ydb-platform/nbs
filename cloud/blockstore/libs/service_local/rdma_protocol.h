#pragma once

#include "public.h"

#include <cloud/blockstore/libs/rdma/public.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

struct TBlockStoreProtocol
{
    enum EMessageType
    {
        ReadDeviceBlocksRequest = 1,
        ReadDeviceBlocksResponse = 2,

        WriteDeviceBlocksRequest = 3,
        WriteDeviceBlocksResponse = 4,

        ZeroDeviceBlocksRequest = 5,
        ZeroDeviceBlocksResponse = 6,

        ChecksumDeviceBlocksRequest = 7,
        ChecksumDeviceBlocksResponse = 8,
    };

    static NRdma::TProtoMessageSerializer* Serializer();
};

}   // namespace NCloud::NBlockStore::NStorage
