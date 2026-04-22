#include "protocol.h"

#include <cloud/blockstore/tools/testing/rdma-test/protocol.pb.h>

#include <cloud/storage/core/libs/rdma/iface/protobuf.h>

#include <util/generic/singleton.h>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

NCloud::NStorage::NRdma::TProtoMessageSerializer*
TBlockStoreProtocol::Serializer()
{
    struct TSerializer: NCloud::NStorage::NRdma::TProtoMessageSerializer
    {
        TSerializer()
        {
            RegisterProto<NProto::TReadBlocksRequest>(ReadBlocksRequest);
            RegisterProto<NProto::TReadBlocksResponse>(ReadBlocksResponse);

            RegisterProto<NProto::TWriteBlocksRequest>(WriteBlocksRequest);
            RegisterProto<NProto::TWriteBlocksResponse>(WriteBlocksResponse);
        }
    };

    return Singleton<TSerializer>();
}

}   // namespace NCloud::NBlockStore
