#include "protocol.h"

#include <cloud/blockstore/libs/rdma/iface/protobuf.h>
#include <cloud/blockstore/tools/testing/rdma-test/protocol.pb.h>

#include <util/generic/singleton.h>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

NRdma::TProtoMessageSerializer* TBlockStoreProtocol::Serializer()
{
    struct TSerializer: NRdma::TProtoMessageSerializer
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
