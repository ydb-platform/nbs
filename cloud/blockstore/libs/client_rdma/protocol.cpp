#include "protocol.h"

#include <cloud/blockstore/libs/rdma/protobuf.h>
#include <cloud/blockstore/public/api/protos/io.pb.h>

#include <util/generic/singleton.h>

namespace NCloud::NBlockStore::NClient {

////////////////////////////////////////////////////////////////////////////////

NRdma::TProtoMessageSerializer* TBlockStoreProtocol::Serializer()
{
    struct TSerializer : NRdma::TProtoMessageSerializer
    {
        TSerializer()
        {
            RegisterProto<NProto::TReadBlocksRequest>(ReadBlocksRequest);
            RegisterProto<NProto::TReadBlocksResponse>(ReadBlocksResponse);

            RegisterProto<NProto::TWriteBlocksRequest>(WriteBlocksRequest);
            RegisterProto<NProto::TWriteBlocksResponse>(WriteBlocksResponse);

            RegisterProto<NProto::TZeroBlocksRequest>(ZeroBlocksRequest);
            RegisterProto<NProto::TZeroBlocksResponse>(ZeroBlocksResponse);
        }
    };

    return Singleton<TSerializer>();
}

}   // namespace NCloud::NBlockStore::NClient
