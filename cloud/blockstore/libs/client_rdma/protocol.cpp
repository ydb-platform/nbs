#include "protocol.h"

#include <cloud/blockstore/libs/rdma/iface/protobuf.h>
#include <cloud/blockstore/public/api/protos/io.pb.h>
#include <cloud/blockstore/public/api/protos/ping.pb.h>

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

            RegisterProto<NProto::TPingRequest>(PingRequest);
            RegisterProto<NProto::TPingResponse>(PingResponse);
        }
    };

    return Singleton<TSerializer>();
}

}   // namespace NCloud::NBlockStore::NClient
