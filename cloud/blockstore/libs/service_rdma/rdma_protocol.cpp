#include "rdma_protocol.h"

#include <cloud/blockstore/libs/rdma/iface/protobuf.h>
#include <cloud/blockstore/public/api/protos/io.pb.h>
#include <cloud/blockstore/public/api/protos/mount.pb.h>
#include <cloud/blockstore/public/api/protos/ping.pb.h>

#include <util/generic/singleton.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

NRdma::TProtoMessageSerializer* TBlockStoreServerProtocol::Serializer()
{
    struct TSerializer: NRdma::TProtoMessageSerializer
    {
        TSerializer()
        {
            RegisterProto<NProto::TReadBlocksRequest>(EvReadBlocksRequest);
            RegisterProto<NProto::TReadBlocksResponse>(EvReadBlocksResponse);

            RegisterProto<NProto::TWriteBlocksRequest>(EvWriteBlocksRequest);
            RegisterProto<NProto::TWriteBlocksResponse>(EvWriteBlocksResponse);

            RegisterProto<NProto::TZeroBlocksRequest>(EvZeroBlocksRequest);
            RegisterProto<NProto::TZeroBlocksResponse>(EvZeroBlocksResponse);

            RegisterProto<NProto::TPingRequest>(EvPingRequest);
            RegisterProto<NProto::TPingResponse>(EvPingResponse);

            RegisterProto<NProto::TMountVolumeRequest>(EvMountVolumeRequest);
            RegisterProto<NProto::TMountVolumeResponse>(EvMountVolumeResponse);

            RegisterProto<NProto::TUnmountVolumeRequest>(EvUnmountVolumeRequest);
            RegisterProto<NProto::TUnmountVolumeResponse>(EvUnmountVolumeResponse);
        }
    };

    return Singleton<TSerializer>();
}

}   // namespace NCloud::NBlockStore::NStorage
