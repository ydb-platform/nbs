#include "protocol.h"

#include <cloud/blockstore/public/api/protos/io.pb.h>
#include <cloud/blockstore/public/api/protos/mount.pb.h>
#include <cloud/blockstore/public/api/protos/ping.pb.h>

#include <cloud/storage/core/libs/rdma/iface/protobuf.h>

#include <util/generic/singleton.h>

namespace NCloud::NBlockStore::NClient {

////////////////////////////////////////////////////////////////////////////////

using NCloud::NStorage::NRdma::TProtoMessageSerializer;

TProtoMessageSerializer* TBlockStoreProtocol::Serializer()
{
    struct TSerializer: TProtoMessageSerializer
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

            RegisterProto<NProto::TMountVolumeRequest>(MountVolumeRequest);
            RegisterProto<NProto::TMountVolumeResponse>(MountVolumeResponse);

            RegisterProto<NProto::TUnmountVolumeRequest>(UnmountVolumeRequest);
            RegisterProto<NProto::TUnmountVolumeResponse>(
                UnmountVolumeResponse);
        }
    };

    return Singleton<TSerializer>();
}

}   // namespace NCloud::NBlockStore::NClient
