#include "rdma_protocol.h"

#include <cloud/blockstore/libs/rdma/iface/protobuf.h>
#include <cloud/blockstore/libs/storage/protos/disk.pb.h>

#include <util/generic/singleton.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

#define BLOCKSTORE_REGISTER_PROTO(name, ...)                      \
    RegisterProto<NProto::T##name##Request>(Ev##name##Request);   \
    RegisterProto<NProto::T##name##Response>(Ev##name##Response); \
    // BLOCKSTORE_REGISTER_PROTO

NRdma::TProtoMessageSerializer* TBlockStoreServerProtocol::Serializer()
{
    struct TSerializer: NRdma::TProtoMessageSerializer
    {
        TSerializer()
        {
            BLOCKSTORE_SERVICE(BLOCKSTORE_REGISTER_PROTO)
        }
    };

    return Singleton<TSerializer>();
}

#undef BLOCKSTORE_REGISTER_PROTO

}   // namespace NCloud::NBlockStore::NStorage
