#include "protocol.h"

#include <cloud/filestore/private/api/protos/tablet.pb.h>

#include <cloud/storage/core/libs/rdma/iface/protobuf.h>

#include <util/generic/singleton.h>

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

using NCloud::NStorage::NRdma::TProtoMessageSerializer;

TProtoMessageSerializer* TTabletRdmaProtocol::Serializer()
{
    struct TSerializer: TProtoMessageSerializer
    {
        TSerializer()
        {
            RegisterProto<NProtoPrivate::TTabletRdmaRequest>(TabletRequest);
            RegisterProto<NProtoPrivate::TTabletRdmaResponse>(TabletResponse);
        }
    };

    return Singleton<TSerializer>();
}

}   // namespace NCloud::NFileStore::NStorage
