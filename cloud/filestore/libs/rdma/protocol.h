#pragma once

#include <cloud/storage/core/libs/rdma/iface/public.h>

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

struct TTabletRdmaProtocol
{
    enum EMessageType
    {
        TabletRequest = 1,
        TabletResponse = 2,
    };

    static NCloud::NStorage::NRdma::TProtoMessageSerializer* Serializer();
};

}   // namespace NCloud::NFileStore::NStorage
