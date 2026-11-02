#pragma once

#include <cloud/storage/core/libs/rdma/iface/public.h>

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

// Every proxied request travels in one envelope (TTabletRdmaRequest /
// TTabletRdmaResponse) carrying the target tablet and the event type, so one
// registration covers every method.
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
