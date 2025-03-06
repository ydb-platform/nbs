#pragma once

#include <cloud/blockstore/libs/rdma/iface/public.h>
#include <cloud/blockstore/libs/service/request.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

struct TBlockStoreServerProtocol
{

#define BLOCKSTORE_DEFINE_E_MESSAGE_TYPE(name, ...) \
        Ev##name##Request,                          \
        Ev##name##Response,                         \
                                                    \

    enum EMessageType
    {
        BLOCKSTORE_SERVICE(BLOCKSTORE_DEFINE_E_MESSAGE_TYPE)
    };

#undef BLOCKSTORE_DEFINE_E_MESSAGE_TYPE

    static NRdma::TProtoMessageSerializer* Serializer();
};

}   // namespace NCloud::NBlockStore::NStorage
