#include "rpc.h"

#include "spdk/util.h"

namespace NCloud::NFileStore::NFsdev {

TRpcFilestoreCreate::~TRpcFilestoreCreate()
{
    if (Name) {
        free(Name);
    }
}

bool TRpcFilestoreCreate::Decode(const struct spdk_json_val* params)
{
    static const struct spdk_json_object_decoder decoders[] = {
        {
            "name",
            offsetof(TRpcFilestoreCreate, Name),
            spdk_json_decode_string,
            false,   // not optional
        },
    };

    return spdk_json_decode_object(
               params,
               decoders,
               SPDK_COUNTOF(decoders),
               this) == 0;
}

TRpcFilestoreDelete::~TRpcFilestoreDelete()
{
    if (Name) {
        free(Name);
    }
}

bool TRpcFilestoreDelete::Decode(const struct spdk_json_val* params)
{
    static const struct spdk_json_object_decoder decoders[] = {
        {
            "name",
            offsetof(TRpcFilestoreDelete, Name),
            spdk_json_decode_string,
            false,   // not optional
        },
    };

    return spdk_json_decode_object(
               params,
               decoders,
               SPDK_COUNTOF(decoders),
               this) == 0;
}

}   // namespace NCloud::NFileStore::NFsdev
