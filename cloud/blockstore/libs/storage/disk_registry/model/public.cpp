#include "public.h"

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

NProto::EStoragePoolKind ToStoragePoolKind(NProto::EDevicePoolKind kind)
{
    switch (kind) {
        case NProto::DEVICE_POOL_KIND_DEFAULT:
            return NProto::STORAGE_POOL_KIND_DEFAULT;

        case NProto::DEVICE_POOL_KIND_GLOBAL:
            return NProto::STORAGE_POOL_KIND_GLOBAL;

        case NProto::DEVICE_POOL_KIND_LOCAL:
            return NProto::STORAGE_POOL_KIND_LOCAL;

        default:
            Y_ABORT("unexpected device pool kind %d", kind);
    }
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NCloud::NBlockStore::NStorage
