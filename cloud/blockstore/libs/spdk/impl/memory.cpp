#include "memory.h"

#include "log.h"
#include "spdk.h"

namespace NCloud::NBlockStore::NSpdk {

namespace {

SPDK_LOG_REGISTER_COMPONENT(memory);

////////////////////////////////////////////////////////////////////////////////

int MapMemory(void* addr, size_t len, void* priv)
{
    Y_UNUSED(priv);

    int ret = spdk_mem_register(addr, len);
    if (ret) {
        SPDK_ERRLOG("unable to register memory region %p-%p: %s", addr,
            static_cast<char*>(addr) + len, strerror(-ret));
        return ret;
    }
    SPDK_INFOLOG(memory, "register memory region %p-%p", addr,
        static_cast<char*>(addr) + len);

    return ret;
}

int UnmapMemory(void* addr, size_t len, void* priv)
{
    Y_UNUSED(priv);

    int ret = spdk_mem_unregister(addr, len);
    if (ret) {
        SPDK_ERRLOG("unable to unregister memory region %p-%p: %s", addr,
            static_cast<char*>(addr) + len, strerror(-ret));
    } else {
        SPDK_INFOLOG(memory, "unregister memory region %p-%p", addr,
            static_cast<char*>(addr) + len);
    }

    return ret;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

NVhost::TVhostCallbacks VhostCallbacks()
{
    return NVhost::TVhostCallbacks {
        MapMemory,
        UnmapMemory,
    };
}

}   // namespace NCloud::NBlockStore::NSpdk
