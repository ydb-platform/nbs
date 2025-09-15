#include "alloc.h"

#include "spdk.h"

#include <util/generic/singleton.h>
#include <util/system/yassert.h>

namespace NCloud::NFileStore::NSpdk {

namespace {

////////////////////////////////////////////////////////////////////////////////

struct THugePageAllocator final
    : public IAllocator
{
    TBlock Allocate(size_t len) override
    {
        auto p = spdk_malloc(
            len,
            4*1024,     // align
            nullptr,    // phys_addr
            SPDK_ENV_SOCKET_ID_ANY,
            SPDK_MALLOC_DMA);

        return { p, len };
    }

    void Release(const TBlock& block) override
    {
        spdk_free(block.Data);
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TSpdkBuffer Allocate(size_t bytesCount, bool zeroInit)
{
    void* p = (zeroInit ? spdk_zmalloc : spdk_malloc)(
        bytesCount,
        4*1024,     // align
        nullptr,    // phys_addr
        SPDK_ENV_SOCKET_ID_ANY,
        SPDK_MALLOC_DMA
    );

    Y_ABORT_UNLESS(p, "allocation failed! bytesCount: %zu", bytesCount);
    return { (char*)p, [] (auto* p) { spdk_free(p); }};
}

IAllocator* GetHugePageAllocator()
{
    return SingletonWithPriority<THugePageAllocator, 0>();
}

}   // namespace NCloud::NFileStore::NSpdk
