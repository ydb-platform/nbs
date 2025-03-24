#include "page_size.h"

#include <unistd.h>

namespace {

static size_t PageSizeInit()
{
    const long result = sysconf(_SC_PAGESIZE);
    if (result <= 0) {
        return 4096;
    }
    return result;
}

const size_t pageSizeValue = PageSizeInit();

}   // namespace

namespace NCloud::NBlockStore {

size_t GetPageSize()
{
    return pageSizeValue;
}

}   // namespace NCloud::NBlockStore
