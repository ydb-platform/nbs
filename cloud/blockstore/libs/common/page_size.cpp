#include "page_size.h"

#include <unistd.h>

namespace NCloud::NBlockStore {

const size_t TPageSize::Value = TPageSize::Get();

size_t TPageSize::Get()
{
    const auto result = sysconf(_SC_PAGESIZE);
    if (result <= 0) {
        return 4096;
    }
    return result;
}

}   // namespace NCloud::NBlockStore::NRdma
