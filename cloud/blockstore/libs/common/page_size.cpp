#include "page_size.h"

#include <unistd.h>

namespace NCloud::NBlockStore {

const size_t TPageSize::Value = TPageSize::Init();

size_t TPageSize::Init()
{
    const long result = sysconf(_SC_PAGESIZE);
    if (result <= 0) {
        return 4096;
    }
    return result;
}

size_t TPageSize::GetPageSize()
{
    return Value;
}

}   // namespace NCloud::NBlockStore
