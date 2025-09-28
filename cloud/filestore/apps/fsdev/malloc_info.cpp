#include <library/cpp/malloc/api/malloc.h>

namespace NMalloc {

////////////////////////////////////////////////////////////////////////////////

TMallocInfo MallocInfo()
{
    TMallocInfo r;
    r.Name = "system";
    return r;
}

}   // namespace NMalloc
