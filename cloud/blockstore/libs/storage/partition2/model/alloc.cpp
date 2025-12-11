#include "alloc.h"

#include <util/generic/singleton.h>

namespace NCloud::NBlockStore::NStorage::NPartition2 {

////////////////////////////////////////////////////////////////////////////////

TProfilingAllocator* GetAllocatorByTag(EAllocatorTag tag)
{
    static auto* r = Singleton<TProfilingAllocatorRegistry<EAllocatorTag>>();
    return r->GetAllocator(tag);
}

}   // namespace NCloud::NBlockStore::NStorage::NPartition2
