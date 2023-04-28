#include "alloc.h"

#include <util/generic/singleton.h>

namespace NCloud {

////////////////////////////////////////////////////////////////////////////////

TProfilingAllocator* TProfilingAllocator::Instance()
{
    return SingletonWithPriority<TProfilingAllocator, 0>();
}

}   // namespace NCloud
