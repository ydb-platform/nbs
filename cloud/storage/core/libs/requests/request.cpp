#include "request.h"

#include <library/cpp/actors/prof/tag.h>

#include <util/datetime/cputimer.h>

namespace NCloud::NStorage::NRequests {

////////////////////////////////////////////////////////////////////////////////

void* TRequestHandlerBase::AcquireCompletionTag()
{
    AtomicIncrement(RefCount);
    return this;
}

bool TRequestHandlerBase::ReleaseCompletionTag()
{
    return AtomicDecrement(RefCount) == 0;
}

}   // namespace NCloud::NStorage::NRequests
