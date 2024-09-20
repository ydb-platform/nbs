#pragma once

#include "public.h"

#include <util/generic/noncopyable.h>
#include <library/cpp/deprecated/atomic/atomic.h>

namespace NCloud::NFileStore::NReplay {

////////////////////////////////////////////////////////////////////////////////

struct TAppContext : TNonCopyable
{
    TAtomic ShouldStop = 0;
    TAtomic ExitCode = 0;
};

}   // namespace NCloud::NFileStore::NReplay
