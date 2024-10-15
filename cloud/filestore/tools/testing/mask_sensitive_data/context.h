#pragma once

#include <library/cpp/deprecated/atomic/atomic.h>

#include <util/generic/noncopyable.h>

namespace NCloud::NFileStore::NMaskSensitiveData {

////////////////////////////////////////////////////////////////////////////////

struct TAppContext: TNonCopyable
{
    TAtomic ShouldStop = 0;
    TAtomic ExitCode = 0;
};

}   // namespace NCloud::NFileStore::NMaskSensitiveData
