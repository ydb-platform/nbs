#pragma once

#include <util/generic/fwd.h>
#include <util/system/yassert.h>

namespace NCloud {

////////////////////////////////////////////////////////////////////////////////

// represents [Start, End) interval in bytes.
struct TSizeInterval
{
    ui64 Start;
    ui64 End;

    TSizeInterval(ui64 start, ui64 end)
        : Start(start)
        , End(end)
    {
        Y_ABORT_UNLESS(start < end);
    }
};

}   // namespace NCloud

TString ToString(const NCloud::TSizeInterval& interval);
