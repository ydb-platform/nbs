#include "interval.h"

#include <cloud/storage/core/libs/common/format.h>

#include <util/string/builder.h>

using namespace NCloud;

TString ToString(const NCloud::TSizeInterval& interval)
{
    if (interval.Start + 1 == interval.End) {
        return FormatByteSize(interval.Start);
    }
    return TStringBuilder() << FormatByteSize(interval.Start) << "-"
                            << FormatByteSize(interval.End);
}
