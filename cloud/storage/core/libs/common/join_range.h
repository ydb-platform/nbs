#pragma once

#include <util/generic/string.h>
#include <util/string/builder.h>

namespace NCloud {

template <typename TFunc>
inline TString
JoinRangeWithTransform(auto beginIt, auto endIt, const TStringBuf delim, TFunc func)
{
    TStringBuilder builder;
    for (auto it = beginIt; it != endIt; ++it) {
        if (it != beginIt) {
            builder << delim;
        }
        func(builder, *it);
    }
    return builder;
}

}   // namespace NCloud
