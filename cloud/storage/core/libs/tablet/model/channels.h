#pragma once

#include <util/random/random.h>

namespace NCloud {

////////////////////////////////////////////////////////////////////////////////

inline double Normalize(double x, double lo, double hi)
{
    if (x > hi) {
        return 1;
    }

    if (x < lo) {
        return 0;
    }

    return (x - lo) / (hi - lo);
}

inline bool CheckChannelFreeSpaceShare(
    double freeSpaceShare,
    double minFreeSpace,
    double freeSpaceThreshold)
{
    if (freeSpaceShare == 0.) {
        return true;
    }

    const auto fss = Normalize(
        freeSpaceShare,
        minFreeSpace,
        freeSpaceThreshold);

    return RandomNumber<double>() < fss;
}

}   // namespace NCloud
