#pragma once

#include <util/generic/flags.h>

namespace NCloud {

enum class EHistogramCounterOption {
    ReportSingleCounter     = (1 << 0),
    ReportMultipleCounters  = (1 << 1),
};

Y_DECLARE_FLAGS(EHistogramCounterOptions, EHistogramCounterOption);
Y_DECLARE_OPERATORS_FOR_FLAGS(EHistogramCounterOptions);

}   // namespace NCloud

