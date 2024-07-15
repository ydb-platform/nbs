#pragma once

#include <unordered_map>

#include "defs.h"

#include <contrib/ydb/library/pdisk_io/device_type.h>
#include <contrib/ydb/core/control/immediate_control_board_wrapper.h>

namespace NKikimr {

struct TCostMetricsParameters {
    TCostMetricsParameters(ui64 defaultBurstThresholdMs = 100);
    TControlWrapper BurstThresholdNs;
    TControlWrapper DiskTimeAvailableScale;
};

using TCostMetricsParametersByMedia = TStackVec<TCostMetricsParameters, 3>;

} // namespace NKikimr
