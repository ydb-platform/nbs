#pragma once
#include "defs.h"
#include <contrib/ydb/core/quoter/public/quoter.h>

namespace NKikimr {

struct TQuoterServiceConfig {
    TDuration ScheduleTickSize = TDuration::MilliSeconds(1);
 };

IActor* CreateQuoterService(const TQuoterServiceConfig &config = TQuoterServiceConfig());

}
