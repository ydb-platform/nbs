#pragma once

#include <contrib/ydb/core/base/defs.h>

namespace NKikimr {
    struct TPathId;
}

namespace NKikimr::NReplication::NService {

IActor* CreateLocalTableWriter(const TPathId& tablePathId);

}
