#pragma once

#include <contrib/ydb/core/base/defs.h>

namespace NKikimr {
    struct TPathId;
}

namespace NKikimr::NBackup::NImpl {

IActor* CreateLocalTableWriter(const TPathId& tablePathId);

}
