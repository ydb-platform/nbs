#pragma once

#include <contrib/ydb/core/base/defs.h>

namespace NKikimr::NReplication::NTestHelpers {

IActor* CreateReplicationMockService(const TActorId& edge);

}
