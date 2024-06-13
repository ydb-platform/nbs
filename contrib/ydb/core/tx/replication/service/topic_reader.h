#pragma once

#include <contrib/ydb/core/base/defs.h>

namespace NYdb::NTopic {
    struct TReadSessionSettings;
}

namespace NKikimr::NReplication::NService {

IActor* CreateRemoteTopicReader(const TActorId& ydbProxy, const NYdb::NTopic::TReadSessionSettings& opts);

}
