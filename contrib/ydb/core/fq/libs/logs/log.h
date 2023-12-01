#pragma once

#include <contrib/ydb/core/protos/config.pb.h>

#include <contrib/ydb/library/actors/core/actor.h>

namespace NKikimr {

NActors::IActor* CreateYqlLogsUpdater(const NKikimrConfig::TLogConfig& logConfig);

NActors::TActorId MakeYqlLogsUpdaterId();

} /* namespace NKikimr */
