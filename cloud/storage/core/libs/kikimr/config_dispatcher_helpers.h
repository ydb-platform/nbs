
#pragma once

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/diagnostics/critical_events.h>

#include <cloud/storage/core/protos/ydb_config_dispatcher_settings.pb.h>

#include <contrib/ydb/core/config/init/init.h>

#include <util/datetime/base.h>
#include <util/generic/string.h>

namespace NCloud::NStorage {

////////////////////////////////////////////////////////////////////////////////

void SetupConfigDispatcher(
    const NProto::TYdbConfigDispatcherSettings& settings,
    NKikimr::NConfig::TConfigsDispatcherInitInfo* config);

}
