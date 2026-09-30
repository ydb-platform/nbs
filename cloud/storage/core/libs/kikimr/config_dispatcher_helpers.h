#pragma once

#include "node.h"

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/diagnostics/critical_events.h>

#include <cloud/storage/core/protos/config_dispatcher_settings.pb.h>

#include <contrib/ydb/core/config/init/init.h>

#include <util/datetime/base.h>
#include <util/generic/string.h>

namespace NCloud::NStorage {

////////////////////////////////////////////////////////////////////////////////

TRegisterDynamicNodeOptions::TNodeLabels GetLabels(
    const NCloud::NProto::TConfigDispatcherSettings& settings,
    const TString& tenantName,
    const TString& nodeType);

// Make a config kind available without changing other allow/deny rules.
void AllowConfigItem(
    ui32 kind,
    NKikimr::NConfig::TConfigsDispatcherInitInfo* info);

void SetupConfigDispatcher(
    const NProto::TConfigDispatcherSettings& settings,
    const TString& tenantName,
    const TString& nodeType,
    NKikimr::NConfig::TConfigsDispatcherInitInfo* config);

}   // namespace NCloud::NStorage
