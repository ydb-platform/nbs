#pragma once

// FIXME: fix includes & remove
#include <cloud/storage/core/libs/actors/public.h>

#include <util/generic/ptr.h>

#include <memory>

namespace NKikimr {

////////////////////////////////////////////////////////////////////////////////

class TTabletStorageInfo;
using TTabletStorageInfoPtr = TIntrusivePtr<TTabletStorageInfo>;

class TTabletSetupInfo;
using TTabletSetupInfoPtr = TIntrusivePtr<TTabletSetupInfo>;

}   // namespace NKikimr

namespace NKikimrConfig {

////////////////////////////////////////////////////////////////////////////////

class TAppConfig;
using TAppConfigPtr = std::shared_ptr<TAppConfig>;

}   // namespace NKikimrConfig

namespace NCloud::NStorage {

////////////////////////////////////////////////////////////////////////////////

struct TOptionsYdbBase;
using TOptionsYdbBasePtr = std::shared_ptr<TOptionsYdbBase>;

class TLoggingProxy;
using TLoggingProxyPtr = std::shared_ptr<TLoggingProxy>;

class TMonitoringProxy;
using TMonitoringProxyPtr = std::shared_ptr<TMonitoringProxy>;

}   // namespace NCloud::NStorage
