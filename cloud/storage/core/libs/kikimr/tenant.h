#pragma once

#include "public.h"

#include <util/system/types.h>

namespace NKikimr {
    struct TAppData;
    struct TLocalConfig;
};

namespace NActors {
    struct TActorContext;
};

namespace NCloud::NStorage {

////////////////////////////////////////////////////////////////////////////////

ui64 GetHiveTabletId(const NActors::TActorContext& ctx);

void ConfigureTenantSystemTablets(
    const NKikimr::TAppData& appData,
    NKikimr::TLocalConfig& config,
    bool allowAdditionalSystemTablets);

}   // namespace NCloud::NStorage
