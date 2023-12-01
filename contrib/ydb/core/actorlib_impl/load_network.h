#pragma once

#include <contrib/ydb/library/actors/core/actorsystem.h>
#include <contrib/ydb/core/base/appdata.h>

namespace IC_Load {
    void InitializeService(NActors::TActorSystemSetup* setup,
                           const NKikimr::TAppData* appData,
                           ui32 totalNodesCount);
}
