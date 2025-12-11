#pragma once

#include "config.h"

namespace NActors {
struct TActorContext;
}   // namespace NActors

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

ui64 GetHiveTabletId(
    const TStorageConfigPtr& config,
    const NActors::TActorContext& ctx);

}   // namespace NCloud::NBlockStore::NStorage
