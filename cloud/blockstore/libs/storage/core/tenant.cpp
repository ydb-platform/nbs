#include "tenant.h"

#include <cloud/storage/core/libs/kikimr/tenant.h>

#include <ydb/core/base/appdata.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

////////////////////////////////////////////////////////////////////////////////

ui64 GetHiveTabletId(const TStorageConfigPtr& config, const NActors::TActorContext& ctx)
{
    if (config->GetTenantHiveTabletId()) {
        return config->GetTenantHiveTabletId();
    }

    return NCloud::NStorage::GetHiveTabletId(ctx);
}

}   // namespace NCloud::NBlockStore::NStorage

