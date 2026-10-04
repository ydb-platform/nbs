#include "hive_proxy.h"

#include <contrib/ydb/library/actors/core/actor.h>

namespace NCloud::NStorage {

using namespace NActors;

////////////////////////////////////////////////////////////////////////////////

TActorId MakeHiveProxyServiceId()
{
    return TActorId(0, "sto-hproxy");
}

void UpdateTabletBootInfoBackup(
    const TActorContext& ctx,
    NKikimr::TTabletStorageInfoPtr storageInfo,
    ui32 generation)
{
    ctx.Send(
        MakeHiveProxyServiceId(),
        new TEvHiveProxy::TEvUpdateTabletBootInfoBackup(
            std::move(storageInfo),
            generation));
}

}   // namespace NCloud::NStorage
