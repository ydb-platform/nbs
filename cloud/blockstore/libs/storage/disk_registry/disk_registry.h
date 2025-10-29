#pragma once

#include "public.h"

#include <cloud/blockstore/libs/diagnostics/public.h>
#include <cloud/blockstore/libs/kikimr/public.h>
#include <cloud/blockstore/libs/logbroker/iface/public.h>
#include <cloud/blockstore/libs/notify/iface/public.h>
#include <cloud/blockstore/libs/storage/core/public.h>

#include <ydb/library/actors/core/actorid.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

NActors::IActorPtr CreateDiskRegistry(
    const NActors::TActorId& owner,
    ILoggingServicePtr logging,
    NKikimr::TTabletStorageInfoPtr storage,
    TStorageConfigPtr config,
    TDiagnosticsConfigPtr diagnosticsConfig,
    NLogbroker::IServicePtr logbrokerService,
    NNotify::IServicePtr notifyService);

}   // namespace NCloud::NBlockStore::NStorage
