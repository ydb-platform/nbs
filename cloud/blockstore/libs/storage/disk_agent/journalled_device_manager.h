#pragma once

#include "public.h"

#include <cloud/blockstore/libs/storage/disk_agent/model/public.h>
#include <cloud/fastshard/journal/server/public.h>
#include <cloud/storage/core/libs/common/public.h>

#include <contrib/ydb/library/actors/core/actorid.h>

namespace NActors {
class TActorSystem;
}   // namespace NActors

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

NJournalled::IDeviceManagerPtr CreateDeviceManager(
    ITimerPtr timer,
    TDeviceClientPtr deviceClient,
    NActors::TActorSystem* actorSystem,
    const NActors::TActorId& diskAgentActorId);

}   // namespace NCloud::NBlockStore::NStorage
