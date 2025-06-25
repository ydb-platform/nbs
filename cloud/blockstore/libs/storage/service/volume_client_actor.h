#pragma once

#include "public.h"

#include <cloud/blockstore/libs/endpoints/public.h>
#include <cloud/blockstore/libs/diagnostics/public.h>
#include <cloud/blockstore/libs/storage/core/config.h>

#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

NActors::IActorPtr CreateVolumeClient(
    TStorageConfigPtr config,
    ITraceSerializerPtr traceSerializer,
    NServer::IEndpointEventHandlerPtr endpointEventHandler,
    const NActors::TActorId& sessionActorId,
    TString sessionId,
    TString clientId,
    bool temporaryServer,
    TString diskId,
    ui64 tabletId);

}   // namespace NCloud::NBlockStore::NStorage
