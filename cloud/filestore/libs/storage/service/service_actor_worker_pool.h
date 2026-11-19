#pragma once

#include "service_state.h"
#include "worker_actor_pool.h"

#include <cloud/filestore/libs/diagnostics/trace_serializer.h>

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

class TReadDataActorPool final: public TWorkerActorPool
{
public:
    TReadDataActorPool(
        IProfileLogPtr profileLog,
        ITraceSerializerPtr traceSerializer,
        TInFlightRequestStoragePtr inFlightRequests);
};

}   // namespace NCloud::NFileStore::NStorage
