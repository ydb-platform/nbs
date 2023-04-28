#pragma once

#include <cloud/storage/core/libs/diagnostics/public.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

struct TRequestTraceInfo
{
    const bool IsTraced;
    const ui64 ReceiveTime;
    const ITraceSerializerPtr TraceSerializer;

    TRequestTraceInfo(
            bool isTraced,
            ui64 traceTs,
            ITraceSerializerPtr traceSerializer)
        : IsTraced(isTraced)
        , ReceiveTime(traceTs)
        , TraceSerializer(std::move(traceSerializer))
    {}
};

}   // namespace NCloud::NBlockStore::NStorage
