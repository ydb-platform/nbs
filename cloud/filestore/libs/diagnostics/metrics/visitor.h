#pragma once

#include "public.h"

#include "metric.h"

#include <util/datetime/base.h>
#include <util/stream/output.h>

namespace NCloud::NFileStore::NMetrics {

////////////////////////////////////////////////////////////////////////////////

struct IRegistryVisitor
{
    virtual ~IRegistryVisitor() = default;

    virtual void OnStreamBegin() = 0;
    virtual void OnStreamEnd() = 0;

    virtual void OnMetricBegin(
        TInstant time,
        EAggregationType aggrType,
        EMetricType metrType) = 0;
    virtual void OnMetricEnd() = 0;

    virtual void OnLabelsBegin() = 0;
    virtual void OnLabelsEnd() = 0;

    virtual void OnValue(i64 value) = 0;

    virtual void OnLabel(TStringBuf name, TStringBuf value) = 0;
};

////////////////////////////////////////////////////////////////////////////////

IRegistryVisitorPtr CreateRegistryVisitor(IOutputStream& out);

}   // namespace NCloud::NFileStore::NMetrics
