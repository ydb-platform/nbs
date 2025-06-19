#pragma once

#include "public.h"

#include <cloud/storage/core/libs/common/startable.h>
#include <cloud/storage/core/protos/trace.pb.h>

#include <library/cpp/lwtrace/all.h>

#include <util/datetime/base.h>
#include <util/generic/vector.h>

namespace NLWTrace {
    class TManager;
}   // namespace NLWTrace

namespace NCloud {

////////////////////////////////////////////////////////////////////////////////

struct TTraceInterval
{
    ui64 StartTime = 0;
    ui64 EndTime = 0;
};

////////////////////////////////////////////////////////////////////////////////

struct TTraceIntervalParams
{
    i64 Shift = 0;
    double Scale = 1.0;
};

////////////////////////////////////////////////////////////////////////////////

struct ITraceSerializer
    : public IStartable
    , public std::enable_shared_from_this<ITraceSerializer>
{
    virtual ~ITraceSerializer() = default;

    virtual void BuildTraceInfo(
        NProto::TTraceInfo& msg,
        NLWTrace::TOrbit& orbit,
        ui64 startTime,
        ui64 endTime) = 0;

    virtual void HandleTraceInfo(
        const NProto::TTraceInfo& msg,
        NLWTrace::TOrbit& orbit,
        ui64 startTime,
        ui64 endTime) = 0;

    virtual bool HandleTraceRequest(
        const NLWTrace::TTraceRequest& traceRequest,
        NLWTrace::TOrbit& orbit) = 0;

    virtual void BuildTraceRequest(
        NLWTrace::TTraceRequest& msg,
        NLWTrace::TOrbit& orbit) = 0;

    virtual bool IsTraced(NLWTrace::TOrbit& orbit) = 0;
};

////////////////////////////////////////////////////////////////////////////////

ITraceSerializerPtr CreateTraceSerializer(
    ILoggingServicePtr logging,
    TString componentName,
    NLWTrace::TManager& lwManager);

ITraceSerializerPtr CreateTraceSerializerStub();

TTraceIntervalParams AdjustRemoteInterval(
    const TTraceInterval& local,
    const TTraceInterval& remote);

}   // namespace NCloud
