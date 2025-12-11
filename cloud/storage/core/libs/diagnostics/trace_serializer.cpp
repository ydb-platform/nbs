#include "trace_serializer.h"

#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <library/cpp/lwtrace/all.h>
#include <library/cpp/lwtrace/log_shuttle.h>
#include <library/cpp/lwtrace/mon/mon_lwtrace.h>
#include <library/cpp/lwtrace/signature.h>

#include <util/datetime/cputimer.h>
#include <util/generic/string.h>
#include <util/stream/str.h>
#include <util/string/join.h>

namespace NCloud {

using namespace NMonitoring;

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TTraceSerializer final: public ITraceSerializer
{
private:
    const ILoggingServicePtr Logging;
    const TString ComponentName;
    NLWTrace::TManager& LWManager;
    TLog Log;

    NLWTrace::TProbeMap Probes;

public:
    TTraceSerializer(
        ILoggingServicePtr logging,
        TString componentName,
        NLWTrace::TManager& lwManager)
        : Logging(std::move(logging))
        , ComponentName(std::move(componentName))
        , LWManager(lwManager)
    {}

    void Start() override
    {
        Log = Logging->CreateLog(ComponentName);

        Probes = LWManager.GetProbesMap();
    }

    void Stop() override
    {}

    void BuildTraceInfo(
        NProto::TTraceInfo& traceResponse,
        NLWTrace::TOrbit& orbit,
        ui64 startTime,
        ui64 endTime) override
    {
        traceResponse.SetRequestStartTime(
            NLWTrace::CyclesToEpochNanoseconds(startTime));
        traceResponse.SetRequestEndTime(
            NLWTrace::CyclesToEpochNanoseconds(endTime));
        LWManager.CreateTraceResponse(*traceResponse.MutableLWTrace(), orbit);
    }

    void HandleTraceInfo(
        const NProto::TTraceInfo& trace,
        NLWTrace::TOrbit& orbit,
        ui64 startTime,
        ui64 endTime) override
    {
        // do not try to deserialize trace if it was not filled in
        if (!trace.GetRequestStartTime()) {
            return;
        }

        auto res = AdjustRemoteInterval(
            {startTime, endTime},
            {NLWTrace::EpochNanosecondsToCycles(trace.GetRequestStartTime()),
             NLWTrace::EpochNanosecondsToCycles(trace.GetRequestEndTime())});

        auto result = LWManager.HandleTraceResponse(
            trace.GetLWTrace(),
            Probes,
            orbit,
            res.Shift,
            res.Scale);

        if (!result.IsSuccess) {
            TString probeSeq = JoinSeq(",", result.FailedEventNames);
            STORAGE_ERROR(
                "Failed to deserialize trace. Failed events: "
                << probeSeq.Quote());
        }
    }

    bool HandleTraceRequest(
        const NLWTrace::TTraceRequest& traceRequest,
        NLWTrace::TOrbit& orbit) override
    {
        return LWManager.HandleTraceRequest(traceRequest, orbit);
    }

    void BuildTraceRequest(
        NLWTrace::TTraceRequest& msg,
        NLWTrace::TOrbit& orbit) override
    {
        LWManager.CreateTraceRequest(msg, orbit);
    }

    bool IsTraced(NLWTrace::TOrbit& orbit) override
    {
        return LWManager.IsTraced(orbit);
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TTraceSerializerStub final: public ITraceSerializer
{
    void Start() override
    {}

    void Stop() override
    {}

    void BuildTraceInfo(
        NProto::TTraceInfo& msg,
        NLWTrace::TOrbit& orbit,
        ui64 startTime,
        ui64 endTime) override
    {
        Y_UNUSED(orbit);
        Y_UNUSED(msg);
        Y_UNUSED(startTime);
        Y_UNUSED(endTime);
    }

    void HandleTraceInfo(
        const NProto::TTraceInfo& msg,
        NLWTrace::TOrbit& orbit,
        ui64 startTime,
        ui64 endTime) override
    {
        Y_UNUSED(msg);
        Y_UNUSED(orbit);
        Y_UNUSED(startTime);
        Y_UNUSED(endTime);
    }

    bool HandleTraceRequest(
        const NLWTrace::TTraceRequest& traceRequest,
        NLWTrace::TOrbit& orbit) override
    {
        Y_UNUSED(traceRequest);
        Y_UNUSED(orbit);
        return false;
    }

    void BuildTraceRequest(
        NLWTrace::TTraceRequest& msg,
        NLWTrace::TOrbit& orbit) override
    {
        Y_UNUSED(msg);
        Y_UNUSED(orbit);
    }

    bool IsTraced(NLWTrace::TOrbit& orbit) override
    {
        Y_UNUSED(orbit);
        return false;
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

ITraceSerializerPtr CreateTraceSerializer(
    ILoggingServicePtr logging,
    TString componentName,
    NLWTrace::TManager& lwManager)
{
    return std::make_shared<TTraceSerializer>(
        std::move(logging),
        std::move(componentName),
        lwManager);
}

ITraceSerializerPtr CreateTraceSerializerStub()
{
    return std::make_shared<TTraceSerializerStub>();
}

TTraceIntervalParams AdjustRemoteInterval(
    const TTraceInterval& local,
    const TTraceInterval& remote)
{
    ui64 localDuration = local.EndTime - local.StartTime;
    ui64 remoteDuration = remote.EndTime - remote.StartTime;

    double scale = Min(double(localDuration) / remoteDuration, 1.0);
    double realRemoteDuration = remoteDuration * scale;

    ui64 networkTime = (localDuration - realRemoteDuration) / 2;
    i64 shift = local.StartTime - remote.StartTime + networkTime;

    return {shift, scale};
}

}   // namespace NCloud
