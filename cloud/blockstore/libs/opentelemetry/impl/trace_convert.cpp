#include "trace_convert.h"

#include "helpers.h"

#include <contrib/libs/opentelemetry-proto/opentelemetry/proto/common/v1/common.pb.h>
#include <contrib/libs/opentelemetry-proto/opentelemetry/proto/trace/v1/trace.pb.h>

#include <util/generic/hash.h>
#include <util/generic/vector.h>

#include <algorithm>
#include <random>

namespace NCloud::NBlockStore {

using namespace opentelemetry::proto::trace::v1;
using namespace opentelemetry::proto::common::v1;

namespace {

////////////////////////////////////////////////////////////////////////////////

AnyValue ConvertToOpenTelemtry(
    const NLWTrace::TParam& param,
    NLWTrace::EParamTypePb type)
{
    AnyValue value;
    switch (type) {
        case NLWTrace::PT_I64:
            value.Setint_value(param.Get<i64>());
            break;
        case NLWTrace::PT_Ui64:
            value.Setint_value(param.Get<ui64>());
            break;
        case NLWTrace::PT_Double:
            value.Setdouble_value(param.Get<double>());
            break;
        case NLWTrace::PT_Str:
            value.Setstring_value(param.Get<TString>());
            break;
        case NLWTrace::PT_Symbol:
            value.Setstring_value(*param
                                       .Get<typename NLWTrace::TParamTraits<
                                           NLWTrace::TSymbol>::TStoreType>()
                                       .Str);
            break;
        case NLWTrace::PT_Check:
            value.Setint_value(param
                                   .Get<typename NLWTrace::TParamTraits<
                                       NLWTrace::TCheck>::TStoreType>()
                                   .Value);
            break;
        default:
            value.Setstring_value("unknown type");
            break;
    }

    return value;
}

struct TReferenceTimeCycle
{
    TInstant ReferenceTimeStamp;
    ui64 ReferenceTimeCycle = 0;
};

TInstant GetTimestampFromCycle(
    TReferenceTimeCycle referenceTimeCycle,
    ui64 timeCycles)
{
    if (referenceTimeCycle.ReferenceTimeCycle >= timeCycles) {
        return referenceTimeCycle.ReferenceTimeStamp -
               CyclesToDurationSafe(
                   referenceTimeCycle.ReferenceTimeCycle - timeCycles);
    }

    return referenceTimeCycle.ReferenceTimeStamp +
           CyclesToDurationSafe(
               timeCycles - referenceTimeCycle.ReferenceTimeCycle);
}

ui64 GetTimestampInNanoSeconds(
    TReferenceTimeCycle referenceTimeCycle,
    ui64 timeCycles)
{
    return GetTimestampFromCycle(referenceTimeCycle, timeCycles).NanoSeconds();
}

Span_Event ProcessRegularItem(
    const NLWTrace::TTrackLog::TItem& item,
    TReferenceTimeCycle referenceTimeCycle)
{
    Span_Event event;
    event.Setname(item.Probe->Event.Name);
    event.Settime_unix_nano(
        GetTimestampInNanoSeconds(referenceTimeCycle, item.TimestampCycles));

    Y_ABORT_UNLESS(
        item.SavedParamsCount <= LWTRACE_MAX_PARAMS,
        "Too many params");
    for (size_t paramIdx = 0; paramIdx < item.SavedParamsCount; ++paramIdx) {
        KeyValue kv;
        kv.Setkey(item.Probe->Event.Signature.ParamNames[paramIdx]);

        auto type = NLWTrace::ParamTypeToProtobuf(
            item.Probe->Event.Signature.ParamTypes[paramIdx]);
        const auto& param = item.Params.Param[paramIdx];
        *kv.Mutablevalue() = std::move(ConvertToOpenTelemtry(param, type));

        auto* attributes = event.Mutableattributes();
        attributes->Add(std::move(kv));
    }

    return event;
}

struct TLogItemsIterationContext
{
    ui64 Idx = 0;
    ui64 Count = 0;

    operator bool() const
    {
        return Idx < Count;
    }

    bool Next()
    {
        Idx += 1;
        return Idx < Count;
    }

    // Construct iteration context that will iterate subSpanCount elements from
    // Idx + 1. Skips next subSpanCount elements from idx.
    TLogItemsIterationContext ConstructSubSpanIterationContext(
        ui64 subSpanCount)
    {
        Idx += subSpanCount;
        return {.Idx = Idx + 1, .Count = Min(Idx + 1 + subSpanCount, Count)};
    }
};

template <typename T>
T FindFirst(
    const NLWTrace::TTrackLog& tl,
    const char* fieldName,
    TLogItemsIterationContext iterationContext)
{
    while (iterationContext) {
        const auto& item = tl.Items[iterationContext.Idx];
        for (size_t paramIdx = 0; paramIdx < item.SavedParamsCount; ++paramIdx)
        {
            if (strcmp(
                    item.Probe->Event.Signature.ParamNames[paramIdx],
                    fieldName) == 0)
            {
                return item.Params.Param[paramIdx].Get<T>();
            }
        }
        iterationContext.Next();
    }

    return {};
}

ui64 FindRequestId(
    const NLWTrace::TTrackLog& tl,
    TLogItemsIterationContext iterationContext)
{
    return FindFirst<ui64>(tl, "requestId", iterationContext);
}

TString FindOperationName(
    const NLWTrace::TTrackLog& tl,
    TLogItemsIterationContext iterationContext)
{
    return FindFirst<TString>(tl, "requestType", iterationContext);
}

class TSpanTree
{
private:
    TVector<Span> Spans;
    ui64 TraceId;
    ui64 RootSpanId;

public:
    TSpanTree(ui64 traceId, ui64 spanId)
        : TraceId(traceId)
        , RootSpanId(spanId)
    {
        Spans.emplace_back();
        GetParentSpan().Setspan_id(ToHexString8(RootSpanId));
        GetParentSpan().Settrace_id(ToHexString16(TraceId));
    }

    void AddSpanSubTree(TSpanTree subtree)
    {
        subtree.GetParentSpan().Setparent_span_id(ToHexString8(RootSpanId));
        std::ranges::move(subtree.Spans, std::back_inserter(Spans));
    }

    [[nodiscard]] Span& GetParentSpan()
    {
        return Spans.front();
    }

    [[nodiscard]] const Span& GetParentSpan() const
    {
        return Spans.front();
    }

    static TVector<Span> ExtractSpans(TSpanTree&& spanTree)
    {
        return std::move(spanTree.Spans);
    }
};

TSpanTree ProcessItemsToSpanTree(
    const NLWTrace::TTrackLog& tl,
    ui64 traceId,
    ui64 currentSpanId,
    TLogItemsIterationContext iterationContext,
    TReferenceTimeCycle referenceTimeCycle)
{
    TSpanTree spanTree(traceId, currentSpanId);

    THashMap<ui64, ui64> spanIdToStartTime;

    ui64 startTimeCycles = tl.Items[iterationContext.Idx].TimestampCycles;
    ui64 endTimeCycles = 0;

    auto operationName = FindOperationName(tl, iterationContext);
    spanTree.GetParentSpan().Setname(operationName);

    while (iterationContext) {
        Y_DEFER
        {
            iterationContext.Next();
        };

        const auto& item = tl.Items[iterationContext.Idx];
        const auto& name = item.Probe->Event.Name;
        auto timeCycles = item.TimestampCycles;

        endTimeCycles = Max(endTimeCycles, timeCycles);
        auto curTimeStampNano =
            GetTimestampInNanoSeconds(referenceTimeCycle, timeCycles);

        if (strcmp(name, "Fork") == 0) {
            const auto childSpanId = item.Params.Param[0].Get<ui64>();
            spanIdToStartTime[childSpanId] = curTimeStampNano;
            continue;
        }

        if (strcmp(name, "Join") == 0) {
            const auto childSpanId = item.Params.Param[0].Get<ui64>();
            const auto itemsCount = item.Params.Param[1].Get<ui64>();

            // recursively process child spans
            auto subSpans = ProcessItemsToSpanTree(
                tl,
                traceId,
                childSpanId,
                iterationContext.ConstructSubSpanIterationContext(itemsCount),
                referenceTimeCycle);

            subSpans.GetParentSpan().Setstart_time_unix_nano(
                spanIdToStartTime[childSpanId]);
            subSpans.GetParentSpan().Setend_time_unix_nano(curTimeStampNano);

            spanTree.AddSpanSubTree(std::move(subSpans));
            continue;
        }

        *(spanTree.GetParentSpan()).Addevents() =
            ProcessRegularItem(item, referenceTimeCycle);
    }

    spanTree.GetParentSpan().Setstart_time_unix_nano(
        GetTimestampInNanoSeconds(referenceTimeCycle, startTimeCycles));
    spanTree.GetParentSpan().Setend_time_unix_nano(
        GetTimestampInNanoSeconds(referenceTimeCycle, endTimeCycles));

    return spanTree;
}

TReferenceTimeCycle GetReferenceTimeCycle(const NLWTrace::TTrackLog& tl)
{
    ui64 maxTimeCycle = 0;
    for (size_t itemIdx = 0; itemIdx < tl.Items.size(); ++itemIdx) {
        maxTimeCycle = Max(maxTimeCycle, tl.Items[itemIdx].TimestampCycles);
    }

    return {
        .ReferenceTimeStamp = TInstant::Now(),
        .ReferenceTimeCycle = maxTimeCycle};
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TVector<Span> ConvertToOpenTelemetrySpans(const NLWTrace::TTrackLog& tl)
{
    auto traceId = FindRequestId(tl, {0, tl.Items.size()});
    if (!traceId) {
        std::random_device r;

        std::default_random_engine e(r());
        std::uniform_int_distribution<ui64> uniform_dist(0, UINT64_MAX);

        traceId = uniform_dist(e);
    }

    return TSpanTree::ExtractSpans(std::move(ProcessItemsToSpanTree(
        tl,
        traceId,                    // traceId
        0,                          // currentSpanId
        {0, tl.Items.size()},       // iterationContext
        GetReferenceTimeCycle(tl)   // referenceTimeCycle
        )));
}

}   // namespace NCloud::NBlockStore
