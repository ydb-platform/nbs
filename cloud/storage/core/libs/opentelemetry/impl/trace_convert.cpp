#include "trace_convert.h"

#include "helpers.h"

#include <contrib/libs/opentelemetry-proto/opentelemetry/proto/common/v1/common.pb.h>
#include <contrib/libs/opentelemetry-proto/opentelemetry/proto/trace/v1/trace.pb.h>

#include <library/cpp/protobuf/util/pb_io.h>

#include <util/generic/hash.h>
#include <util/generic/hash_set.h>
#include <util/generic/vector.h>
#include <util/random/random.h>

#include <algorithm>
#include <random>

namespace NCloud {

using namespace opentelemetry::proto::trace::v1;
using namespace opentelemetry::proto::common::v1;

namespace {

////////////////////////////////////////////////////////////////////////////////

AnyValue ConvertToOpenTelemetry(
    const NLWTrace::TParam& param,
    NLWTrace::EParamTypePb type)
{
    AnyValue value;
    switch (type) {
        case NLWTrace::PT_I64:
            value.set_int_value(param.Get<i64>());
            break;
        case NLWTrace::PT_Ui64:
            value.set_int_value(param.Get<ui64>());
            break;
        case NLWTrace::PT_Double:
            value.set_double_value(param.Get<double>());
            break;
        case NLWTrace::PT_Str:
            value.set_string_value(param.Get<TString>());
            break;
        case NLWTrace::PT_Symbol:
            value.set_string_value(*param
                                        .Get<typename NLWTrace::TParamTraits<
                                            NLWTrace::TSymbol>::TStoreType>()
                                        .Str);
            break;
        case NLWTrace::PT_Check:
            value.set_int_value(param
                                    .Get<typename NLWTrace::TParamTraits<
                                        NLWTrace::TCheck>::TStoreType>()
                                    .Value);
            break;
        default:
            value.set_string_value("unknown type");
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
    event.set_name(item.Probe->Event.Name);
    event.set_time_unix_nano(
        GetTimestampInNanoSeconds(referenceTimeCycle, item.TimestampCycles));

    Y_ABORT_UNLESS(
        item.SavedParamsCount <= LWTRACE_MAX_PARAMS,
        "Too many params");
    for (size_t paramIdx = 0; paramIdx < item.SavedParamsCount; ++paramIdx) {
        KeyValue kv;
        kv.set_key(item.Probe->Event.Signature.ParamNames[paramIdx]);

        auto type = NLWTrace::ParamTypeToProtobuf(
            item.Probe->Event.Signature.ParamTypes[paramIdx]);
        const auto& param = item.Params.Param[paramIdx];
        *kv.mutable_value() = std::move(ConvertToOpenTelemetry(param, type));

        auto* attributes = event.mutable_attributes();
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
        TLogItemsIterationContext res{
            .Idx = Idx + 1,
            .Count = Min(Idx + 1 + subSpanCount, Count)};
        Idx += subSpanCount;
        return res;
    }
};

template <typename T>
T FindFirst(
    const NLWTrace::TTrackLog& tl,
    const char* fieldName,
    TLogItemsIterationContext iterationContext)
{
    for (; iterationContext; iterationContext.Next()) {
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
    const ui64 TraceId;
    const ui64 RootSpanId;

public:
    TSpanTree(ui64 traceId, ui64 spanId)
        : TraceId(traceId)
        , RootSpanId(spanId)
    {
        Spans.emplace_back();
        GetParentSpan().set_span_id(ToHexString8(RootSpanId));
        GetParentSpan().set_trace_id(ToHexString16(TraceId));
    }

    void AddSpanSubTree(TSpanTree subtree)
    {
        subtree.GetParentSpan().set_parent_span_id(ToHexString8(RootSpanId));
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

class TTraceConverter
{
private:
    const ui64 TraceId;
    const TReferenceTimeCycle ReferenceTimeCycle;
    const bool HasConflicts;

    ui32 NextSpanId = 0;

public:
    TTraceConverter(
            ui64 traceId,
            TReferenceTimeCycle referenceTimeCycle,
            bool hasConflicts)
        : TraceId(traceId)
        , ReferenceTimeCycle(referenceTimeCycle)
        , HasConflicts(hasConflicts)
    {}

    TSpanTree ProcessItemsToSpanTree(const NLWTrace::TTrackLog& tl)
    {
        return ProcessItemsToSpanTreeImpl(
            tl,
            TakeSpanId(),
            {0, tl.Items.size()}   // iterationContext
        );
    }

private:
    TSpanTree ProcessItemsToSpanTreeImpl(
        const NLWTrace::TTrackLog& tl,
        ui64 currentSpanId,
        TLogItemsIterationContext iterationContext)
    {
        TSpanTree spanTree(TraceId, currentSpanId);

        THashMap<ui64, ui64> spanIdToStartTime;

        ui64 startTimeCycles = tl.Items[iterationContext.Idx].TimestampCycles;
        ui64 endTimeCycles = 0;

        auto operationName = FindOperationName(tl, iterationContext);
        spanTree.GetParentSpan().Setname(operationName);

        for (; iterationContext; iterationContext.Next()) {
            const auto& item = tl.Items[iterationContext.Idx];
            const auto& name = item.Probe->Event.Name;
            auto timeCycles = item.TimestampCycles;

            endTimeCycles = Max(endTimeCycles, timeCycles);
            auto curTimeStampNano =
                GetTimestampInNanoSeconds(ReferenceTimeCycle, timeCycles);

            if (TStringBuf(name) == "Fork") {
                const auto childSpanId = item.Params.Param[0].Get<ui64>();
                spanIdToStartTime[childSpanId] = curTimeStampNano;
                continue;
            }

            if (TStringBuf(name) == "Join") {
                const auto childSpanId = HasConflicts
                                             ? TakeSpanId()
                                             : item.Params.Param[0].Get<ui64>();
                const auto itemsCount = item.Params.Param[1].Get<ui64>();

                // recursively process child spans
                auto subSpans = ProcessItemsToSpanTreeImpl(
                    tl,
                    childSpanId,
                    iterationContext.ConstructSubSpanIterationContext(
                        itemsCount));

                if (!HasConflicts) {
                    subSpans.GetParentSpan().set_start_time_unix_nano(
                        spanIdToStartTime[childSpanId]);
                }
                subSpans.GetParentSpan().set_end_time_unix_nano(
                    curTimeStampNano);

                spanTree.AddSpanSubTree(std::move(subSpans));
                continue;
            }

            *spanTree.GetParentSpan().Addevents() =
                ProcessRegularItem(item, ReferenceTimeCycle);
        }

        spanTree.GetParentSpan().set_start_time_unix_nano(
            GetTimestampInNanoSeconds(ReferenceTimeCycle, startTimeCycles));
        spanTree.GetParentSpan().set_end_time_unix_nano(
            GetTimestampInNanoSeconds(ReferenceTimeCycle, endTimeCycles));

        return spanTree;
    }

    ui32 TakeSpanId()
    {
        return NextSpanId++;
    }
};

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

struct TConflictDetecter
{
    THashSet<ui64> SpanIdsParents;
    THashSet<ui64> SpanIdsDeclaredAndNotJoinedYet;

    bool HasConflict(
        const NLWTrace::TTrackLog& tl,
        TLogItemsIterationContext iCtx,
        ui64 currentSpanId)
    {
        auto [_, inserted] = SpanIdsParents.insert(currentSpanId);
        if (!inserted) {
            return true;
        }

        for (; iCtx; iCtx.Next()) {
            const auto& item = tl.Items[iCtx.Idx];
            const auto& name = item.Probe->Event.Name;

            if (TStringBuf(name) == "Fork") {
                const auto childSpanId = item.Params.Param[0].Get<ui64>();
                auto [_, inserted] =
                    SpanIdsDeclaredAndNotJoinedYet.insert(childSpanId);
                if (!inserted) {
                    return true;
                }
                continue;
            }

            if (TStringBuf(name) != "Join") {
                continue;
            }

            const auto childSpanId = item.Params.Param[0].Get<ui64>();
            const auto itemsCount = item.Params.Param[1].Get<ui64>();

            SpanIdsDeclaredAndNotJoinedYet.erase(childSpanId);

            if (HasConflict(
                    tl,
                    iCtx.ConstructSubSpanIterationContext(itemsCount),
                    childSpanId))
            {
                return true;
            }
        }

        SpanIdsParents.erase(currentSpanId);
        return false;
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TTraceInfo ConvertToOpenTelemetrySpans(const NLWTrace::TTrackLog& tl)
{
    TTraceInfo traceInfo;

    TLogItemsIterationContext traceItCtx{.Idx = 0, .Count = tl.Items.size()};

    traceInfo.RequestId = FindRequestId(tl, traceItCtx);
    traceInfo.DiskId = FindFirst<TString>(tl, "diskId", traceItCtx);

    auto converter = TTraceConverter(
        RandomNumber<ui64>(),   // traceId
        GetReferenceTimeCycle(tl),
        TConflictDetecter().HasConflict(tl, traceItCtx, 0));

    traceInfo.Spans =
        TSpanTree::ExtractSpans(converter.ProcessItemsToSpanTree(tl));

    return traceInfo;
}

}   // namespace NCloud
