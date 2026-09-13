#include "request_timing.h"

#include "request_timing_collector.h"
#include "request_timing_graph_builder.h"
#include "request_timing_journal.h"

#include <cloud/storage/core/protos/request_timing.pb.h>

#include <library/cpp/json/json_value.h>
#include <library/cpp/json/json_writer.h>

#include <algorithm>

namespace NCloud {

TString FormatRequestTimingTrace(const NProto::TRequestTimingTrace& trace)
{
    NJson::TJsonValue json(NJson::JSON_MAP);
    if (trace.HasTotalMicros()) {
        json["total_us"] = trace.GetTotalMicros();
    }
    if (trace.HasRequestId()) {
        json["request_id"] = trace.GetRequestId();
    }
    auto invalid = [&]
    {
        json["complete"] = false;
        json["reason"] = trace.HasVersion() && trace.GetVersion() != 1 &&
                                 trace.GetVersion() != 2
                             ? "unsupported_timing_trace_version"
                             : "invalid_timing_trace";
        json["without_waits_us"] = NJson::TJsonValue(NJson::JSON_NULL);
        json["wait_impact_us"] = NJson::TJsonValue(NJson::JSON_NULL);
        return NJson::WriteJson(json, false);
    };
    if ((trace.GetVersion() != 1 && trace.GetVersion() != 2) ||
        !trace.HasRequestId() || !trace.HasTotalMicros() ||
        !trace.HasSelectedCategories() || !trace.HasErrorCode())
    {
        return invalid();
    }

    if (trace.GetVersion() == 2) {
        if (trace.HasImplicitRoot() || trace.PartsSize() ||
            trace.StagesSize() || trace.HasCompletionNode() ||
            trace.HasFailure())
        {
            return invalid();
        }
        TRequestTimingGraphBuilder builder(trace.GetRequestId());
        if (!ReplayRequestTimingJournal(
                {trace.GetJournal().data(),
                 static_cast<size_t>(trace.JournalSize())}, builder))
        {
            return invalid();
        }
        builder.Freeze(
            trace.GetTotalMicros(),
            trace.GetSelectedCategories(), trace.GetErrorCode());
        NProto::TRequestTimingTrace restored;
        builder.FillTrace(restored);
        return FormatRequestTimingTrace(restored);
    }
    if (trace.JournalSize()) {
        return invalid();
    }

    // Bound work on malformed input before allocating the evaluation graph.
    constexpr size_t MaxItems = 4 * TRequestTimingCollector::MaxEvents;
    size_t items = static_cast<size_t>(trace.PartsSize()) + trace.StagesSize();
    if (items > MaxItems) {
        return invalid();
    }
    for (const auto& stage: trace.GetStages()) {
        items +=
            static_cast<size_t>(stage.DependenciesSize()) + stage.WaitsSize();
        if (items > MaxItems) {
            return invalid();
        }
    }
    if (trace.GetImplicitRoot()) {
        if (trace.PartsSize() || trace.StagesSize() ||
            trace.HasCompletionNode() || !trace.GetFailure().empty())
        {
            return invalid();
        }
    } else if (
        !trace.PartsSize() || !trace.StagesSize() || !trace.HasCompletionNode())
    {
        return invalid();
    }

    const auto now = trace.GetTotalMicros();
    TVector<TTimingStage> stages;
    const ui32 completion =
        trace.GetImplicitRoot() ? 0 : trace.GetCompletionNode();
    if (trace.GetImplicitRoot()) {
        stages.emplace_back();
        stages.back().End = now;
    } else {
        stages.reserve(trace.StagesSize());
        for (const auto& encoded: trace.GetStages()) {
            if (!encoded.HasBegin() || !encoded.HasEnd() ||
                !encoded.HasPart() || encoded.GetPart() >= trace.PartsSize() ||
                encoded.UnlocatedWaitsSize() != 3)
            {
                return invalid();
            }
            auto& stage = stages.emplace_back();
            stage.Begin = encoded.GetBegin();
            stage.End = encoded.GetEnd();
            stage.NotBefore = encoded.GetNotBefore();
            stage.MissingCategories = encoded.GetMissingCategories();
            stage.IncompleteReason = encoded.GetIncompleteReason();
            std::copy(
                encoded.GetUnlocatedWaits().begin(),
                encoded.GetUnlocatedWaits().end(),
                stage.UnlocatedWaits.begin());
            for (const auto& d: encoded.GetDependencies()) {
                if (!d.HasNode() || !d.HasLag()) {
                    return invalid();
                }
                stage.Dependencies.push_back({d.GetNode(), d.GetLag()});
            }
            for (const auto& w: encoded.GetWaits()) {
                if (!w.HasBegin() || !w.HasEnd() || !w.HasCategories()) {
                    return invalid();
                }
                stage.Waits.push_back(
                    {w.GetBegin(), w.GetEnd(), w.GetCategories()});
            }
        }
        for (ui32 i = 0; i < trace.PartsSize(); ++i) {
            const auto& part = trace.GetParts(i);
            if (!part.HasClosed() || !part.HasObservationStopped() ||
                (i == 0 && part.HasParent()) ||
                (i != 0 && (!part.HasParent() || part.GetParent() >= i)))
            {
                return invalid();
            }
        }
    }
    auto result = TRequestTiming::Calculate(
        stages, completion, trace.GetSelectedCategories(), now);
    if (!trace.GetFailure().empty()) {
        result.IncompleteReason = trace.GetFailure();
        result.TimeWithoutWaits.reset();
        result.WaitImpact.reset();
    }
    json["version"] = 1;
    json["request_id"] = trace.GetRequestId();
    json["error_code"] = trace.GetErrorCode();
    json["total_us"] = now;
    json["selected_categories"] = trace.GetSelectedCategories();
    json["complete"] = result.TimeWithoutWaits.has_value();
    json["reason"] = result.IncompleteReason;
    json["without_waits_us"] = NJson::TJsonValue(NJson::JSON_NULL);
    json["wait_impact_us"] = NJson::TJsonValue(NJson::JSON_NULL);
    if (result.TimeWithoutWaits) {
        json["without_waits_us"] = result.TimeWithoutWaits->MicroSeconds();
        json["wait_impact_us"] = result.WaitImpact->MicroSeconds();
    }
    json["parts"] = NJson::TJsonValue(NJson::JSON_ARRAY);
    const ui32 parts = trace.GetImplicitRoot() ? 1 : trace.PartsSize();
    for (ui32 i = 0; i < parts; ++i) {
        NJson::TJsonValue part(NJson::JSON_MAP);
        part["id"] = i;
        part["parent"] = NJson::TJsonValue(NJson::JSON_NULL);
        if (!trace.GetImplicitRoot() && trace.GetParts(i).HasParent()) {
            part["parent"] = trace.GetParts(i).GetParent();
        }
        part["observation_stopped"] = !trace.GetImplicitRoot() &&
                                      trace.GetParts(i).GetObservationStopped();
        part["closed"] =
            trace.GetImplicitRoot() || trace.GetParts(i).GetClosed();
        json["parts"].AppendValue(std::move(part));
    }
    json["stages"] = NJson::TJsonValue(NJson::JSON_ARRAY);
    for (ui32 i = 0; i < stages.size(); ++i) {
        const auto& s = stages[i];
        if (s.Begin > now) {
            continue;
        }
        NJson::TJsonValue stage(NJson::JSON_MAP);
        stage["id"] = i;
        stage["part"] =
            trace.GetImplicitRoot() ? 0 : trace.GetStages(i).GetPart();
        stage["diagnostic_only"] =
            !trace.GetImplicitRoot() && trace.GetStages(i).GetDiagnosticOnly();
        stage["begin_us"] = s.Begin;
        stage["end_us"] = std::min(s.End, now);
        stage["not_before_us"] = s.NotBefore;
        stage["missing_categories"] = s.MissingCategories;
        stage["unlocated_wait_us"] = NJson::TJsonValue(NJson::JSON_ARRAY);
        for (auto duration: s.UnlocatedWaits) {
            stage["unlocated_wait_us"].AppendValue(duration);
        }
        stage["dependencies"] = NJson::TJsonValue(NJson::JSON_ARRAY);
        for (const auto& dep: s.Dependencies) {
            NJson::TJsonValue d(NJson::JSON_MAP);
            d["node"] = dep.Node;
            d["lag_us"] = dep.Lag;
            stage["dependencies"].AppendValue(std::move(d));
        }
        stage["waits"] = NJson::TJsonValue(NJson::JSON_ARRAY);
        for (const auto& wait: s.Waits) {
            if (wait.Begin > now) {
                continue;
            }
            NJson::TJsonValue w(NJson::JSON_MAP);
            w["begin_us"] = wait.Begin;
            w["end_us"] = std::min(wait.End, now);
            w["categories"] = wait.Categories;
            stage["waits"].AppendValue(std::move(w));
        }
        json["stages"].AppendValue(std::move(stage));
    }
    json["completion_node"] = completion;
    return NJson::WriteJson(json, false);
}

}   // namespace NCloud
