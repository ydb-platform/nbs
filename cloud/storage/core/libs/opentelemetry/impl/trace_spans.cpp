#include "trace_spans.h"

#include <util/generic/hash.h>
#include <util/generic/hash_set.h>
#include <util/generic/strbuf.h>
#include <util/random/random.h>

#include <optional>
#include <utility>

namespace NCloud {

namespace {

using namespace opentelemetry::proto::trace::v1;
using namespace opentelemetry::proto::common::v1;

struct TStage
{
    TStringBuf Begin;
    TStringBuf End;
    TStringBuf Name;
};

constexpr TStage Stages[] = {
    {"RequestStarted", "RequestCompleted", "Server"},
    // Forwarded requests reply directly to the service proxy.
    {"RequestReceived_Service", "ResponseReceived_Proxy", "Service"},
    {"RequestReceived_Throttler", "RequestCompleted_Throttler", "Throttler"},
    {"RequestPostponed_Volume", "RequestAdvanced_Volume", "Throttler.Queue"},
    {"RequestSent_PartitionWorker_DSProxy",
     "ResponseSent_PartitionWorker_DSProxy",
     "BlobStorage"},
};

const AnyValue* FindAttribute(const Span_Event& event, TStringBuf name)
{
    for (const auto& attribute: event.attributes()) {
        if (attribute.key() == name) {
            return &attribute.value();
        }
    }
    return nullptr;
}

TString GetWireParentId(const Span_Event& event)
{
    const auto* value = FindAttribute(event, "spanId");
    if (!value || !value->has_int_value() || !value->int_value()) {
        return {};
    }
    const ui64 id = value->int_value();
    return TString(reinterpret_cast<const char*>(&id), sizeof(id));
}

using TRequestKey = std::pair<TString, i64>;

std::optional<TRequestKey> GetRequestKey(const Span_Event& event)
{
    const auto* requestId = FindAttribute(event, "requestId");
    const auto* requestType = FindAttribute(event, "requestType");
    if (!requestId || !requestId->has_int_value() || !requestId->int_value() ||
        !requestType || !requestType->has_string_value() ||
        requestType->string_value().empty())
    {
        return {};
    }

    return TRequestKey{requestType->string_value(), requestId->int_value()};
}

}   // namespace

void AddDiagnosticSpans(TVector<Span>& spans)
{
    THashSet<TString> usedIds;
    for (const auto& span: spans) {
        usedIds.insert(span.span_id());
    }

    THashMap<TString, ui32> wireIds;
    for (const auto& parent: spans) {
        for (const auto& event: parent.events()) {
            if (event.name() == "RequestSent_PartitionWorker_DSProxy") {
                const auto id = GetWireParentId(event);
                if (id) {
                    ++wireIds[id];
                }
            }
        }
    }
    for (auto& [id, count]: wireIds) {
        if (!usedIds.insert(id).second) {
            ++count;
        }
    }

    auto takeId = [&]
    {
        for (;;) {
            const ui64 id = RandomNumber<ui64>();
            if (id) {
                TString bytes(reinterpret_cast<const char*>(&id), sizeof(id));
                if (usedIds.insert(bytes).second) {
                    return bytes;
                }
            }
        }
    };

    TVector<Span> stages;
    for (const auto& parent: spans) {
        for (const auto& stage: Stages) {
            THashMap<TRequestKey, const Span_Event*> pending;
            for (const auto& event: parent.events()) {
                if (event.name() != stage.Begin && event.name() != stage.End) {
                    continue;
                }

                const auto key = GetRequestKey(event);
                if (!key) {
                    continue;
                }

                if (event.name() == stage.Begin) {
                    // Overlapping starts with the same key cannot be
                    // attributed to a particular attempt. Keep them as events.
                    const auto [it, inserted] = pending.emplace(*key, &event);
                    if (!inserted) {
                        it->second = nullptr;
                    }
                    continue;
                }

                const auto it = pending.find(*key);
                if (it == pending.end()) {
                    continue;
                }
                const auto* beginEvent = it->second;
                pending.erase(it);
                if (!beginEvent) {
                    continue;
                }
                const auto& begin = *beginEvent;

                if (event.time_unix_nano() < begin.time_unix_nano()) {
                    continue;
                }

                Span span;
                span.set_trace_id(parent.trace_id());
                const auto wireId = stage.Name == "BlobStorage"
                                        ? GetWireParentId(begin)
                                        : TString{};
                span.set_span_id(
                    wireId && wireIds.at(wireId) == 1 ? wireId : takeId());
                span.set_parent_span_id(parent.span_id());
                span.set_name(TString(stage.Name) + "." + key->first);
                span.set_kind(Span::SPAN_KIND_INTERNAL);
                span.set_start_time_unix_nano(begin.time_unix_nano());
                span.set_end_time_unix_nano(event.time_unix_nano());
                *span.mutable_attributes() = begin.attributes();
                auto* attribute = span.add_attributes();
                attribute->set_key("nbs.stage");
                attribute->mutable_value()->set_string_value(
                    stage.Name.data(), stage.Name.size());

                *span.add_events() = begin;
                *span.add_events() = event;
                if (const auto* error = FindAttribute(event, "error");
                    error && error->has_int_value())
                {
                    auto* errorAttribute = span.add_attributes();
                    errorAttribute->set_key("error");
                    *errorAttribute->mutable_value() = *error;
                    if (static_cast<ui64>(error->int_value()) & 0x80000000ULL) {
                        span.mutable_status()->set_code(
                            Status::STATUS_CODE_ERROR);
                    }
                }

                stages.push_back(std::move(span));
            }
        }
    }

    for (auto& stage: stages) {
        spans.push_back(std::move(stage));
    }
}

}   // namespace NCloud
