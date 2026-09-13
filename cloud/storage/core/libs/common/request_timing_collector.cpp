#include "request_timing_collector.h"

#include "request_timing_graph_builder.h"

#include <cloud/storage/core/protos/request_timing.pb.h>

#include <algorithm>

namespace NCloud {

namespace {

thread_local ui64 ThreadEventSequence = 0;

template <typename T>
void ReserveTimingVector(TVector<T>& values, size_t required, size_t limit)
{
    Y_ABORT_UNLESS(required <= limit);
    if (required > values.capacity()) {
        const size_t grown = values.capacity() > limit / 2
                                 ? limit
                                 : std::max<size_t>(8, values.capacity() * 2);
        values.reserve(std::max(required, std::min(grown, limit)));
    }
}

}   // namespace

ui64 TRequestTimingCollector::GetThreadEventSequence()
{
    return ThreadEventSequence;
}

TRequestTimingCollector::TRequestTimingCollector(ui64 requestId)
    : RequestId(requestId)
{
    Spans.emplace_back();
}

TRequestTimingCollector::~TRequestTimingCollector() = default;

size_t TRequestTimingCollector::RemainingEvents() const
{
    return MaxEvents - std::min(Events, MaxEvents);
}

void TRequestTimingCollector::Materialize()
{
    // Build off to the side: allocation failure leaves the journal intact.
    auto builder = std::make_unique<TRequestTimingGraphBuilder>(RequestId);
    Y_ABORT_UNLESS(ReplayRequestTimingJournal(Journal, *builder));
    Builder = std::move(builder);
    // Release all front-only storage when switching representations.
    TVector<ui64>().swap(Journal);
    TVector<TNodeMeta>().swap(Nodes);
    TVector<TSpanMeta>().swap(Spans);
}

bool TRequestTimingCollector::BeginRecord(
    size_t wordCount, size_t extraNodes, size_t extraSpans)
{
    if (wordCount > MaxRequestTimingJournalWords ||
        Journal.size() > MaxRequestTimingJournalWords - wordCount)
    {
        Materialize();
        return false;
    }
    // Reserve geometrically, before committing the journal or metadata.
    ReserveTimingVector(
        Journal, Journal.size() + wordCount, MaxRequestTimingJournalWords);
    const size_t remaining = RemainingEvents();
    ReserveTimingVector(
        Nodes, Nodes.size() + std::min(extraNodes, remaining), MaxEvents);
    ReserveTimingVector(
        Spans, Spans.size() + std::min(extraSpans, remaining), MaxEvents + 1);
    return true;
}

void TRequestTimingCollector::Append(
    ERequestTimingOp op, std::initializer_list<ui64> args)
{
    Journal.push_back(static_cast<ui64>(op));
    for (ui64 value: args) {
        Journal.push_back(value);
    }
}

ui32 TRequestTimingCollector::Seal(ui32 span, ui64 now)
{
    if (span >= Spans.size() || Spans[span].Closed) {
        return InvalidId;
    }
    auto& part = Spans[span];
    if (part.Suspended || now < part.Begin || ++Events > MaxEvents) {
        return InvalidId;
    }
    const ui32 node = Nodes.size();
    Nodes.push_back({now, span});
    part.Begin = now;
    part.Last = node;
    return node;
}

ui32 TRequestTimingCollector::Fork(ui32 span, ui64 now)
{
    std::lock_guard guard(Mutex);
    if (Frozen) {
        return InvalidId;
    }
    if (Builder || !BeginRecord(3, 1, 0)) {
        return Builder->Fork(span, now);
    }
    Append(ERequestTimingOp::Fork, {span, now});
    const ui32 node = Seal(span, now);
    if (node != InvalidId) {
        Spans[span].Suspended = true;
    }
    return node;
}

ui32 TRequestTimingCollector::Start(ui32 fork, ui64 now)
{
    std::lock_guard guard(Mutex);
    if (Frozen) {
        return InvalidId;
    }
    if (Builder || !BeginRecord(3, 0, 1)) {
        return Builder->Start(fork, now);
    }
    Append(ERequestTimingOp::Start, {fork, now});
    if (fork >= Nodes.size() || ++Events > MaxEvents || now < Nodes[fork].End) {
        return InvalidId;
    }
    const ui32 id = Spans.size();
    TSpanMeta part;
    part.Begin = now;
    part.ParentFork = fork;
    part.ParentPart = Nodes[fork].Part;
    Spans.push_back(part);
    return id;
}

void TRequestTimingCollector::Finish(ui32 span, ui64 now)
{
    std::lock_guard guard(Mutex);
    if (Frozen) {
        return;
    }
    if (Builder) {
        Builder->Finish(span, now);
        return;
    }
    if (span >= Spans.size() || Spans[span].Closed) {
        return;
    }
    if (!BeginRecord(3, 1, 0)) {
        Builder->Finish(span, now);
        return;
    }
    Append(ERequestTimingOp::Finish, {span, now});
    Seal(span, now);
    Spans[span].Closed = true;
}

void TRequestTimingCollector::Join(
    ui32 span, const TVector<ui32>& children, ui64 now)
{
    std::lock_guard guard(Mutex);
    if (Frozen) {
        return;
    }
    if (Builder) {
        Builder->Join(span, children, now);
        return;
    }
    if (span >= Spans.size() || Spans[span].Closed) {
        return;
    }
    if (children.size() > MaxRequestTimingJournalWords - 4) {
        Materialize();
        Builder->Join(span, children, now);
        return;
    }
    bool duplicate = false;
    if (Spans[span].Suspended && !children.empty() &&
        children.size() <= RemainingEvents())
    {
        auto sorted = children;
        std::sort(sorted.begin(), sorted.end());
        duplicate =
            std::adjacent_find(sorted.begin(), sorted.end()) != sorted.end();
    }
    if (!BeginRecord(4 + children.size(), 0, 0)) {
        Builder->Join(span, children, now);
        return;
    }
    Append(ERequestTimingOp::Join, {span, now, children.size()});
    for (ui32 child: children) {
        Journal.push_back(child);
    }
    auto& part = Spans[span];
    if (!part.Suspended || children.empty() ||
        children.size() > RemainingEvents() || duplicate)
    {
        return;
    }
    Events += children.size();
    const ui32 parentFork = part.Last;
    part.Begin = 0;
    part.Suspended = false;
    // A malformed join retains the original builder's partial state.
    for (ui32 child: children) {
        if (child >= Spans.size()) {
            return;
        }
        const auto& childPart = Spans[child];
        if (!childPart.Closed || childPart.Last == InvalidId ||
            childPart.Stopped || childPart.ParentFork != parentFork)
        {
            return;
        }
        part.Begin = std::max(part.Begin, Nodes[childPart.Last].End);
    }
}

void TRequestTimingCollector::Cancel(ui32 span, ui64 now)
{
    std::lock_guard guard(Mutex);
    if (Frozen) {
        return;
    }
    if (Builder) {
        Builder->Cancel(span, now);
        return;
    }
    if (span >= Spans.size()) {
        return;
    }
    if (!BeginRecord(3, Spans.size() - span, 0)) {
        Builder->Cancel(span, now);
        return;
    }
    Append(ERequestTimingOp::Cancel, {span, now});
    for (ui32 i = span; i < Spans.size(); ++i) {
        auto& part = Spans[i];
        if (i != span && (part.ParentPart >= Spans.size() ||
                          !Spans[part.ParentPart].Stopped))
        {
            continue;
        }
        if (!part.Closed && !part.Suspended) {
            Seal(i, now);
        }
        part.Closed = true;
        part.Stopped = true;
    }
}

void TRequestTimingCollector::Wait(
    ui32 span, ui32 categories, ui64 begin, ui64 end)
{
    std::lock_guard guard(Mutex);
    ++ThreadEventSequence;
    if (Frozen) {
        return;
    }
    if (Builder) {
        Builder->Wait(span, categories, begin, end);
        return;
    }
    if (span >= Spans.size() || Spans[span].Closed) {
        return;
    }
    if (!BeginRecord(5, 0, 0)) {
        Builder->Wait(span, categories, begin, end);
        return;
    }
    Append(ERequestTimingOp::Wait, {span, categories, begin, end});
    ++Events;
}

void TRequestTimingCollector::Missing(ui32 span, ui32 categories, ui64 duration)
{
    std::lock_guard guard(Mutex);
    if (duration) {
        ++ThreadEventSequence;
    }
    if (Frozen || !duration) {
        return;
    }
    if (Builder || !BeginRecord(4, 0, 0)) {
        Builder->Missing(span, categories, duration);
        return;
    }
    Append(ERequestTimingOp::Missing, {span, categories, duration});
}

void TRequestTimingCollector::Incomplete(TString reason)
{
    std::lock_guard guard(Mutex);
    if (Frozen) {
        return;
    }
    const size_t payloadWords = reason.size() / 8 + (reason.size() % 8 != 0);
    if (Builder || !BeginRecord(2 + payloadWords, 0, 0)) {
        Builder->Incomplete(std::move(reason));
        return;
    }
    Append(ERequestTimingOp::Incomplete, {reason.size()});
    for (size_t offset = 0; offset < reason.size(); offset += 8) {
        ui64 word = 0;
        const size_t count = std::min<size_t>(8, reason.size() - offset);
        for (size_t i = 0; i < count; ++i) {
            word |= static_cast<ui64>(
                        static_cast<unsigned char>(reason[offset + i]))
                    << (i * 8);
        }
        Journal.push_back(word);
    }
}

void TRequestTimingCollector::Freeze(ui64 now, ui32 categories, ui32 errorCode)
{
    std::lock_guard guard(Mutex);
    if (!Frozen) {
        if (Builder) {
            Builder->Freeze(now, categories, errorCode);
        }
        FrozenEnd = now;
        FrozenCategories = categories;
        FrozenError = errorCode;
        Frozen = true;
    }
}

void TRequestTimingCollector::FillTrace(NProto::TRequestTimingTrace& trace)
{
    {
        std::lock_guard guard(Mutex);
        Y_ABORT_UNLESS(Frozen);
    }
    // Freeze publishes an immutable owner and buffer. Late recording cannot
    // mutate either representation, and serializers own separate outputs.
    if (Builder) {
        Builder->FillTrace(trace);
        return;
    }
    trace.Clear();
    trace.SetVersion(2);
    trace.SetRequestId(RequestId);
    trace.SetTotalMicros(FrozenEnd);
    trace.SetSelectedCategories(FrozenCategories);
    trace.SetErrorCode(FrozenError);
    trace.MutableJournal()->Assign(Journal.begin(), Journal.end());
}

TString TRequestTimingCollector::Complete(
    ui64 now, ui32 categories, ui32 errorCode)
{
    Freeze(now, categories, errorCode);
    std::call_once(
        SnapshotOnce,
        [this]
        {
            try {
                BuildSnapshot();
            } catch (...) {
                SnapshotException = std::current_exception();
            }
        });
    if (SnapshotException) {
        std::rethrow_exception(SnapshotException);
    }
    return Snapshot;
}

void TRequestTimingCollector::BuildSnapshot()
{
    NProto::TRequestTimingTrace trace;
    FillTrace(trace);
    Snapshot = FormatRequestTimingTrace(trace);
}

}   // namespace NCloud
