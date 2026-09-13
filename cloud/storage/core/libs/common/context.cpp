#include "context.h"

#include "request_timing_collector.h"

#include <cloud/storage/core/protos/request_timing.pb.h>

#include <util/datetime/cputimer.h>
#include <util/system/spin_wait.h>

namespace NCloud {

////////////////////////////////////////////////////////////////////////////////

class TCallContextBase::TTimingGuard
{
private:
    TCallContextBase& Context;
    ETimingPhase Previous;
    bool Owns = true;

public:
    explicit TTimingGuard(TCallContextBase& context)
        : TTimingGuard(context, context.LoadTimingPhase())
    {}

    TTimingGuard(TCallContextBase& context, ETimingPhase previous)
        : Context(context)
        , Previous(previous)
    {
        while (!Context.TimingPhase.compare_exchange_weak(
            Previous,
            ETimingPhase::Locked,
            std::memory_order_acquire, std::memory_order_acquire))
        {
            Previous = Context.LoadTimingPhase();
        }
    }

    TTimingGuard(const TTimingGuard&) = delete;
    TTimingGuard& operator=(const TTimingGuard&) = delete;

    ~TTimingGuard()
    {
        if (Owns) {
            Commit(Previous);
        }
    }

    ETimingPhase GetPrevious() const
    {
        return Previous;
    }

    void Commit(ETimingPhase next)
    {
        Owns = false;
        Context.TimingPhase.store(next, std::memory_order_release);
    }
};

TCallContextBase::ETimingPhase TCallContextBase::LoadTimingPhase() const
{
    auto phase = TimingPhase.load(std::memory_order_acquire);
    if (phase == ETimingPhase::Locked) {
        // Contention is limited to metadata/first allocation, never I/O, graph
        // evaluation or JSON. Use the same backoff as TAdaptiveLock; no waiter
        // notification is needed on the uncontended request path.
        TSpinWait wait;
        do {
            wait.Sleep();
            phase = TimingPhase.load(std::memory_order_acquire);
        } while (phase == ETimingPhase::Locked);
    }
    return phase;
}

TCallContextBase::TCallContextBase(ui64 requestId, TCallContextBasePtr parent)
    : LegacyParent(std::move(parent))
    , RequestId(requestId)
    , LWOrbit(LegacyParent ? LegacyParent->LWOrbit : OwnLWOrbit)
{}

TRequestTime TCallContextBase::CalcRequestTime(ui64 nowCycles) const
{
    const ui64 startCycles = GetRequestStartedCycles();
    if (!startCycles || startCycles >= nowCycles) {
        return TRequestTime{
            .TotalTime = TDuration::Zero(),
            .ExecutionTime = TDuration::Zero(),
        };
    }

    TRequestTime requestTime;
    requestTime.TotalTime = CyclesToDurationSafe(nowCycles - startCycles);

    const ui64 postponeStart = AtomicGet(PostponeTsCycles);
    if (postponeStart && startCycles < postponeStart &&
        postponeStart < nowCycles)
    {
        nowCycles = postponeStart;
    }

    const auto postponeDuration = Time(EProcessingStage::Postponed);
    const auto backoffTime = Time(EProcessingStage::Backoff);
    const auto shapingTime = Time(EProcessingStage::Shaping);

    auto responseSentCycles = GetResponseSentCycles();
    auto responseDuration = CyclesToDurationSafe(
        (responseSentCycles ? responseSentCycles : nowCycles) - startCycles);

    requestTime.ExecutionTime = responseDuration - postponeDuration -
                                backoffTime - shapingTime -
                                GetPossiblePostponeDuration();

    return requestTime;
}

TDuration TCallContextBase::GetPossiblePostponeDuration() const
{
    if (LegacyParent) {
        return LegacyParent->GetPossiblePostponeDuration();
    }

    return TDuration::MicroSeconds(AtomicGet(PossiblePostponeMicroSeconds));
}

void TCallContextBase::SetPossiblePostponeDuration(TDuration d)
{
    if (LegacyParent) {
        LegacyParent->SetPossiblePostponeDuration(d);
        return;
    }

    AtomicSet(PossiblePostponeMicroSeconds, d.MicroSeconds());
}

ui64 TCallContextBase::GetRequestStartedCycles() const
{
    if (LegacyParent) {
        return LegacyParent->GetRequestStartedCycles();
    }

    return AtomicGet(RequestStartedCycles);
}

void TCallContextBase::SetRequestStartedCycles(ui64 cycles)
{
    if (LegacyParent) {
        LegacyParent->SetRequestStartedCycles(cycles);
        return;
    }

    AtomicSet(RequestStartedCycles, cycles);
}

TInstant TCallContextBase::GetPostponeTs() const
{
    return PostponeTs;
}

void TCallContextBase::SetPostponeTs(TInstant ts)
{
    PostponeTs = ts;
}

ui64 TCallContextBase::GetResponseSentCycles() const
{
    if (LegacyParent) {
        return LegacyParent->GetResponseSentCycles();
    }

    return AtomicGet(ResponseSentCycles);
}

void TCallContextBase::SetResponseSentCycles(ui64 cycles)
{
    if (LegacyParent) {
        LegacyParent->SetResponseSentCycles(cycles);
        return;
    }

    AtomicSet(ResponseSentCycles, cycles);
}

void TCallContextBase::Postpone(ui64 nowCycles)
{
    Y_DEBUG_ABORT_UNLESS(
        AtomicGet(PostponeTsCycles) == 0, "Request was not advanced.");
    Y_DEBUG_ABORT_UNLESS(nowCycles > 0);
    AtomicSet(PostponeTsCycles, nowCycles);
    if (LegacyParent) {
        LegacyParent->SetLegacyPostponeCycles(nowCycles);
    }
    if (const auto timing = GetOrCreateRequestTiming()) {
        timing->Wait(
            TimingPart,
            1u << static_cast<ui32>(EProcessingStage::Postponed),
            TimingOffset(nowCycles), std::numeric_limits<ui64>::max());
    }
}

TDuration TCallContextBase::Advance(ui64 nowCycles)
{
    const auto start = AtomicGet(PostponeTsCycles);
    Y_DEBUG_ABORT_UNLESS(start != 0, "Request was not postponed.");

    const auto delay = CyclesToDurationSafe(nowCycles - start);
    AddTimedWait(EProcessingStage::Postponed, start, nowCycles);
    AtomicSet(PostponeTsCycles, 0);
    if (LegacyParent) {
        LegacyParent->SetLegacyPostponeCycles(0);
    }

    return delay;
}

TDuration TCallContextBase::Time(EProcessingStage stage) const
{
    if (LegacyParent) {
        return LegacyParent->Time(stage);
    }

    return TDuration::MicroSeconds(
        AtomicGet(Stage2Time[static_cast<int>(stage)]));
}

void TCallContextBase::AddTime(EProcessingStage stage, TDuration d)
{
    AddLegacyTime(stage, d);
    if (d && IsRequestTimingEnabled()) {
        const auto timing = GetOrCreateRequestTiming();
        if (!timing) {
            return;
        }
        timing->Missing(
            TimingPart, 1u << static_cast<ui32>(stage), d.MicroSeconds());
    }
}

void TCallContextBase::SetLegacyPostponeCycles(ui64 cycles)
{
    if (LegacyParent) {
        LegacyParent->SetLegacyPostponeCycles(cycles);
    } else {
        // Preserve the legacy live-request observation. This single timestamp
        // is not used by the new diagnostic graph.
        AtomicSet(PostponeTsCycles, cycles);
    }
}

void TCallContextBase::AddLegacyTime(EProcessingStage stage, TDuration d)
{
    if (LegacyParent) {
        LegacyParent->AddLegacyTime(stage, d);
    } else {
        AtomicAdd(Stage2Time[static_cast<int>(stage)], d.MicroSeconds());
    }
}

ui64 TCallContextBase::TimingOffset(ui64 cycles) const
{
    return cycles >= TimingOrigin
               ? CyclesToDurationSafe(cycles - TimingOrigin).MicroSeconds()
               : std::numeric_limits<ui64>::max();
}

ui64 TCallContextBase::GetThreadTimingEventSequence()
{
    return TRequestTimingCollector::GetThreadEventSequence();
}

bool TCallContextBase::IsRequestTimingEnabled() const
{
    return LoadTimingPhase() != ETimingPhase::Disabled;
}

void TCallContextBase::EnableRequestTiming()
{
    const auto started = GetRequestStartedCycles();
    const auto phase = LoadTimingPhase();
    if (!started || phase == ETimingPhase::Frozen ||
        (phase != ETimingPhase::Disabled && TimingOrigin == started))
    {
        return;
    }
    TTimingGuard guard(*this, phase);
    if (guard.GetPrevious() == ETimingPhase::Frozen) {
        return;
    }
    if (!TimingOrigin) {
        TimingOrigin = started;
        guard.Commit(ETimingPhase::Enabled);
    } else if (TimingOrigin != started) {
        if (!RequestTiming) {
            // Publish only after all potentially throwing initialization.
            auto timing = std::make_shared<TRequestTimingCollector>(RequestId);
            timing->Incomplete("measurement_start_changed");
            RequestTiming = std::move(timing);
        } else {
            RequestTiming->Incomplete("measurement_start_changed");
        }
        guard.Commit(ETimingPhase::CollectorReady);
    }
}

TRequestTimingCollector* TCallContextBase::GetOrCreateRequestTiming()
{
    const auto phase = LoadTimingPhase();
    if (phase == ETimingPhase::Disabled) {
        return nullptr;
    }
    if (phase == ETimingPhase::CollectorReady || phase == ETimingPhase::Frozen)
    {
        // The owner is never reassigned after its first release publication.
        return RequestTiming.get();
    }
    TTimingGuard guard(*this, phase);
    if (guard.GetPrevious() == ETimingPhase::Enabled) {
        auto timing = std::make_shared<TRequestTimingCollector>(RequestId);
        RequestTiming = std::move(timing);
        guard.Commit(ETimingPhase::CollectorReady);
    }
    return RequestTiming.get();
}

ui32 TCallContextBase::ForkRequestTiming(ui64 nowCycles)
{
    const auto timing = GetOrCreateRequestTiming();
    return timing ? timing->Fork(TimingPart, TimingOffset(nowCycles))
                  : TRequestTimingCollector::InvalidId;
}

void TCallContextBase::InitChildRequestTiming(
    TCallContextBasePtr parent, ui32 fork, ui64 nowCycles)
{
    // CreateRequestTimingChild has not published this new child yet.
    // Repeated initialization must never replace a published collector owner.
    TTimingGuard childGuard(*this);
    if (childGuard.GetPrevious() != ETimingPhase::Disabled) {
        return;
    }
    Y_ABORT_UNLESS(parent && parent.Get() != this);
    auto parentPhase = parent->LoadTimingPhase();
    while (parentPhase == ETimingPhase::Enabled) {
        parent->GetOrCreateRequestTiming();
        parentPhase = parent->LoadTimingPhase();
    }
    std::shared_ptr<TRequestTimingCollector> timing;
    TFrozenTiming frozen;
    ui64 origin = 0;
    if (parentPhase == ETimingPhase::CollectorReady ||
        parentPhase == ETimingPhase::Frozen)
    {
        // Acquire publishes the owner/origin, which never change afterwards.
        // No parent lock is needed for each child of an already materialized
        // request. The strong parent argument keeps both fields alive.
        timing = parent->RequestTiming;
        origin = parent->TimingOrigin;
        if (parentPhase == ETimingPhase::Frozen) {
            frozen = parent->FrozenTiming;
        }
        // Do not read FrozenTiming in Ready: concurrent Freeze may write it.
    }
    // Start can allocate. Keep child metadata untouched until it succeeds.
    const ui32 part =
        timing
            ? timing->Start(
                  fork,
                  nowCycles >= origin
                      ? CyclesToDurationSafe(nowCycles - origin).MicroSeconds()
                      : std::numeric_limits<ui64>::max())
            : 0;
    LegacyParent = std::move(parent);
    RequestTiming = std::move(timing);
    FrozenTiming = std::move(frozen);
    TimingOrigin = origin;
    TimingPart = part;
    childGuard.Commit(parentPhase);
}

void TCallContextBase::FinishRequestTiming(ui64 nowCycles)
{
    if (const auto timing = GetOrCreateRequestTiming()) {
        timing->Finish(TimingPart, TimingOffset(nowCycles));
    }
}

void TCallContextBase::JoinRequestTiming(
    const TVector<TCallContextBasePtr>& children, ui64 nowCycles)
{
    if (const auto timing = GetOrCreateRequestTiming()) {
        TVector<ui32> parts;
        parts.reserve(children.size());
        for (const auto& child: children) {
            parts.push_back(child->TimingPart);
        }
        timing->Join(TimingPart, parts, TimingOffset(nowCycles));
    }
}

void TCallContextBase::CancelRequestTiming(ui64 nowCycles)
{
    if (const auto timing = GetOrCreateRequestTiming()) {
        timing->Cancel(TimingPart, TimingOffset(nowCycles));
    }
}

void TCallContextBase::MarkRequestTimingIncomplete(TString reason)
{
    if (const auto timing = GetOrCreateRequestTiming()) {
        timing->Incomplete(std::move(reason));
    }
}

void TCallContextBase::AddTimedWait(
    EProcessingStage stage, ui64 beginCycles, ui64 endCycles)
{
    AddLegacyTime(stage, CyclesToDurationSafe(endCycles - beginCycles));
    if (const auto timing = GetOrCreateRequestTiming()) {
        timing->Wait(
            TimingPart,
            1u << static_cast<ui32>(stage),
            TimingOffset(beginCycles), TimingOffset(endCycles));
    }
}

TRequestTimingSnapshot TCallContextBase::GetFrozenRequestTiming() const
{
    // The phase acquire (or the caller's guard) publishes these immutable
    // scalars. Keep the collector owner only once in the live context.
    TRequestTimingSnapshot snapshot;
    if (RequestTiming) {
        snapshot.Data = RequestTiming;
    } else {
        snapshot.Data = TRequestTimingSnapshot::TOrdinary{
            FrozenTiming.RequestId,
            FrozenTiming.TotalMicros,
            FrozenTiming.ErrorCode};
    }
    return snapshot;
}

TRequestTimingSnapshot TCallContextBase::FreezeRequestTiming(
    TDuration total, ui32 errorCode)
{
    const auto phase = LoadTimingPhase();
    if (phase == ETimingPhase::Frozen) {
        return GetFrozenRequestTiming();
    }
    if (phase == ETimingPhase::Disabled) {
        return {};
    }
    TTimingGuard guard(*this, phase);
    if (guard.GetPrevious() != ETimingPhase::Frozen) {
        if (RequestTiming) {
            RequestTiming->Freeze(
                total.MicroSeconds(),
                TRequestTiming::SupportedWaitCategories, errorCode);
        } else {
            FrozenTiming.RequestId = RequestId;
            FrozenTiming.TotalMicros = total.MicroSeconds();
            FrozenTiming.ErrorCode = errorCode;
        }
        guard.Commit(ETimingPhase::Frozen);
    }
    return GetFrozenRequestTiming();
}

TString TCallContextBase::CompleteRequestTiming(TDuration total, ui32 errorCode)
{
    return FreezeRequestTiming(total, errorCode).Serialize();
}

void TRequestTimingSnapshot::FillTrace(NProto::TRequestTimingTrace& trace) const
{
    trace.Clear();
    if (const auto* owner =
            std::get_if<std::shared_ptr<TRequestTimingCollector>>(&Data))
    {
        (*owner)->FillTrace(trace);
        return;
    }
    if (const auto* ordinary = std::get_if<TOrdinary>(&Data)) {
        trace.SetVersion(1);
        trace.SetRequestId(ordinary->RequestId);
        trace.SetTotalMicros(ordinary->TotalMicros);
        trace.SetSelectedCategories(TRequestTiming::SupportedWaitCategories);
        trace.SetErrorCode(ordinary->ErrorCode);
        trace.SetImplicitRoot(true);
    }
}

TString TRequestTimingSnapshot::Serialize() const
{
    if (!HasData()) {
        return {};
    }
    if (const auto* owner =
            std::get_if<std::shared_ptr<TRequestTimingCollector>>(&Data))
    {
        // The collector already owns the first frozen completion.
        return (*owner)->Complete(0, 0, 0);
    }
    NProto::TRequestTimingTrace trace;
    FillTrace(trace);
    return FormatRequestTimingTrace(trace);
}

}   // namespace NCloud
