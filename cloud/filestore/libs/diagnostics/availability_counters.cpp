#include "availability_counters.h"

#include "critical_events.h"

#include <util/string/builder.h>
#include <util/system/yassert.h>

#include <cerrno>

namespace NCloud::NFileStore {

namespace {

////////////////////////////////////////////////////////////////////////////////

// Limits the number of intervals finished by a single UpdateStats() call.
// Protects against unbounded catch-up loops after large forward clock jumps.
// With the default 5-minute interval this covers a stats-updater stall of up
// to one hour, anything beyond that realigns without evaluation.
constexpr size_t MaxIntervalsPerUpdate = 12;

constexpr TDuration DefaultIntervalDuration = TDuration::Minutes(5);

}   // namespace

////////////////////////////////////////////////////////////////////////////////

////////////////////////////////////////////////////////////////////////////////

const char* GetAvailabilityRequestTypeName(
    EFileStoreAvailabilityRequestType requestType)
{
    using EType = EFileStoreAvailabilityRequestType;
    switch (requestType) {
        case EType::Lookup: return "lookup";
        case EType::GetAttr: return "getattr";
        case EType::SetAttr: return "setattr";
        case EType::ReadLink: return "readlink";
        case EType::MkDir: return "mkdir";
        case EType::RmDir: return "rmdir";
        case EType::Unlink: return "unlink";
        case EType::SymLink: return "symlink";
        case EType::Link: return "link";
        case EType::Rename: return "rename";
        case EType::Open: return "open";
        case EType::Create: return "create";
        case EType::Read: return "read";
        case EType::Write: return "write";
        case EType::WriteBuf: return "write_buf";
        case EType::Flush: return "flush";
        case EType::Fsync: return "fsync";
        case EType::Release: return "release";
        case EType::OpenDir: return "opendir";
        case EType::ReadDir: return "readdir";
        case EType::ReadDirPlus: return "readdirplus";
        case EType::ReleaseDir: return "releasedir";
        case EType::None:
        case EType::MAX:
            break;
    }
    return "unknown";
}

////////////////////////////////////////////////////////////////////////////////

void TAvailabilityCounters::EnableAndRegister(
    TDuration intervalDuration,
    NMonitoring::TDynamicCounters& counters)
{
    // Serialize concurrent calls; the losers become no-ops.
    TGuard g{EnableLock};

    if (CountersRegistered.load(std::memory_order_acquire)) {
        return;
    }

    // A zero interval duration selects the default one.
    IntervalDuration = intervalDuration == TDuration::Zero()
        ? DefaultIntervalDuration
        : intervalDuration;

    TotalIntervalsCounter = counters.GetCounter(
        "Availability_TotalIntervals",
        true);   // derivative
    AvailableIntervalsCounter = counters.GetCounter(
        "Availability_AvailableIntervals",
        true);   // derivative
    UnavailableIntervalsCounter = counters.GetCounter(
        "Availability_UnavailableIntervals",
        true);   // derivative
    LastIntervalAvailableCounter = counters.GetCounter(
        "Availability_LastIntervalAvailable");
    MissingIntervalsCounter = counters.GetCounter(
        "Availability_MissingIntervals",
        true);   // derivative

    // No intervals have been reported yet - start as available.
    *LastIntervalAvailableCounter = 1;

    // Index 0 is EFileStoreAvailabilityRequestType::None and stays unused.
    for (size_t i = 1; i < RequestTypeStates.size(); ++i) {
        auto& state = RequestTypeStates[i];
        auto requestCounters = counters.GetSubgroup(
            "request",
            GetAvailabilityRequestTypeName(
                static_cast<EFileStoreAvailabilityRequestType>(i)));

        state.AvailableIntervalsCounter = requestCounters->GetCounter(
            "Availability_AvailableIntervals",
            true);   // derivative
        state.UnavailableIntervalsCounter = requestCounters->GetCounter(
            "Availability_UnavailableIntervals",
            true);   // derivative
        state.LastIntervalAvailableCounter = requestCounters->GetCounter(
            "Availability_LastIntervalAvailable");

        *state.LastIntervalAvailableCounter = 1;
    }

    // Publishes the fully initialized state for the methods below.
    CountersRegistered.store(true, std::memory_order_release);
}

void TAvailabilityCounters::RequestStarted(
    TCallContext& callContext,
    TInstant now)
{
    if (!CountersRegistered.load(std::memory_order_acquire)) {
        return;
    }

    if (callContext.AvailabilityRequestType ==
        EFileStoreAvailabilityRequestType::None)
    {
        return;
    }

    auto& state = RequestTypeStates[
        static_cast<size_t>(callContext.AvailabilityRequestType)];

    for (;;) {
        // Assign the event to its actual wall-clock interval: roll the
        // elapsed intervals over before applying it.
        RollIntervals(now);

        TGuard g{state.Lock};

        // The interval may have rolled between RollIntervals() and taking
        // the state lock - retry so that the event can never be applied to
        // an interval that has already ended.
        if (now.MicroSeconds() >=
            CurrentIntervalEndUs.load(std::memory_order_acquire))
        {
            continue;
        }

        DoRequestStarted(callContext, state);
        return;
    }
}

// Protected by state.Lock.
void TAvailabilityCounters::DoRequestStarted(
    TCallContext& callContext,
    TRequestTypeState& state)
{
    // A repeated registration without a completion in between would leak
    // the previous registration's accounting: only one completion follows,
    // so the extra inflight unit would be reported as hung forever.
    if (callContext.AvailabilityIntervalSeqNo != 0) {
        ReportAvailabilityCountersDoubleRegistration(
            TStringBuilder() << "request type: "
                << static_cast<ui32>(callContext.AvailabilityRequestType));
        return;
    }

    ++state.Inflight;
    ++state.InflightStartedInInterval;

    callContext.AvailabilityIntervalSeqNo = state.IntervalSeqNo;
}

void TAvailabilityCounters::RequestCompleted(
    TCallContext& callContext,
    TInstant now)
{
    if (!CountersRegistered.load(std::memory_order_acquire)) {
        return;
    }

    if (callContext.AvailabilityRequestType ==
        EFileStoreAvailabilityRequestType::None)
    {
        return;
    }

    auto& state = RequestTypeStates[
        static_cast<size_t>(callContext.AvailabilityRequestType)];

    for (;;) {
        // Assign the event to its actual wall-clock interval: roll the
        // elapsed intervals over before applying it, so that a completion
        // arriving after an interval boundary cannot retroactively make the
        // interval it hung through available.
        RollIntervals(now);

        TGuard g{state.Lock};

        // The interval may have rolled between RollIntervals() and taking
        // the state lock - retry so that the event can never be applied to
        // an interval that has already ended.
        if (now.MicroSeconds() >=
            CurrentIntervalEndUs.load(std::memory_order_acquire))
        {
            continue;
        }

        DoRequestCompleted(callContext, state);
        return;
    }
}

// Protected by state.Lock.
void TAvailabilityCounters::DoRequestCompleted(
    TCallContext& callContext,
    TRequestTypeState& state)
{
    // The stamp is put there by RequestStarted() and consumed below by the
    // first completion. A zero stamp means the request either started
    // before the tracking was enabled or was already completed, it is
    // ignored either way.
    if (callContext.AvailabilityIntervalSeqNo == 0) {
        return;
    }

    const ui64 startSeqNo = callContext.AvailabilityIntervalSeqNo;
    callContext.AvailabilityIntervalSeqNo = 0;

    Y_DEBUG_ABORT_UNLESS(state.Inflight);
    if (state.Inflight > 0) {
        --state.Inflight;
    }

    if (startSeqNo == state.IntervalSeqNo) {
        Y_DEBUG_ABORT_UNLESS(state.InflightStartedInInterval);
        // The request started in the current interval and thus can no longer
        // be counted as a fresh outstanding request at the interval end.
        if (state.InflightStartedInInterval > 0) {
            --state.InflightStartedInInterval;
        }
    }

    if (callContext.GuestReplyErrno == EIO) {
        ++state.CompletedEio;
    } else {
        ++state.CompletedNonEio;
    }
}

void TAvailabilityCounters::UpdateStats(TInstant now)
{
    if (!CountersRegistered.load(std::memory_order_acquire)) {
        return;
    }

    RollIntervals(now);
}

void TAvailabilityCounters::RollIntervals(TInstant now)
{
    // Fast path: an event or update well inside the current interval.
    if (now.MicroSeconds() <
        CurrentIntervalEndUs.load(std::memory_order_acquire))
    {
        return;
    }

    TGuard g{RollLock};

    if (!CurrentIntervalStart) {
        // First event or update - start measuring from the current
        // wall-clock-aligned interval boundary so that intervals of all
        // clients are aligned. If the measurement does not begin exactly at
        // a boundary, the first interval is only partially observed and is
        // rolled over without classification below.
        CurrentIntervalStart = AlignToInterval(now);
        SkipCurrentInterval = now != CurrentIntervalStart;
        CurrentIntervalEndUs.store(
            (CurrentIntervalStart + IntervalDuration).MicroSeconds(),
            std::memory_order_release);
        return;
    }

    if (SkipCurrentInterval &&
        now >= CurrentIntervalStart + IntervalDuration)
    {
        // A partially observed interval (the one during which the
        // measurement began mid-interval) is rolled over - accounting
        // resets, outstanding requests become old - but is not classified
        // or counted: neither the per-type nor the aggregated counters are
        // published for it.
        SkipCurrentInterval = false;
        RollInterval(false /* publishCounters */);
    }

    size_t finishedIntervals = 0;
    while (now >= CurrentIntervalStart + IntervalDuration &&
           finishedIntervals < MaxIntervalsPerUpdate)
    {
        // More than one interval may have elapsed if neither the stats
        // updater nor any request event has rolled the accounting for a
        // while. The first iteration evaluates all the accumulated data;
        // subsequent iterations see empty per-interval counters, so a gap
        // interval is classified as unavailable iff some requests remained
        // outstanding (i.e. hung) throughout it.
        RollInterval(true /* publishCounters */);
        ++finishedIntervals;
    }

    if (now >= CurrentIntervalStart + IntervalDuration) {
        // Too many intervals elapsed in a single update (e.g. after a large
        // clock jump) - realign without evaluating the remaining ones, but
        // report how many were skipped so they are not silently lost from
        // the SLA denominator.
        const TInstant alignedNow = AlignToInterval(now);
        const ui64 missingIntervals =
            (alignedNow - CurrentIntervalStart).MicroSeconds() /
            IntervalDuration.MicroSeconds();
        MissingIntervalsCounter->Add(missingIntervals);

        CurrentIntervalStart = alignedNow;
    }

    CurrentIntervalEndUs.store(
        (CurrentIntervalStart + IntervalDuration).MicroSeconds(),
        std::memory_order_release);
}

void TAvailabilityCounters::RollInterval(bool publishCounters)
{
    bool intervalAvailable = true;

    // Index 0 is EFileStoreAvailabilityRequestType::None, stays unused.
    for (size_t i = 1; i < RequestTypeStates.size(); ++i) {
        if (!RollRequestTypeStateAndReturnAvailability(
                RequestTypeStates[i],
                publishCounters))
        {
            // The aggregated interval availability is the logical AND over
            // the request types.
            intervalAvailable = false;
        }
    }

    if (publishCounters) {
        TotalIntervalsCounter->Inc();
        if (intervalAvailable) {
            AvailableIntervalsCounter->Inc();
        } else {
            UnavailableIntervalsCounter->Inc();
        }
        *LastIntervalAvailableCounter = intervalAvailable ? 1 : 0;
    }

    CurrentIntervalStart += IntervalDuration;
}

bool TAvailabilityCounters::RollRequestTypeStateAndReturnAvailability(
    TRequestTypeState& state,
    bool publishCounters)
{
    TGuard g{state.Lock};

    // Inflight >= InflightStartedInInterval holds but the subtraction is
    // clamped defensively so that a violation can never wrap it into a huge
    // hung count.
    Y_DEBUG_ABORT_UNLESS(state.Inflight >= state.InflightStartedInInterval);
    // Requests that were outstanding at the interval start and are still
    // outstanding at the interval end, i.e. remained outstanding throughout
    // the entire interval => hung.
    const ui64 hung = state.Inflight >= state.InflightStartedInInterval
        ? state.Inflight - state.InflightStartedInInterval
        : 0;

    // A request type makes the interval unavailable if it shows failure
    // evidence - at least one request completed with an EIO error during
    // the interval or hung through it - and no success evidence - no
    // request completed with a non-EIO outcome during the interval.
    // Requests that were started during the interval and have not completed
    // by its end count as neither: their outcome is not known yet and will
    // be accounted for in the interval where they complete (or in the
    // intervals they hang through).
    const bool hadFailedRequest = state.CompletedEio > 0 || hung > 0;
    const bool hadNonFailedRequest = state.CompletedNonEio > 0;
    const bool available = !hadFailedRequest || hadNonFailedRequest;

    if (publishCounters) {
        if (available) {
            state.AvailableIntervalsCounter->Inc();
        } else {
            state.UnavailableIntervalsCounter->Inc();
        }
        *state.LastIntervalAvailableCounter = available ? 1 : 0;
    }

    // Roll over to the next interval. All requests still outstanding become
    // "outstanding since the interval start" for the new interval.
    state.CompletedNonEio = 0;
    state.CompletedEio = 0;
    state.InflightStartedInInterval = 0;
    ++state.IntervalSeqNo;

    return available;
}

TInstant TAvailabilityCounters::AlignToInterval(TInstant instant) const
{
    const ui64 interval = IntervalDuration.MicroSeconds();
    return TInstant::MicroSeconds(
        instant.MicroSeconds() / interval * interval);
}

}   // namespace NCloud::NFileStore
