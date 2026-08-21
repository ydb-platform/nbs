#pragma once

#include "public.h"

#include <cloud/filestore/libs/service/context.h>
#include <cloud/filestore/libs/service/request.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>

#include <util/datetime/base.h>
#include <util/system/guard.h>
#include <util/system/spinlock.h>
#include <util/system/types.h>

#include <array>

namespace NCloud::NFileStore {

////////////////////////////////////////////////////////////////////////////////

// The value of the "request" label of the per-request-type availability
// sensors: the FUSE request name in lower case, as listed in the SLA (e.g.
// "lookup", "write_buf").
const char* GetAvailabilityRequestTypeName(
    EFileStoreAvailabilityRequestType requestType);

////////////////////////////////////////////////////////////////////////////////

// Implements the per-client (instance, filesystem) availability metric on top
// of the request stats hooks.
//
// Definitions
//
// A Shared Filesystem is considered unavailable during any N-minute interval in
// which, for at least one filesystem request type, at least one request was
// outstanding and every such request either failed with an EIO error, or hung.
//
// In all N-minute intervals in which the unavailability condition is not met,
// a Shared Filesystem is considered available.
//
// Shared Filesystem availability for the measurement period shall be calculated
// as:
// Number of Available intervals / Total number of intervals in the measurement
// period × 100%
//
// A request is outstanding at a given point in time if it was submitted but had
// received no response, success or error, by that point.
// A request is hung if it was outstanding for the entire duration of the N-minute
// interval.
//
// Request type - an individual FUSE request type subject to the SLA, see
// EFileStoreAvailabilityRequestType. Distinct FUSE request types are
// accounted independently even when they map to the same backend request
// type, and requests outside the SLA (AvailabilityRequestType == None) are
// ignored entirely.
//
// The EIO classification is based on TCallContext::GuestReplyErrno - the
// errno actually sent to the guest - because the internal request error does
// not always match the guest-visible outcome.
//
// Published sensors (registered on the per-filesystem per-client counters
// subgroup of the request stats):
//  * Availability_TotalIntervals       - derivative, finished 5-min intervals;
//  * Availability_AvailableIntervals   - derivative, available intervals;
//  * Availability_UnavailableIntervals - derivative, unavailable intervals;
//  * Availability_LastIntervalAvailable - gauge, 1 if the last finished
//                                        interval was available, 0 otherwise;
// Availability_{Available,Unavailable}Intervals and
// Availability_LastIntervalAvailable are also published per availability
// request type, on the "request=<type>" subgroup (e.g. request=lookup):
// there an interval is available if that request type alone shows no
// unavailability evidence.
//
// The aggregated availability sensors are the logical AND over the request
// types: an interval is available overall iff it is available for every
// request type.
//
//  * Availability_MissingIntervals     - derivative, elapsed intervals that
//                                        were not evaluated because a single
//                                        update had to catch up with more
//                                        than MaxIntervalsPerUpdate of them
//                                        (e.g. after a large clock jump).
//                                        Such intervals are excluded from
//                                        Availability_TotalIntervals, so the
//                                        SLA computation can decide how to
//                                        account for them. Aggregated only:
//                                        missed intervals are a property of
//                                        the updater and are identical for
//                                        all request types.
//
// Filesystem availability for an arbitrary measurement period is then
// increment(Availability_AvailableIntervals) /
// increment(Availability_TotalIntervals) * 100%.
class TAvailabilityCounters
{
private:
    // Aligned to a cache line to avoid false sharing between request types.
    struct alignas(64) TRequestTypeState
    {
        TAdaptiveLock Lock;

        // Total number of outstanding requests of this type. Never reset.
        ui64 Inflight = 0;

        // The counters below describe the current (not yet finished)
        // interval and are reset by FinishInterval().

        // Outstanding requests that were started in the current interval.
        // The remaining (Inflight - InflightStartedInInterval) requests were
        // started in earlier intervals, i.e. have been outstanding since the
        // beginning of the current interval and will be classified as hung
        // if they do not complete before the interval ends.
        ui64 InflightStartedInInterval = 0;

        // Requests that reached a terminal outcome other than an EIO error
        // response during the current interval (successful completions and
        // completions with any other error response).
        ui64 CompletedNonEio = 0;

        // Requests that completed with an EIO error response during the
        // current interval.
        ui64 CompletedEio = 0;

        // Sequence number of the current interval, incremented by
        // FinishInterval(). Used to detect whether a completing request was
        // started in the current interval.
        ui64 IntervalSeqNo = 0;

        // Per-request-type published counters (on the "request=<type>"
        // subgroup). Unset until Register() is called.
        NMonitoring::TDynamicCounters::TCounterPtr AvailableIntervalsCounter;
        NMonitoring::TDynamicCounters::TCounterPtr
            UnavailableIntervalsCounter;
        // Gauge counter.
        NMonitoring::TDynamicCounters::TCounterPtr
            LastIntervalAvailableCounter;
    };

private:
    const TDuration IntervalDuration;

    std::array<TRequestTypeState, FileStoreAvailabilityRequestTypeCount>
        RequestTypeStates;

    // Only accessed from UpdateStats().
    TInstant CurrentIntervalStart;

    // Published aggregated counters.
    NMonitoring::TDynamicCounters::TCounterPtr TotalIntervalsCounter;
    NMonitoring::TDynamicCounters::TCounterPtr AvailableIntervalsCounter;
    NMonitoring::TDynamicCounters::TCounterPtr UnavailableIntervalsCounter;
    NMonitoring::TDynamicCounters::TCounterPtr LastIntervalAvailableCounter;
    NMonitoring::TDynamicCounters::TCounterPtr MissingIntervalsCounter;

    bool CountersRegistered = false;

public:
    explicit TAvailabilityCounters(TDuration intervalDuration);

    void Register(NMonitoring::TDynamicCounters& counters);

    void RequestStarted(TCallContext& callContext);

    void RequestCompleted(TCallContext& callContext);

    // Rolls availability intervals over. Invoked periodically from the
    // stats-updater thread.
    void UpdateStats(TInstant now);

private:
    void FinishInterval();

    TInstant AlignToInterval(TInstant instant) const;
};

}   // namespace NCloud::NFileStore
