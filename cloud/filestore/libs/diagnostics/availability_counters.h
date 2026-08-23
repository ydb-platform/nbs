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
#include <atomic>

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
//  * Availability_TotalIntervals       - derivative, finished intervals;
//  * Availability_AvailableIntervals   - derivative, available intervals;
//  * Availability_UnavailableIntervals - derivative, unavailable intervals;
//  * Availability_LastIntervalAvailable - gauge, 1 if the last evaluated
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
        // interval and are reset when the interval is rolled over.

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

        // Sequence number of the current interval (availability intervals
        // are numbered starting from 1), incremented by the interval
        // rollover. Used to detect whether a completing request was started
        // in the current interval.
        ui64 IntervalSeqNo = 1;

        // Per-request-type published counters (on the "request=<type>"
        // subgroup). Unset until EnableAndRegister() is called.
        NMonitoring::TDynamicCounters::TCounterPtr AvailableIntervalsCounter;
        NMonitoring::TDynamicCounters::TCounterPtr
            UnavailableIntervalsCounter;
        // Gauge counter.
        NMonitoring::TDynamicCounters::TCounterPtr
            LastIntervalAvailableCounter;
    };

private:
    // Assigned by EnableAndRegister().
    TDuration IntervalDuration;

    std::array<TRequestTypeState, FileStoreAvailabilityRequestTypeCount>
        RequestTypeStates;

    // Guarded by RollLock.
    TInstant CurrentIntervalStart;

    // The first interval after enabling may begin mid-interval (measurement
    // starts at the enabling instant, aligned backward): such a partially
    // observed interval is rolled over without classification.
    //
    // Guarded by RollLock.
    bool SkipCurrentInterval = false;

    // End of the current interval in microseconds, 0 until the first event
    // or update initializes the measurement.
    std::atomic<ui64> CurrentIntervalEndUs = 0;

    // Serializes interval rolling, see RollIntervals().
    TAdaptiveLock RollLock;

    // Published aggregated counters.
    NMonitoring::TDynamicCounters::TCounterPtr TotalIntervalsCounter;
    NMonitoring::TDynamicCounters::TCounterPtr AvailableIntervalsCounter;
    NMonitoring::TDynamicCounters::TCounterPtr UnavailableIntervalsCounter;
    NMonitoring::TDynamicCounters::TCounterPtr LastIntervalAvailableCounter;
    NMonitoring::TDynamicCounters::TCounterPtr MissingIntervalsCounter;

    // Set by EnableAndRegister() with release ordering once the interval
    // duration and the sensors are fully initialized, the methods below
    // load it with acquire ordering and are no-ops until then.
    std::atomic<bool> CountersRegistered = false;

    // Serializes EnableAndRegister() calls.
    TAdaptiveLock EnableLock;

    TString FileSystemId;

public:
    // Registers the sensors and enables the tracking with the given
    // interval duration (zero selects the default one).
    void EnableAndRegister(
        TString fileSystemId,
        TDuration intervalDuration,
        NMonitoring::TDynamicCounters& counters);

    // The request hooks take the actual event time and assign the event to
    // its wall-clock interval: elapsed intervals are rolled over first, so
    // an event arriving after an interval boundary but before the periodic
    // updater tick is never attributed to the interval that has already
    // ended.
    void RequestStarted(TCallContext& callContext, TInstant now);

    void RequestCompleted(TCallContext& callContext, TInstant now);

    // Rolls availability intervals over. Invoked periodically from the
    // stats-updater thread; the request hooks roll on demand as well, so
    // this only bounds the classification latency of quiet periods.
    void UpdateStats(TInstant now);

private:
    // Rolls all the intervals that have fully elapsed by the given
    // instant: classifies and publishes each one (except a partially
    // observed first interval), bounded by MaxIntervalsPerUpdate with the
    // overflow reported as missing intervals.
    void RollIntervals(TInstant now);

    void DoRequestStarted(
        TCallContext& callContext,
        TRequestTypeState& state);

    void DoRequestCompleted(
        TCallContext& callContext,
        TRequestTypeState& state);

    // Finishes the current interval and advances to the next one: rolls
    // every request type state over and, when publishCounters is set,
    // classifies the interval and publishes both the per-type and the
    // aggregated counters.
    //
    // Called under RollLock.
    void RollInterval(bool publishCounters);

    // Rolls the state of one request type over to the next interval and
    // returns whether the finished interval was available for it.
    bool RollRequestTypeStateAndReturnAvailability(
        TRequestTypeState& state,
        bool publishCounters);

    TInstant AlignToInterval(TInstant instant) const;
};

}   // namespace NCloud::NFileStore
