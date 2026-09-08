#pragma once

#include "public.h"

#include <cloud/blockstore/config/diagnostics.pb.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/protos/media.pb.h>

#include <library/cpp/threading/hot_swap/hot_swap.h>

#include <util/datetime/base.h>
#include <util/generic/ptr.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/system/yassert.h>

#include <array>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

// A single operation-size bucket: the lower bound (inclusive) of the
// operation size range together with the execution time thresholds for read
// and write operations of that size. An operation is considered timely if it
// completes within the threshold configured for its size class. The upper
// bound of a bucket is the next bucket's MinRequestBytes (open-ended for the
// last bucket of a media kind).
struct TLatencyThresholdBucket
{
    ui64 MinRequestBytes = 0;
    TDuration ReadThreshold;
    TDuration WriteThreshold;
};

// Ascending-by-MinRequestBytes ladder of buckets for a single media kind.
// Non-empty and ladder.front().MinRequestBytes == 0 are invariants guaranteed
// by BuildLatencyThresholdsTable for every ladder it stores in a
// TLatencyThresholdsTable.
using TLatencyThresholdLadder = TVector<TLatencyThresholdBucket>;

////////////////////////////////////////////////////////////////////////////////

struct TLatencyThresholdsValidationResult;

// Immutable, validated snapshot of the whole latency thresholds table. Built
// once from config and stored behind a THotSwap (see TLatencyThresholdsHotSwap
// below) so that a future dynamic config source can replace it without
// touching the request-completion hot path.
class TLatencyThresholdsTable
    : public TAtomicRefCount<TLatencyThresholdsTable>
{
public:
    // Returns nullptr if the media kind has no configured ladder (including
    // out-of-range values). Callers must treat "no ladder" as "do not judge
    // this operation at all", not as "bad operation".
    [[nodiscard]] const TLatencyThresholdLadder* FindLadder(
        NCloud::NProto::EStorageMediaKind mediaKind) const;

private:
    // Indexed by NCloud::NProto::EStorageMediaKind. A media kind with an
    // empty ladder has no configured thresholds: operations of that media
    // kind must be skipped entirely (neither total nor good), not treated as
    // bad - see FindLadder.
    std::array<
        TLatencyThresholdLadder,
        NCloud::NProto::EStorageMediaKind_ARRAYSIZE> Ladders;

    // Filling the table is the validating builder's job alone, so that a
    // published snapshot really is immutable: THotSwap synchronizes
    // replacing the pointer, not writes to the object it points at, and a
    // holder able to write here would race with the request path once a
    // dynamic config source starts replacing tables.
    friend TLatencyThresholdsValidationResult BuildLatencyThresholdsTable(
        const TVector<NProto::TMediaKindLatencyThresholds>& config);
};

// Shared, hot-swappable holder for the current table. A single instance is
// owned by the volume stats component and referenced (via shared_ptr) by
// every per-volume object, mirroring how THotSwap<TVolumePerfSettings> is
// used for PerfSettings on the same request-completion path (see
// volume_perf.h). Read access is a single AtomicLoad() per operation.
using TLatencyThresholdsHotSwap = THotSwap<TLatencyThresholdsTable>;

////////////////////////////////////////////////////////////////////////////////

struct TLatencyThresholdsValidationResult
{
    // Non-null only if the table is structurally valid (Error is empty).
    TIntrusivePtr<TLatencyThresholdsTable> Table;

    // First rule violation found; empty if valid. Validation stops at the
    // first violation, so only one error is ever reported per call.
    TString Error;

    // Non-fatal notices (currently: thresholds that are not non-decreasing
    // with operation size within a media kind). The table is still usable
    // when only warnings are present.
    TVector<TString> Warnings;

    [[nodiscard]] bool IsValid() const
    {
        return Error.empty();
    }
};

// Validates the raw proto config and builds the runtime lookup table.
// Rules, in the order they are checked:
//   - the list is not empty (an enabled mechanism with an empty table is a
//     config mistake, not "disable via empty table");
//   - every entry sets MediaKind explicitly (the field is optional, so an
//     entry that omits it would parse as STORAGE_MEDIA_DEFAULT);
//   - MediaKind is within the declared enum range (it indexes a fixed-size
//     array of ladders);
//   - a media kind appears at most once in the list;
//   - every media kind entry has at least one bucket;
//   - the first bucket of every media kind has MinRequestBytes == 0;
//   - MinRequestBytes strictly increases within a media kind (no
//     duplicates, no reordering; never silently sorted);
//   - both thresholds in every bucket are > 0.
// Non-decreasing thresholds with size (the "running max" contract) is
// checked too, but only produces a Warning, not an Error.
TLatencyThresholdsValidationResult BuildLatencyThresholdsTable(
    const TVector<NProto::TMediaKindLatencyThresholds>& config);

////////////////////////////////////////////////////////////////////////////////

// Finds the bucket for requestBytes using [MinRequestBytes[i],
// MinRequestBytes[i+1)) semantics: the returned bucket is the one with the
// largest MinRequestBytes <= requestBytes ("last lower bound not exceeding
// the size"), and the last bucket is implicitly open-ended upward.
//
// A naive std::lower_bound over the lower bounds is *not* equivalent to this:
// it returns the first bound >= requestBytes, which for a size strictly
// between two boundaries names the *next* (bigger) bucket, and for a size
// above the last boundary returns end() (out of bounds). Both failure modes
// bias toward a bigger/softer threshold than intended, i.e. the reported
// metric would look better than reality. The correct idiom is
// upper_bound(...) - 1: "the last boundary <= requestBytes".
//
// ladder must be non-empty with ladder.front().MinRequestBytes == 0 (both
// guaranteed by BuildLatencyThresholdsTable), so upper_bound never returns
// begin() and the decrement below is always safe.
const TLatencyThresholdBucket& FindLatencyThresholdBucket(
    const TLatencyThresholdLadder& ladder,
    ui64 requestBytes);

////////////////////////////////////////////////////////////////////////////////

// Outcome of judging a single completed read/write operation against the
// latency thresholds table. Fatal errors are always bad (nothing to compare
// a duration against); throttling/checkpoint rejections and retriable
// outcomes are excluded entirely (not judged, not counted as bad);
// everything else is judged by comparing execution time against the
// threshold for its media kind, direction (read/write), and size bucket.
// See ClassifyLatencyOutcome for a known gap in the retriable case: an
// operation whose retries are exhausted contributes to neither counter.
struct TLatencyThresholdOutcome
{
    // Operation counted in the total (denominator).
    bool CountTotal = false;

    // Operation counted as good (numerator); only meaningful if CountTotal.
    bool CountGood = false;

    // The operation's media kind has no configured ladder at all: the
    // operation was skipped entirely (neither total nor good). Callers
    // should tally this separately (a diagnostic "skipped" counter), never
    // as a bad operation - see TLatencyThresholdsTable::FindLadder.
    bool MediaKindNotConfigured = false;
};

// Pure classification function, no side effects (the caller performs the
// actual counter increments). `ladder` is nullptr when the operation's media
// kind has no configured thresholds at all.
TLatencyThresholdOutcome ClassifyLatencyOutcome(
    const TLatencyThresholdLadder* ladder,
    EDiagnosticsErrorKind errorKind,
    bool isWrite,
    ui64 requestBytes,
    TDuration execTime);

}   // namespace NCloud::NBlockStore
