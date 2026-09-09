#pragma once

#include "public.h"

#include <cloud/blockstore/config/diagnostics.pb.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/protos/media.pb.h>

#include <util/datetime/base.h>
#include <util/generic/ptr.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/system/yassert.h>

#include <array>
#include <cstddef>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

// Keeps the external-vhost v1 environment value well below Linux's per-string
// exec limit even when every numeric field uses its maximum decimal width.
inline constexpr size_t MaxLatencyThresholdBucketsPerMediaKind = 1024;

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

// Immutable, validated snapshot of the whole latency thresholds table. It is
// built once from startup config and then shared by all per-volume objects.
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
    // published snapshot really is immutable.
    friend TLatencyThresholdsValidationResult BuildLatencyThresholdsTable(
        const TVector<NProto::TMediaKindLatencyThresholds>& config);
};

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
//   - every media kind entry has at most
//     MaxLatencyThresholdBucketsPerMediaKind buckets;
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

// Outcome of judging one final logical read/write operation after all
// splitting and retries have completed. Successful operations are compared
// with the size-dependent threshold. A final service failure is bad. Explicit
// load-shedding/checkpoint rejections, invalid input and cancellation are
// excluded from latency accounting.
struct TLatencyThresholdOutcome
{
    // Operation counted in the total (denominator).
    bool CountTotal = false;

    // Operation counted as good (numerator); only meaningful if CountTotal.
    bool CountGood = false;

    // The operation was deliberately excluded (neither total nor good).
    // This covers both an unconfigured media kind and a final outcome that
    // is outside the latency counter contract. Callers tally it in the diagnostic
    // skipped counter.
    bool CountSkipped = false;
};

// Pure classification function, no side effects (the caller performs the
// actual counter increments). `ladder` is nullptr when the operation's media
// kind has no configured thresholds at all. `error` is the final result
// visible at the endpoint boundary, not an individual server attempt.
TLatencyThresholdOutcome ClassifyLatencyOutcome(
    const TLatencyThresholdLadder* ladder,
    const NProto::TError& error,
    bool isWrite,
    ui64 requestBytes,
    TDuration execTime);

}   // namespace NCloud::NBlockStore
