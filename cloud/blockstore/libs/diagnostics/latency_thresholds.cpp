#include "latency_thresholds.h"

#include <util/generic/algorithm.h>
#include <util/generic/hash_set.h>
#include <util/generic/string.h>
#include <util/string/builder.h>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

const TLatencyThresholdLadder* TLatencyThresholdsTable::FindLadder(
    NCloud::NProto::EStorageMediaKind mediaKind) const
{
    if (mediaKind < 0 ||
        static_cast<size_t>(mediaKind) >= Ladders.size())
    {
        return nullptr;
    }

    const auto& ladder = Ladders[mediaKind];
    return ladder.empty() ? nullptr : &ladder;
}

////////////////////////////////////////////////////////////////////////////////

TLatencyThresholdsValidationResult BuildLatencyThresholdsTable(
    const TVector<NProto::TMediaKindLatencyThresholds>& config)
{
    TLatencyThresholdsValidationResult result;

    // An enabled mechanism with an empty table is a config mistake, not a
    // way to disable the mechanism (the flag itself already does that).
    // Left invalid so the caller keeps the mechanism off and logs it.
    if (config.empty()) {
        result.Error = "LatencyThresholds is empty";
        return result;
    }

    auto table = MakeIntrusive<TLatencyThresholdsTable>();
    THashSet<int> seenMediaKinds;

    for (const auto& mediaKindThresholds: config) {
        // MediaKind is an optional field, so an entry that omits it parses
        // cleanly and reads back as STORAGE_MEDIA_DEFAULT. That would
        // silently calibrate a media kind nobody asked for while leaving
        // the intended one unjudged, with the config reported as valid. An
        // entry that spells out STORAGE_MEDIA_DEFAULT is still accepted:
        // volumes whose config leaves the media kind unset do exist.
        if (!mediaKindThresholds.HasMediaKind()) {
            result.Error = "missing MediaKind in a LatencyThresholds entry";
            return result;
        }

        const auto mediaKind = mediaKindThresholds.GetMediaKind();

        // mediaKind indexes a fixed-size array of ladders below, so it is
        // bounds-checked here rather than trusted. Today the field cannot
        // hold an out-of-range value: diagnostics.proto is proto2, whose
        // enum fields are closed, so an unrecognized number from a config
        // or from the wire is kept in the unknown-field set and the getter
        // returns the default instead. That guarantee comes from the file's
        // syntax, not from anything visible at this call site, so the check
        // keeps the indexing safe if the enum ever becomes open
        // (proto3/editions), where an out-of-range index would be UB.
        if (!NCloud::NProto::EStorageMediaKind_IsValid(mediaKind)) {
            result.Error = TStringBuilder()
                << "invalid media kind " << static_cast<int>(mediaKind)
                << " in LatencyThresholds";
            return result;
        }

        const TString mediaKindName =
            NCloud::NProto::EStorageMediaKind_Name(mediaKind);

        // A media kind must not repeat within the list.
        if (!seenMediaKinds.insert(static_cast<int>(mediaKind)).second) {
            result.Error = TStringBuilder()
                << "duplicate media kind " << mediaKindName
                << " in LatencyThresholds";
            return result;
        }

        // Every media kind needs at least one bucket.
        if (mediaKindThresholds.BucketsSize() == 0) {
            result.Error = TStringBuilder()
                << "media kind " << mediaKindName
                << " has no buckets in LatencyThresholds";
            return result;
        }

        if (static_cast<size_t>(mediaKindThresholds.BucketsSize()) >
            MaxLatencyThresholdBucketsPerMediaKind)
        {
            result.Error = TStringBuilder()
                << "media kind " << mediaKindName << " has "
                << mediaKindThresholds.BucketsSize()
                << " buckets in LatencyThresholds; maximum is "
                << MaxLatencyThresholdBucketsPerMediaKind;
            return result;
        }

        // Safe to index Ladders with mediaKind: bounds-checked above.
        TLatencyThresholdLadder ladder;
        ladder.reserve(mediaKindThresholds.BucketsSize());

        ui32 lastReadThresholdMs = 0;
        ui32 lastWriteThresholdMs = 0;

        for (const auto& bucket: mediaKindThresholds.GetBuckets()) {
            const bool first = ladder.empty();

            // The first bucket must start at 0, so that no operation
            // size falls outside of every bucket (this is what makes the
            // ladder total, not just "the ranges we happened to calibrate").
            if (first && bucket.GetMinRequestBytes() != 0) {
                result.Error = TStringBuilder()
                    << "media kind " << mediaKindName
                    << ": first bucket MinRequestBytes must be 0, got "
                    << bucket.GetMinRequestBytes();
                return result;
            }

            // Strictly increasing lower bounds, never silently
            // sorted - a disordered/duplicated config is a typo, not
            // something to paper over.
            if (!first &&
                bucket.GetMinRequestBytes() <= ladder.back().MinRequestBytes)
            {
                result.Error = TStringBuilder()
                    << "media kind " << mediaKindName
                    << ": MinRequestBytes must be strictly increasing, "
                    << bucket.GetMinRequestBytes() << " does not follow "
                    << ladder.back().MinRequestBytes;
                return result;
            }

            // A zero threshold would fail every operation of that
            // size class outright.
            if (bucket.GetReadThresholdMs() == 0 ||
                bucket.GetWriteThresholdMs() == 0)
            {
                result.Error = TStringBuilder()
                    << "media kind " << mediaKindName << ": bucket at "
                    << bucket.GetMinRequestBytes()
                    << " has a zero threshold";
                return result;
            }

            // Running-max (monotonicity) contract: a violation is not
            // mechanically dangerous and a hard failure here could block an
            // urgent config fix, so this is a warning, not an error.
            if (!first &&
                (bucket.GetReadThresholdMs() < lastReadThresholdMs ||
                 bucket.GetWriteThresholdMs() < lastWriteThresholdMs))
            {
                result.Warnings.push_back(TStringBuilder()
                    << "media kind " << mediaKindName
                    << ": thresholds are not non-decreasing at bucket "
                    << bucket.GetMinRequestBytes()
                    << " (running-max contract violated)");
            }

            ladder.push_back(TLatencyThresholdBucket{
                .MinRequestBytes = bucket.GetMinRequestBytes(),
                .ReadThreshold =
                    TDuration::MilliSeconds(bucket.GetReadThresholdMs()),
                .WriteThreshold =
                    TDuration::MilliSeconds(bucket.GetWriteThresholdMs()),
            });

            lastReadThresholdMs =
                Max(lastReadThresholdMs, bucket.GetReadThresholdMs());
            lastWriteThresholdMs =
                Max(lastWriteThresholdMs, bucket.GetWriteThresholdMs());
        }

        table->Ladders[mediaKind] = std::move(ladder);
    }

    result.Table = std::move(table);
    return result;
}

////////////////////////////////////////////////////////////////////////////////

const TLatencyThresholdBucket& FindLatencyThresholdBucket(
    const TLatencyThresholdLadder& ladder,
    ui64 requestBytes)
{
    Y_DEBUG_ABORT_UNLESS(!ladder.empty());
    Y_DEBUG_ABORT_UNLESS(ladder.front().MinRequestBytes == 0);

    // upper_bound(requestBytes) - 1 == "the last bucket whose
    // MinRequestBytes <= requestBytes". See the header comment for why a
    // plain lower_bound over the lower bounds is wrong here.
    auto it = UpperBound(
        ladder.begin(),
        ladder.end(),
        requestBytes,
        [](ui64 bytes, const TLatencyThresholdBucket& bucket)
        { return bytes < bucket.MinRequestBytes; });
    --it;
    return *it;
}

////////////////////////////////////////////////////////////////////////////////

TLatencyThresholdOutcome ClassifyLatencyOutcome(
    const TLatencyThresholdLadder* ladder,
    const NProto::TError& error,
    bool isWrite,
    ui64 requestBytes,
    TDuration latency)
{
    // A media kind without a configured ladder is not judged at all: not
    // counting it (rather than counting it as bad) avoids showing a bogus
    // 0% good-operation rate for media kinds nobody has calibrated yet. This
    // gate applies unconditionally, before looking at errorKind, so it also
    // covers ErrorFatal operations.
    if (!ladder) {
        return {.CountSkipped = true};
    }

    const auto errorKind = GetDiagnosticsErrorKind(error);

    // These outcomes do not describe a storage operation whose latency can
    // fairly be judged: the service explicitly refused to start it or its
    // owner cancelled it. E_ARGUMENT is deliberately not excluded here: the
    // same code is also used for invalid service responses after a valid
    // operation has executed, so a final E_ARGUMENT at this boundary is a
    // service failure unless the endpoint recorded a known pre-execution
    // rejection explicitly.
    if (errorKind == EDiagnosticsErrorKind::ErrorThrottling ||
        errorKind == EDiagnosticsErrorKind::ErrorWriteRejectedByCheckpoint ||
        error.GetCode() == E_CANCELLED)
    {
        return {.CountSkipped = true};
    }

    // At this API boundary the error is the final logical outcome. Retriable,
    // session, aborted-transport and silent errors are therefore terminal for
    // this operation (including retry exhaustion), rather than intermediate
    // attempts to exclude.
    if (HasError(error)) {
        return {.CountTotal = true};
    }

    switch (errorKind) {
        case EDiagnosticsErrorKind::Success:
            break;

        // A successful error code is the source of truth. The remaining
        // diagnostic kinds are defensive fallbacks for inconsistent input;
        // count such an operation as failed instead of allowing it into the
        // numerator.
        case EDiagnosticsErrorKind::ErrorThrottling:
        case EDiagnosticsErrorKind::ErrorWriteRejectedByCheckpoint:
        case EDiagnosticsErrorKind::ErrorRetriable:
        case EDiagnosticsErrorKind::ErrorSession:
        case EDiagnosticsErrorKind::ErrorAborted:
        case EDiagnosticsErrorKind::ErrorSilent:
        case EDiagnosticsErrorKind::ErrorFatal:
        case EDiagnosticsErrorKind::Max:
            return {.CountTotal = true};
    }

    const auto& bucket = FindLatencyThresholdBucket(*ladder, requestBytes);
    const auto threshold =
        isWrite ? bucket.WriteThreshold : bucket.ReadThreshold;

    return {
        .CountTotal = true,
        // Non-strict comparison: an operation exactly at the threshold is
        // good.
        .CountGood = latency <= threshold,
    };
}

}   // namespace NCloud::NBlockStore
