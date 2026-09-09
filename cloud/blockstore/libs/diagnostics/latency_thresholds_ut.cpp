#include "latency_thresholds.h"

#include <cloud/storage/core/protos/media.pb.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/size_literals.h>

namespace NCloud::NBlockStore {

namespace {

////////////////////////////////////////////////////////////////////////////////

using EStorageMediaKind = NCloud::NProto::EStorageMediaKind;

NProto::TLatencyThresholdBucket MakeBucket(
    ui64 minRequestBytes,
    ui32 readThresholdMs,
    ui32 writeThresholdMs)
{
    NProto::TLatencyThresholdBucket bucket;
    bucket.SetMinRequestBytes(minRequestBytes);
    bucket.SetReadThresholdMs(readThresholdMs);
    bucket.SetWriteThresholdMs(writeThresholdMs);
    return bucket;
}

NProto::TMediaKindLatencyThresholds MakeMediaKindThresholds(
    EStorageMediaKind mediaKind,
    const TVector<NProto::TLatencyThresholdBucket>& buckets)
{
    NProto::TMediaKindLatencyThresholds mkt;
    mkt.SetMediaKind(mediaKind);
    for (const auto& bucket: buckets) {
        *mkt.AddBuckets() = bucket;
    }
    return mkt;
}

// A single-media-kind, two-bucket config valid enough to exercise the
// bucket-lookup and accounting logic: [0, 8_KB) and [8_KB, +inf), with
// distinct read/write thresholds so the two are never accidentally swapped.
TVector<NProto::TMediaKindLatencyThresholds> MakeValidConfig(
    EStorageMediaKind mediaKind = NCloud::NProto::STORAGE_MEDIA_SSD)
{
    return {MakeMediaKindThresholds(
        mediaKind,
        {
            MakeBucket(0, 10, 20),
            MakeBucket(8_KB, 100, 200),
        })};
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TLatencyThresholdsValidationTest)
{
    Y_UNIT_TEST(ShouldRejectEmptyConfig)
    {
        auto result = BuildLatencyThresholdsTable({});

        UNIT_ASSERT(!result.IsValid());
        UNIT_ASSERT(!result.Error.empty());
        UNIT_ASSERT(!result.Table);
    }

    Y_UNIT_TEST(ShouldRejectEntryWithoutMediaKind)
    {
        // MediaKind is optional in the proto, so an entry that omits it
        // parses cleanly and reads back as STORAGE_MEDIA_DEFAULT instead of
        // failing - the validator has to reject it explicitly.
        NProto::TMediaKindLatencyThresholds mkt;
        *mkt.AddBuckets() = MakeBucket(0, 10, 10);

        auto result = BuildLatencyThresholdsTable({mkt});

        UNIT_ASSERT(!result.IsValid());
        UNIT_ASSERT(!result.Error.empty());
        UNIT_ASSERT(!result.Table);
    }

    Y_UNIT_TEST(ShouldRejectMediaKindWithoutBuckets)
    {
        TVector<NProto::TMediaKindLatencyThresholds> config = {
            MakeMediaKindThresholds(NCloud::NProto::STORAGE_MEDIA_SSD, {}),
        };

        auto result = BuildLatencyThresholdsTable(config);

        UNIT_ASSERT(!result.IsValid());
    }

    Y_UNIT_TEST(ShouldRejectTooManyBuckets)
    {
        NProto::TMediaKindLatencyThresholds thresholds;
        thresholds.SetMediaKind(NCloud::NProto::STORAGE_MEDIA_SSD);
        for (size_t i = 0;
             i <= MaxLatencyThresholdBucketsPerMediaKind;
             ++i)
        {
            *thresholds.AddBuckets() = MakeBucket(i, 10, 10);
        }

        auto result = BuildLatencyThresholdsTable({thresholds});

        UNIT_ASSERT(!result.IsValid());
        UNIT_ASSERT(!result.Table);
    }

    Y_UNIT_TEST(ShouldAcceptMaximumBucketCount)
    {
        NProto::TMediaKindLatencyThresholds thresholds;
        thresholds.SetMediaKind(NCloud::NProto::STORAGE_MEDIA_SSD);
        for (size_t i = 0;
             i < MaxLatencyThresholdBucketsPerMediaKind;
             ++i)
        {
            *thresholds.AddBuckets() = MakeBucket(i, 10, 10);
        }

        auto result = BuildLatencyThresholdsTable({thresholds});

        UNIT_ASSERT(result.IsValid());
        UNIT_ASSERT(result.Table);
    }

    Y_UNIT_TEST(ShouldRejectFirstBucketWithNonZeroMinRequestBytes)
    {
        TVector<NProto::TMediaKindLatencyThresholds> config = {
            MakeMediaKindThresholds(
                NCloud::NProto::STORAGE_MEDIA_SSD,
                {MakeBucket(4_KB, 10, 10)}),
        };

        auto result = BuildLatencyThresholdsTable(config);

        UNIT_ASSERT(!result.IsValid());
    }

    Y_UNIT_TEST(ShouldRejectNonIncreasingMinRequestBytes)
    {
        TVector<NProto::TMediaKindLatencyThresholds> config = {
            MakeMediaKindThresholds(
                NCloud::NProto::STORAGE_MEDIA_SSD,
                {
                    MakeBucket(0, 10, 10),
                    MakeBucket(8_KB, 10, 10),
                    MakeBucket(4_KB, 10, 10),
                }),
        };

        auto result = BuildLatencyThresholdsTable(config);

        UNIT_ASSERT(!result.IsValid());
    }

    Y_UNIT_TEST(ShouldRejectDuplicateMinRequestBytes)
    {
        TVector<NProto::TMediaKindLatencyThresholds> config = {
            MakeMediaKindThresholds(
                NCloud::NProto::STORAGE_MEDIA_SSD,
                {
                    MakeBucket(0, 10, 10),
                    MakeBucket(8_KB, 10, 10),
                    MakeBucket(8_KB, 10, 10),
                }),
        };

        auto result = BuildLatencyThresholdsTable(config);

        UNIT_ASSERT(!result.IsValid());
    }

    Y_UNIT_TEST(ShouldRejectDuplicateMediaKind)
    {
        TVector<NProto::TMediaKindLatencyThresholds> config = {
            MakeMediaKindThresholds(
                NCloud::NProto::STORAGE_MEDIA_SSD,
                {MakeBucket(0, 10, 10)}),
            MakeMediaKindThresholds(
                NCloud::NProto::STORAGE_MEDIA_SSD,
                {MakeBucket(0, 20, 20)}),
        };

        auto result = BuildLatencyThresholdsTable(config);

        UNIT_ASSERT(!result.IsValid());
    }

    Y_UNIT_TEST(ShouldRejectZeroReadThreshold)
    {
        TVector<NProto::TMediaKindLatencyThresholds> config = {
            MakeMediaKindThresholds(
                NCloud::NProto::STORAGE_MEDIA_SSD,
                {MakeBucket(0, 0, 10)}),
        };

        auto result = BuildLatencyThresholdsTable(config);

        UNIT_ASSERT(!result.IsValid());
    }

    Y_UNIT_TEST(ShouldRejectZeroWriteThreshold)
    {
        TVector<NProto::TMediaKindLatencyThresholds> config = {
            MakeMediaKindThresholds(
                NCloud::NProto::STORAGE_MEDIA_SSD,
                {MakeBucket(0, 10, 0)}),
        };

        auto result = BuildLatencyThresholdsTable(config);

        UNIT_ASSERT(!result.IsValid());
    }

    Y_UNIT_TEST(ShouldAcceptNonMonotonicThresholdsWithWarning)
    {
        TVector<NProto::TMediaKindLatencyThresholds> config = {
            MakeMediaKindThresholds(
                NCloud::NProto::STORAGE_MEDIA_SSD,
                {
                    MakeBucket(0, 100, 100),
                    // Smaller than the previous bucket's threshold - not
                    // mechanically dangerous, just a config smell.
                    MakeBucket(8_KB, 10, 10),
                }),
        };

        auto result = BuildLatencyThresholdsTable(config);

        UNIT_ASSERT(result.IsValid());
        UNIT_ASSERT(result.Table);
        UNIT_ASSERT_VALUES_EQUAL(1u, result.Warnings.size());
    }

    Y_UNIT_TEST(ShouldAcceptValidConfigWithoutWarnings)
    {
        auto result = BuildLatencyThresholdsTable(MakeValidConfig());

        UNIT_ASSERT(result.IsValid());
        UNIT_ASSERT(result.Table);
        UNIT_ASSERT(result.Warnings.empty());
    }

    Y_UNIT_TEST(ShouldAcceptMultipleDistinctMediaKinds)
    {
        TVector<NProto::TMediaKindLatencyThresholds> config = {
            MakeMediaKindThresholds(
                NCloud::NProto::STORAGE_MEDIA_SSD,
                {MakeBucket(0, 10, 10)}),
            MakeMediaKindThresholds(
                NCloud::NProto::STORAGE_MEDIA_HDD,
                {MakeBucket(0, 50, 50)}),
        };

        auto result = BuildLatencyThresholdsTable(config);

        UNIT_ASSERT(result.IsValid());
        UNIT_ASSERT(
            result.Table->FindLadder(NCloud::NProto::STORAGE_MEDIA_SSD));
        UNIT_ASSERT(
            result.Table->FindLadder(NCloud::NProto::STORAGE_MEDIA_HDD));
    }
}

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TLatencyThresholdsLookupTest)
{
    Y_UNIT_TEST(ShouldReturnNullptrForUnconfiguredMediaKind)
    {
        auto result = BuildLatencyThresholdsTable(
            MakeValidConfig(NCloud::NProto::STORAGE_MEDIA_SSD));
        UNIT_ASSERT(result.IsValid());

        UNIT_ASSERT(
            !result.Table->FindLadder(NCloud::NProto::STORAGE_MEDIA_HDD));
    }

    Y_UNIT_TEST(ShouldPlaceZeroSizeInFirstBucket)
    {
        TLatencyThresholdLadder ladder = {
            {
                .MinRequestBytes = 0,
                .ReadThreshold = TDuration::MilliSeconds(10),
            },
            {
                .MinRequestBytes = 8_KB,
                .ReadThreshold = TDuration::MilliSeconds(100),
            },
        };

        const auto& bucket = FindLatencyThresholdBucket(ladder, 0);

        UNIT_ASSERT_VALUES_EQUAL(0u, bucket.MinRequestBytes);
    }

    Y_UNIT_TEST(ShouldPlaceSizeStrictlyBetweenBoundariesInLowerBucket)
    {
        // Boundaries 0 / 8K / 16K; 6K must land in [0, 8K), not [8K, 16K).
        // This is the exact trap described in the task: a naive lower_bound
        // over the lower bounds would land one bucket too high here.
        TLatencyThresholdLadder ladder = {
            {.MinRequestBytes = 0},
            {.MinRequestBytes = 8_KB},
            {.MinRequestBytes = 16_KB},
        };

        const auto& bucket = FindLatencyThresholdBucket(ladder, 6_KB);

        UNIT_ASSERT_VALUES_EQUAL(0u, bucket.MinRequestBytes);
    }

    Y_UNIT_TEST(ShouldPlaceSizeExactlyOnBoundaryInUpperBucket)
    {
        TLatencyThresholdLadder ladder = {
            {.MinRequestBytes = 0},
            {.MinRequestBytes = 8_KB},
            {.MinRequestBytes = 16_KB},
        };

        {
            const auto& bucket = FindLatencyThresholdBucket(ladder, 4_KB);
            UNIT_ASSERT_VALUES_EQUAL(0u, bucket.MinRequestBytes);
        }
        {
            const auto& bucket = FindLatencyThresholdBucket(ladder, 8_KB);
            UNIT_ASSERT_VALUES_EQUAL(8_KB, bucket.MinRequestBytes);
        }
        {
            const auto& bucket = FindLatencyThresholdBucket(ladder, 4_MB);
            UNIT_ASSERT_VALUES_EQUAL(16_KB, bucket.MinRequestBytes);
        }
    }

    Y_UNIT_TEST(ShouldPlaceSizeAboveLastBoundaryInLastBucketWithoutOverrun)
    {
        TLatencyThresholdLadder ladder = {
            {.MinRequestBytes = 0},
            {.MinRequestBytes = 8_KB},
            {.MinRequestBytes = 16_KB},
        };

        const auto& bucket = FindLatencyThresholdBucket(ladder, 8_MB);

        UNIT_ASSERT_VALUES_EQUAL(16_KB, bucket.MinRequestBytes);
    }

    Y_UNIT_TEST(ShouldApplyReadAndWriteThresholdsSeparately)
    {
        TLatencyThresholdLadder ladder = {
            {
                .MinRequestBytes = 0,
                .ReadThreshold = TDuration::MilliSeconds(10),
                .WriteThreshold = TDuration::MilliSeconds(20),
            },
        };

        const auto& bucket = FindLatencyThresholdBucket(ladder, 0);

        UNIT_ASSERT_VALUES_EQUAL(
            TDuration::MilliSeconds(10),
            bucket.ReadThreshold);
        UNIT_ASSERT_VALUES_EQUAL(
            TDuration::MilliSeconds(20),
            bucket.WriteThreshold);
    }
}

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TLatencyThresholdsClassificationTest)
{
    TLatencyThresholdLadder MakeLadder()
    {
        return {{
            .MinRequestBytes = 0,
            .ReadThreshold = TDuration::MilliSeconds(10),
            .WriteThreshold = TDuration::MilliSeconds(10),
        }};
    }

    Y_UNIT_TEST(ShouldSkipOperationWithNoConfiguredLadder)
    {
        auto outcome = ClassifyLatencyOutcome(
            nullptr,
            {},
            false,
            4_KB,
            TDuration::MilliSeconds(1));

        UNIT_ASSERT(outcome.CountSkipped);
        UNIT_ASSERT(!outcome.CountTotal);
        UNIT_ASSERT(!outcome.CountGood);
    }

    void CheckFinalFailureIsBad(ui32 errorCode)
    {
        auto ladder = MakeLadder();

        auto outcome = ClassifyLatencyOutcome(
            &ladder,
            MakeError(errorCode),
            false,
            4_KB,
            TDuration::MilliSeconds(1));

        UNIT_ASSERT(!outcome.CountSkipped);
        UNIT_ASSERT(outcome.CountTotal);
        UNIT_ASSERT(!outcome.CountGood);
    }

    Y_UNIT_TEST(ShouldCountAllFinalServiceFailuresAsBad)
    {
        CheckFinalFailureIsBad(E_FAIL);
        CheckFinalFailureIsBad(E_RETRY_TIMEOUT);
        CheckFinalFailureIsBad(E_REJECTED);
        CheckFinalFailureIsBad(E_BS_INVALID_SESSION);
        CheckFinalFailureIsBad(E_ABORTED);
        CheckFinalFailureIsBad(E_TRANSPORT_ERROR);
        CheckFinalFailureIsBad(E_IO_SILENT);
    }

    void CheckSkipped(const NProto::TError& error)
    {
        auto ladder = MakeLadder();

        auto outcome = ClassifyLatencyOutcome(
            &ladder,
            error,
            false,
            4_KB,
            TDuration::MilliSeconds(1));

        UNIT_ASSERT(outcome.CountSkipped);
        UNIT_ASSERT(!outcome.CountTotal);
        UNIT_ASSERT(!outcome.CountGood);
    }

    Y_UNIT_TEST(ShouldSkipExplicitThrottlingRejection)
    {
        CheckSkipped(MakeError(E_BS_THROTTLED));
        CheckSkipped(MakeError(E_REJECTED, "Throttled"));
    }

    Y_UNIT_TEST(ShouldSkipOperationRejectedByCheckpoint)
    {
        CheckSkipped(MakeError(
            E_REJECTED,
            "Checkpoint reject request. test"));
    }

    Y_UNIT_TEST(ShouldSkipInvalidInputAndCancellation)
    {
        CheckSkipped(MakeError(E_ARGUMENT));
        CheckSkipped(MakeError(E_CANCELLED));
    }

    Y_UNIT_TEST(ShouldCountFastSuccessfulOperationAsGood)
    {
        auto ladder = MakeLadder();

        auto outcome = ClassifyLatencyOutcome(
            &ladder,
            {},
            false,
            4_KB,
            TDuration::MilliSeconds(1));

        UNIT_ASSERT(!outcome.CountSkipped);
        UNIT_ASSERT(outcome.CountTotal);
        UNIT_ASSERT(outcome.CountGood);
    }

    Y_UNIT_TEST(ShouldCountSlowSuccessfulOperationAsTotalOnly)
    {
        auto ladder = MakeLadder();

        auto outcome = ClassifyLatencyOutcome(
            &ladder,
            {},
            false,
            4_KB,
            TDuration::MilliSeconds(20));

        UNIT_ASSERT(outcome.CountTotal);
        UNIT_ASSERT(!outcome.CountGood);
    }

    Y_UNIT_TEST(ShouldTreatExecTimeExactlyAtThresholdAsGood)
    {
        auto ladder = MakeLadder();

        auto outcome = ClassifyLatencyOutcome(
            &ladder,
            {},
            false,
            4_KB,
            TDuration::MilliSeconds(10));

        UNIT_ASSERT(outcome.CountTotal);
        UNIT_ASSERT(outcome.CountGood);
    }

    Y_UNIT_TEST(ShouldCountShapedButExecutedOperation)
    {
        // A throttled-but-executed operation is judged on its execTime with
        // the wait already subtracted by the caller - ClassifyLatencyOutcome
        // itself has no notion of waiting, it only sees the (small) execTime
        // that made it through. Success + a small execTime must count as
        // good, exactly like an unthrottled fast operation.
        auto ladder = MakeLadder();

        auto outcome = ClassifyLatencyOutcome(
            &ladder,
            {},
            false,
            4_KB,
            TDuration::MilliSeconds(1));

        UNIT_ASSERT(outcome.CountTotal);
        UNIT_ASSERT(outcome.CountGood);
    }

    Y_UNIT_TEST(ShouldApplyReadThresholdForReads)
    {
        TLatencyThresholdLadder ladder = {
            {
                .MinRequestBytes = 0,
                .ReadThreshold = TDuration::MilliSeconds(50),
                .WriteThreshold = TDuration::MilliSeconds(10),
            },
        };

        // 20ms is over the write threshold but under the read threshold.
        auto outcome = ClassifyLatencyOutcome(
            &ladder,
            {},
            /*isWrite*/ false,
            4_KB,
            TDuration::MilliSeconds(20));

        UNIT_ASSERT(outcome.CountTotal);
        UNIT_ASSERT(outcome.CountGood);
    }

    Y_UNIT_TEST(ShouldApplyWriteThresholdForWrites)
    {
        TLatencyThresholdLadder ladder = {
            {
                .MinRequestBytes = 0,
                .ReadThreshold = TDuration::MilliSeconds(50),
                .WriteThreshold = TDuration::MilliSeconds(10),
            },
        };

        // Same 20ms, but now judged as a write against the stricter
        // threshold - must fail even though the read case with the same
        // execTime passes.
        auto outcome = ClassifyLatencyOutcome(
            &ladder,
            {},
            /*isWrite*/ true,
            4_KB,
            TDuration::MilliSeconds(20));

        UNIT_ASSERT(outcome.CountTotal);
        UNIT_ASSERT(!outcome.CountGood);
    }

    Y_UNIT_TEST(ShouldKeepGoodNotExceedingTotalOverMixedStream)
    {
        TLatencyThresholdLadder ladder = {
            {
                .MinRequestBytes = 0,
                .ReadThreshold = TDuration::MilliSeconds(10),
                .WriteThreshold = TDuration::MilliSeconds(10),
            },
        };

        ui64 total = 0;
        ui64 good = 0;

        const struct
        {
            NProto::TError Error;
            TDuration ExecTime;
        } ops[] = {
            {{}, TDuration::MilliSeconds(1)},
            {{}, TDuration::MilliSeconds(50)},
            {MakeError(E_FAIL), TDuration::Zero()},
            {MakeError(E_BS_THROTTLED), TDuration::Zero()},
            {{}, TDuration::MilliSeconds(10)},
        };

        for (const auto& op: ops) {
            auto outcome = ClassifyLatencyOutcome(
                &ladder,
                op.Error,
                false,
                4_KB,
                op.ExecTime);
            total += outcome.CountTotal ? 1 : 0;
            good += outcome.CountGood ? 1 : 0;
        }

        UNIT_ASSERT_VALUES_EQUAL(4u, total);
        UNIT_ASSERT_VALUES_EQUAL(2u, good);
        UNIT_ASSERT(good <= total);
    }
}

}   // namespace NCloud::NBlockStore
