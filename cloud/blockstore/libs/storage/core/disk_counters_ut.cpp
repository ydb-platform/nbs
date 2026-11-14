#include "disk_counters.h"

#include <cloud/blockstore/libs/diagnostics/config.h>
#include <cloud/blockstore/libs/storage/testlib/ut_helpers.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TDiskCountersTests)
{
    Y_UNIT_TEST(ShouldAggregateAndPublishBlobCompressionByChunkSize)
    {
        NProto::TDiagnosticsConfig proto;
        for (ui64 size: {4096, 8192, 8192}) {
            proto.AddBlobCompressionChunkSizes(size);
        }
        const TDiagnosticsConfig diagnostics(proto);
        const auto makeCounters = [&]
        {
            return TPartitionDiskCounters(EPublishingPolicy::Repl, diagnostics);
        };
        auto first = makeCounters();
        first.BlobCompression.at(4096).UncompressedBytesWritten.Increment(4096);
        first.BlobCompression.at(4096).CompressedBytesWritten.Increment(100);
        first.BlobCompression.at(4096).ZeroBytesCompressed.Increment(1024);
        first.BlobCompression.at(8192).UncompressedBytesWritten.Increment(4096);
        first.BlobCompression.at(8192).CompressedBytesWritten.Increment(80);
        first.BlobCompression.at(8192).ZeroBytesCompressed.Increment(2048);

        auto accumulated = makeCounters();
        accumulated.Add(first);
        accumulated.Add(first);
        UNIT_ASSERT_VALUES_EQUAL(
            200,
            accumulated.BlobCompression.at(4096).CompressedBytesWritten.Value);
        UNIT_ASSERT_VALUES_EQUAL(
            2 * 1024,
            accumulated.BlobCompression.at(4096).ZeroBytesCompressed.Value);

        auto published = makeCounters();
        NMonitoring::TDynamicCountersPtr dynCounters =
            new NMonitoring::TDynamicCounters();
        published.Register(dynCounters, false);
        UNIT_ASSERT(dynCounters->FindSubgroup("compression_chunk_size", "4.00 KiB"));
        UNIT_ASSERT(dynCounters->FindSubgroup("compression_chunk_size", "8.00 KiB"));
        UNIT_ASSERT_VALUES_EQUAL(2, published.BlobCompression.size());
        published.AggregateWith(accumulated);
        published.AggregateWith(first);

        const auto now = TInstant::Now();
        published.Publish(now);
        const auto singleBlock =
            dynCounters->GetSubgroup("compression_chunk_size", "4.00 KiB");
        const auto twoBlocks =
            dynCounters->GetSubgroup("compression_chunk_size", "8.00 KiB");
        UNIT_ASSERT_VALUES_EQUAL(
            3 * 4096,
            singleBlock->GetCounter("UncompressedBytesWritten", true)->Val());
        UNIT_ASSERT_VALUES_EQUAL(
            300,
            singleBlock->GetCounter("CompressedBytesWritten", true)->Val());
        UNIT_ASSERT_VALUES_EQUAL(
            240,
            twoBlocks->GetCounter("CompressedBytesWritten", true)->Val());
        UNIT_ASSERT_VALUES_EQUAL(
            3 * 1024,
            singleBlock->GetCounter("ZeroBytesCompressed", true)->Val());
        UNIT_ASSERT_VALUES_EQUAL(
            3 * 2048,
            twoBlocks->GetCounter("ZeroBytesCompressed", true)->Val());
        UNIT_ASSERT_VALUES_EQUAL(
            0,
            published.BlobCompression.at(4096).CompressedBytesWritten.Value);
        UNIT_ASSERT_VALUES_EQUAL(
            0,
            published.BlobCompression.at(4096).UncompressedBytesWritten.Value);
        UNIT_ASSERT_VALUES_EQUAL(
            0,
            published.BlobCompression.at(4096).ZeroBytesCompressed.Value);

        published.Publish(now);
        UNIT_ASSERT_VALUES_EQUAL(
            300,
            singleBlock->GetCounter("CompressedBytesWritten", true)->Val());
        UNIT_ASSERT_VALUES_EQUAL(
            3 * 1024,
            singleBlock->GetCounter("ZeroBytesCompressed", true)->Val());

        published.Add(first);
        published.Publish(now);
        UNIT_ASSERT_VALUES_EQUAL(
            400,
            singleBlock->GetCounter("CompressedBytesWritten", true)->Val());
        UNIT_ASSERT_VALUES_EQUAL(
            4 * 1024,
            singleBlock->GetCounter("ZeroBytesCompressed", true)->Val());
        accumulated.Reset();
        UNIT_ASSERT_VALUES_EQUAL(
            0,
            accumulated.BlobCompression.at(8192).CompressedBytesWritten.Value);
        UNIT_ASSERT_VALUES_EQUAL(
            0,
            accumulated.BlobCompression.at(8192).ZeroBytesCompressed.Value);
    }

    Y_UNIT_TEST(ShouldResetAfterPublish)
    {
        TVolumeSelfCounters counters(
            EPublishingPolicy::All,
            EHistogramCounterOption::ReportMultipleCounters);
        NMonitoring::TDynamicCountersPtr dynCounters =
            new NMonitoring::TDynamicCounters();
        counters.Register(dynCounters, false);

        counters.Simple.MaxUsedQuota.Increment(100);
        counters.Cumulative.UsedQuota.Increment(200);
        counters.ThrottlerDelayRequestCounters.ReadBlocks.Increment(1);
        counters.IngestTimeRequestCounters.ReadBlocks.Increment(2);

        UNIT_ASSERT_VALUES_EQUAL(100, counters.Simple.MaxUsedQuota.Value);
        UNIT_ASSERT_VALUES_EQUAL(200, counters.Cumulative.UsedQuota.Value);
        {
            auto buckets =
                counters.ThrottlerDelayRequestCounters.ReadBlocks.GetBuckets();
            UNIT_ASSERT_VALUES_EQUAL(1, buckets[0].second);
        }
        {
            auto buckets =
                counters.IngestTimeRequestCounters.ReadBlocks.GetBuckets();
            UNIT_ASSERT_VALUES_EQUAL(1, buckets[1].second);
        }

        counters.Publish(TInstant::Now());
        UNIT_ASSERT_VALUES_EQUAL(0, counters.Simple.MaxUsedQuota.Value);
        UNIT_ASSERT_VALUES_EQUAL(0, counters.Cumulative.UsedQuota.Value);
        {
            auto buckets =
                counters.ThrottlerDelayRequestCounters.ReadBlocks.GetBuckets();
            UNIT_ASSERT_VALUES_EQUAL(0, buckets[0].second);
        }
        {
            auto buckets =
                counters.IngestTimeRequestCounters.ReadBlocks.GetBuckets();
            UNIT_ASSERT_VALUES_EQUAL(0, buckets[1].second);
        }
    }

    Y_UNIT_TEST(ShouldAccumulateBlobIoCountersAndCountPatchesAsWrites)
    {
        THistogramRequestCounters first(
            EHistogramCounterOption::ReportMultipleCounters);
        first.ReadBlob.Count = 2;
        first.ReadBlob.RequestBytes = 4 * 1024;
        first.WriteBlob.Count = 3;
        first.WriteBlob.RequestBytes = 8 * 1024;
        first.PatchBlob.Count = 5;
        first.PatchBlob.RequestBytes = 16 * 1024;

        THistogramRequestCounters second(
            EHistogramCounterOption::ReportMultipleCounters);
        second.ReadBlob.Count = 7;
        second.ReadBlob.RequestBytes = 32 * 1024;
        second.WriteBlob.Count = 11;
        second.WriteBlob.RequestBytes = 64 * 1024;
        second.PatchBlob.Count = 13;
        second.PatchBlob.RequestBytes = 128 * 1024;

        TBlobIoCounters counters;
        counters.Add(first);
        counters.Add(second);
        UNIT_ASSERT_VALUES_EQUAL(9, counters.ReadBlob.Count);
        UNIT_ASSERT_VALUES_EQUAL(36 * 1024, counters.ReadBlob.Bytes);
        UNIT_ASSERT_VALUES_EQUAL(32, counters.WriteBlob.Count);
        UNIT_ASSERT_VALUES_EQUAL(216 * 1024, counters.WriteBlob.Bytes);

        counters.Reset();
        UNIT_ASSERT_VALUES_EQUAL(0, counters.ReadBlob.Count);
        UNIT_ASSERT_VALUES_EQUAL(0, counters.ReadBlob.Bytes);
        UNIT_ASSERT_VALUES_EQUAL(0, counters.WriteBlob.Count);
        UNIT_ASSERT_VALUES_EQUAL(0, counters.WriteBlob.Bytes);

        counters.Add(first);
        UNIT_ASSERT_VALUES_EQUAL(2, counters.ReadBlob.Count);
        UNIT_ASSERT_VALUES_EQUAL(4 * 1024, counters.ReadBlob.Bytes);
        UNIT_ASSERT_VALUES_EQUAL(8, counters.WriteBlob.Count);
        UNIT_ASSERT_VALUES_EQUAL(24 * 1024, counters.WriteBlob.Bytes);
    }
}

}   // namespace NCloud::NBlockStore::NStorage
