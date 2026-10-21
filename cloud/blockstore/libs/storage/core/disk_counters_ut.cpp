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
        for (ui32 size: {1, 2, 2}) {
            proto.AddBlobCompressionChunkSizes(size);
        }
        const TDiagnosticsConfig diagnostics(proto);
        const auto makeCounters = [&] {
            return TPartitionDiskCounters(
                EPublishingPolicy::Repl,
                diagnostics);
        };
        auto first = makeCounters();
        first.BlobCompression.at(1).UncompressedBytesWritten.Increment(4096);
        first.BlobCompression.at(1).CompressedBytesWritten.Increment(100);
        first.BlobCompression.at(2).UncompressedBytesWritten.Increment(4096);
        first.BlobCompression.at(2).CompressedBytesWritten.Increment(80);

        auto accumulated = makeCounters();
        accumulated.Add(first);
        accumulated.Add(first);
        UNIT_ASSERT_VALUES_EQUAL(
            200,
            accumulated.BlobCompression.at(1).CompressedBytesWritten.Value);

        auto published = makeCounters();
        NMonitoring::TDynamicCountersPtr dynCounters =
            new NMonitoring::TDynamicCounters();
        published.Register(dynCounters, false);
        UNIT_ASSERT(dynCounters->FindSubgroup("compression_chunk_size", "1"));
        UNIT_ASSERT(dynCounters->FindSubgroup("compression_chunk_size", "2"));
        UNIT_ASSERT_VALUES_EQUAL(2, published.BlobCompression.size());
        published.AggregateWith(accumulated);
        published.AggregateWith(first);

        const auto now = TInstant::Now();
        published.Publish(now);
        const auto singleBlock =
            dynCounters->GetSubgroup("compression_chunk_size", "1");
        const auto twoBlocks =
            dynCounters->GetSubgroup("compression_chunk_size", "2");
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
            0,
            published.BlobCompression.at(1).CompressedBytesWritten.Value);
        UNIT_ASSERT_VALUES_EQUAL(
            0,
            published.BlobCompression.at(1).UncompressedBytesWritten.Value);

        published.Publish(now);
        UNIT_ASSERT_VALUES_EQUAL(
            300,
            singleBlock->GetCounter("CompressedBytesWritten", true)->Val());

        published.Add(first);
        published.Publish(now);
        UNIT_ASSERT_VALUES_EQUAL(
            400,
            singleBlock->GetCounter("CompressedBytesWritten", true)->Val());
        accumulated.Reset();
        UNIT_ASSERT_VALUES_EQUAL(
            0,
            accumulated.BlobCompression.at(2).CompressedBytesWritten.Value);
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
}

}   // namespace NCloud::NBlockStore::NStorage
