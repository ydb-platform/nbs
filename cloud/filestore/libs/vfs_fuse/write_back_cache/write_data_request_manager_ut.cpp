#include "write_data_request_manager.h"

#include "sequence_id_generator.h"

#include <cloud/filestore/libs/diagnostics/metrics/metric.h>
#include <cloud/filestore/libs/vfs_fuse/write_back_cache/test/test_persistent_storage.h>
#include <cloud/filestore/libs/vfs_fuse/write_back_cache/test/test_write_back_cache_stats.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/timer_test.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/overloaded.h>

namespace NCloud::NFileStore::NFuse::NWriteBackCache {

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TBootstrap
{
    std::shared_ptr<TTestTimer> Timer;
    std::shared_ptr<TTestWriteBackCacheStats> TestStats;
    std::shared_ptr<TTestStorage> Storage;
    std::shared_ptr<TSequenceIdGenerator> SequenceIdGenerator;
    IWriteDataRequestManagerStatsPtr Stats;
    TWriteDataRequestManagerMetrics Metrics;
    TWriteDataRequestManager RequestManager;

    TMap<ui64, std::unique_ptr<TPendingWriteDataRequest>> PendingRequests;
    TMap<ui64, std::unique_ptr<TCachedWriteDataRequest>> CachedRequests;

    TBootstrap()
        : Timer(std::make_shared<TTestTimer>())
        , TestStats(std::make_shared<TTestWriteBackCacheStats>())
        , Storage(std::make_shared<TTestStorage>(TestStats))
        , SequenceIdGenerator(std::make_shared<TSequenceIdGenerator>())
        , Stats(CreateWriteDataRequestManagerStats())
        , Metrics(Stats->CreateWriteDataRequestManagerMetrics())
        , RequestManager(SequenceIdGenerator, Storage, Timer, Stats)
    {}

    auto Add(ui64 nodeId, ui64 handle, ui64 offset, TString data)
        -> NThreading::TFuture<void>
    {
        auto request = std::make_shared<NProto::TWriteDataRequest>();
        request->SetNodeId(nodeId);
        request->SetHandle(handle);
        request->SetOffset(offset);
        *request->MutableBuffer() = std::move(data);

        auto res = RequestManager.AddRequest(std::move(request));

        return std::visit(
            TOverloaded(
                [this](std::unique_ptr<TPendingWriteDataRequest> request)
                {
                    auto future = request->AccessPromise().GetFuture();
                    PendingRequests[request->GetSequenceId()] =
                        std::move(request);
                    return future.IgnoreResult();
                },
                [this](std::unique_ptr<TCachedWriteDataRequest> request)
                {
                    CachedRequests[request->GetSequenceId()] =
                        std::move(request);
                    return NThreading::MakeFuture();
                }),
            std::move(res));
    }

    void SetFlushed(ui64 sequenceId)
    {
        RequestManager.SetFlushed(CachedRequests[sequenceId].get());
    }

    void Remove(ui64 sequenceId)
    {
        RequestManager.Remove(std::move(PendingRequests[sequenceId]));
        PendingRequests.erase(sequenceId);
    }

    void Evict(ui64 sequenceId)
    {
        RequestManager.Evict(std::move(CachedRequests[sequenceId]));
        CachedRequests.erase(sequenceId);
    }

    bool TryProcessPendingRequests()
    {
        while (RequestManager.HasPendingRequests()) {
            auto request = RequestManager.TryProcessPendingRequest();
            if (!request) {
                return false;
            }

            PendingRequests[request->GetSequenceId()]->AccessPromise().SetValue(
                {});
            PendingRequests.erase(request->GetSequenceId());
            CachedRequests[request->GetSequenceId()] = std::move(request);
        }
        return true;
    }

    ui64 GetAllocationCount() const
    {
        return TestStats->StorageStats.EntryCount;
    }

    TString Dump() const
    {
        TStringBuilder out;

        out << "P[";

        for (const auto& [seqId, request]: PendingRequests) {
            out << "(" << seqId << ":" << request->GetRequest().GetBuffer()
                << ")";
        }

        out << "],C[";

        for (const auto& [seqId, request]: CachedRequests) {
            out << "(" << seqId << ":" << request->GetBuffer() << ")";
        }

        out << "]";

        return out;
    }

    void CheckPendingQueueMetrics(
        i64 expectedActiveCount,
        i64 expectedActiveMaxCount,
        i64 expectedMaxTime,
        i64 expectedCompletedCount,
        i64 expectedCompletedTime) const
    {
        UNIT_ASSERT_VALUES_EQUAL(
            expectedActiveCount,
            Metrics.PendingQueue.Count->Get());

        UNIT_ASSERT_VALUES_EQUAL(
            expectedActiveMaxCount,
            Metrics.PendingQueue.MaxCount->Get());

        UNIT_ASSERT_VALUES_EQUAL(
            expectedMaxTime,
            Metrics.PendingQueue.MaxTime->Get());

        UNIT_ASSERT_VALUES_EQUAL(
            expectedCompletedCount,
            Metrics.PendingQueue.ProcessedCount->Get());

        UNIT_ASSERT_VALUES_EQUAL(
            expectedCompletedTime,
            Metrics.PendingQueue.ProcessedTime->Get());
    }

    void CheckUnflushedQueueMetrics(
        i64 expectedActiveCount,
        i64 expectedActiveMaxCount,
        i64 expectedMaxTime,
        i64 expectedCompletedCount,
        i64 expectedCompletedTime) const
    {
        UNIT_ASSERT_VALUES_EQUAL(
            expectedActiveCount,
            Metrics.UnflushedQueue.Count->Get());

        UNIT_ASSERT_VALUES_EQUAL(
            expectedActiveMaxCount,
            Metrics.UnflushedQueue.MaxCount->Get());

        UNIT_ASSERT_VALUES_EQUAL(
            expectedMaxTime,
            Metrics.UnflushedQueue.MaxTime->Get());

        UNIT_ASSERT_VALUES_EQUAL(
            expectedCompletedCount,
            Metrics.UnflushedQueue.ProcessedCount->Get());

        UNIT_ASSERT_VALUES_EQUAL(
            expectedCompletedTime,
            Metrics.UnflushedQueue.ProcessedTime->Get());
    }

    void CheckFlushedQueueMetrics(
        i64 expectedActiveCount,
        i64 expectedActiveMaxCount,
        i64 expectedCompletedCount) const
    {
        UNIT_ASSERT_VALUES_EQUAL(
            expectedActiveCount,
            Metrics.FlushedQueue.Count->Get());

        UNIT_ASSERT_VALUES_EQUAL(
            expectedActiveMaxCount,
            Metrics.FlushedQueue.MaxCount->Get());

        UNIT_ASSERT_VALUES_EQUAL(
            expectedCompletedCount,
            Metrics.FlushedQueue.ProcessedCount->Get());
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TPersistentRequestStorageTest)
{
    Y_UNIT_TEST(Add_SetFlushed_Evict)
    {
        TBootstrap b;

        UNIT_ASSERT(!b.RequestManager.HasPendingOrUnflushedRequests());
        UNIT_ASSERT_VALUES_EQUAL(
            Max<ui64>(),
            b.RequestManager.GetMinPendingOrUnflushedSequenceId());

        UNIT_ASSERT(b.Add(1, 101, 1, "a").HasValue());

        UNIT_ASSERT_VALUES_EQUAL("P[],C[(1:a)]", b.Dump());
        UNIT_ASSERT_VALUES_EQUAL(1, b.GetAllocationCount());
        UNIT_ASSERT(b.RequestManager.HasPendingOrUnflushedRequests());
        UNIT_ASSERT_VALUES_EQUAL(
            1,
            b.RequestManager.GetMinPendingOrUnflushedSequenceId());

        UNIT_ASSERT(b.Add(1, 102, 2, "b").HasValue());

        UNIT_ASSERT_VALUES_EQUAL("P[],C[(1:a)(2:b)]", b.Dump());
        UNIT_ASSERT_VALUES_EQUAL(2, b.GetAllocationCount());
        UNIT_ASSERT(b.RequestManager.HasPendingOrUnflushedRequests());
        UNIT_ASSERT_VALUES_EQUAL(
            1,
            b.RequestManager.GetMinPendingOrUnflushedSequenceId());

        b.SetFlushed(1);
        UNIT_ASSERT_VALUES_EQUAL(2, b.GetAllocationCount());
        UNIT_ASSERT(b.RequestManager.HasPendingOrUnflushedRequests());
        UNIT_ASSERT_VALUES_EQUAL(
            2,
            b.RequestManager.GetMinPendingOrUnflushedSequenceId());

        b.SetFlushed(2);
        UNIT_ASSERT_VALUES_EQUAL(2, b.GetAllocationCount());
        UNIT_ASSERT(!b.RequestManager.HasPendingOrUnflushedRequests());
        UNIT_ASSERT_VALUES_EQUAL(
            Max<ui64>(),
            b.RequestManager.GetMinPendingOrUnflushedSequenceId());

        b.Evict(1);
        UNIT_ASSERT_VALUES_EQUAL(1, b.GetAllocationCount());

        UNIT_ASSERT(b.TryProcessPendingRequests());
        UNIT_ASSERT_VALUES_EQUAL("P[],C[(2:b)]", b.Dump());

        b.Evict(2);
        UNIT_ASSERT_VALUES_EQUAL(0, b.GetAllocationCount());

        UNIT_ASSERT(b.TryProcessPendingRequests());
        UNIT_ASSERT_VALUES_EQUAL("P[],C[]", b.Dump());
    }

    Y_UNIT_TEST(StorageFull)
    {
        TBootstrap b;
        b.Storage->SetCapacity(2);

        UNIT_ASSERT(b.Add(1, 101, 1, "a").HasValue());
        UNIT_ASSERT(b.Add(1, 102, 2, "b").HasValue());

        auto add3 = b.Add(1, 102, 3, "c");
        auto add4 = b.Add(1, 102, 4, "d");
        UNIT_ASSERT(!add3.HasValue());
        UNIT_ASSERT(!add4.HasValue());

        UNIT_ASSERT_VALUES_EQUAL("P[(3:c)(4:d)],C[(1:a)(2:b)]", b.Dump());
        UNIT_ASSERT_VALUES_EQUAL(2, b.GetAllocationCount());
        UNIT_ASSERT(b.RequestManager.HasPendingOrUnflushedRequests());
        UNIT_ASSERT_VALUES_EQUAL(
            1,
            b.RequestManager.GetMinPendingOrUnflushedSequenceId());

        b.SetFlushed(1);
        b.SetFlushed(2);
        UNIT_ASSERT(b.RequestManager.HasPendingOrUnflushedRequests());
        UNIT_ASSERT_VALUES_EQUAL(
            3,
            b.RequestManager.GetMinPendingOrUnflushedSequenceId());
        UNIT_ASSERT(!add3.HasValue());
        UNIT_ASSERT(!add4.HasValue());

        b.Evict(1);
        UNIT_ASSERT(!b.TryProcessPendingRequests());
        UNIT_ASSERT_VALUES_EQUAL("P[(4:d)],C[(2:b)(3:c)]", b.Dump());
        UNIT_ASSERT_VALUES_EQUAL(2, b.GetAllocationCount());
        UNIT_ASSERT(b.RequestManager.HasPendingOrUnflushedRequests());
        UNIT_ASSERT_VALUES_EQUAL(
            3,
            b.RequestManager.GetMinPendingOrUnflushedSequenceId());
        UNIT_ASSERT(add3.HasValue());
        UNIT_ASSERT(!add4.HasValue());

        b.SetFlushed(3);
        b.Evict(2);
        UNIT_ASSERT(b.TryProcessPendingRequests());
        UNIT_ASSERT_VALUES_EQUAL("P[],C[(3:c)(4:d)]", b.Dump());
        UNIT_ASSERT_VALUES_EQUAL(2, b.GetAllocationCount());
        UNIT_ASSERT(b.RequestManager.HasPendingOrUnflushedRequests());
        UNIT_ASSERT_VALUES_EQUAL(
            4,
            b.RequestManager.GetMinPendingOrUnflushedSequenceId());
        UNIT_ASSERT(add4.HasValue());

        b.SetFlushed(3);
        b.Evict(3);
        b.Evict(4);
        UNIT_ASSERT_VALUES_EQUAL("P[],C[]", b.Dump());
        UNIT_ASSERT_VALUES_EQUAL(0, b.GetAllocationCount());
        UNIT_ASSERT(!b.RequestManager.HasPendingOrUnflushedRequests());
        UNIT_ASSERT_VALUES_EQUAL(
            Max<ui64>(),
            b.RequestManager.GetMinPendingOrUnflushedSequenceId());
    }

    Y_UNIT_TEST(Add_Remove)
    {
        TBootstrap b;
        b.Storage->SetCapacity(1);

        UNIT_ASSERT(b.Add(1, 101, 1, "a").HasValue());

        auto add2 = b.Add(1, 102, 2, "b");
        auto add3 = b.Add(1, 102, 3, "c");
        UNIT_ASSERT(!add2.HasValue());
        UNIT_ASSERT(!add3.HasValue());

        b.Remove(2);
        b.SetFlushed(1);
        b.Evict(1);
        b.TryProcessPendingRequests();

        UNIT_ASSERT_VALUES_EQUAL("P[],C[(3:c)]", b.Dump());
        UNIT_ASSERT_VALUES_EQUAL(1, b.GetAllocationCount());
        UNIT_ASSERT(b.RequestManager.HasPendingOrUnflushedRequests());
        UNIT_ASSERT_VALUES_EQUAL(
            3,
            b.RequestManager.GetMinPendingOrUnflushedSequenceId());
    }

    Y_UNIT_TEST(ShouldReportMetrics)
    {
        TBootstrap b;

        b.Storage->SetCapacity(2);

        b.Add(1, 101, 0, "abc");    // SequenceId = 1

        b.CheckPendingQueueMetrics(0, 0, 0, 0, 0);
        b.CheckUnflushedQueueMetrics(1, 1, 0, 0, 0);
        b.CheckFlushedQueueMetrics(0, 0, 0);

        b.Timer->AdvanceTime(TDuration::MilliSeconds(1));

        b.Add(2, 201, 0, "def");    // SequenceId = 2
        b.Add(2, 202, 1, "xyz");    // SequenceId = 3
        b.Add(2, 203, 2, "ijk");    // SequenceId = 4

        b.Timer->AdvanceTime(TDuration::MilliSeconds(2));
        b.RequestManager.UpdateStats();

        b.CheckPendingQueueMetrics(2, 2, 2000, 0, 0);
        b.CheckUnflushedQueueMetrics(2, 2, 3000, 0, 0);
        b.CheckFlushedQueueMetrics(0, 0, 0);

        b.Timer->AdvanceTime(TDuration::MilliSeconds(1));
        b.SetFlushed(1);
        b.Timer->AdvanceTime(TDuration::MilliSeconds(5));
        b.Evict(1);
        b.TryProcessPendingRequests();
        b.Timer->AdvanceTime(TDuration::MilliSeconds(3));
        b.RequestManager.UpdateStats();

        b.CheckPendingQueueMetrics(1, 2, 11000, 1, 8000);
        b.CheckUnflushedQueueMetrics(2, 2, 11000, 1, 4000);
        b.CheckFlushedQueueMetrics(0, 1, 1);

        b.SetFlushed(2);
        b.SetFlushed(3);
        b.Timer->AdvanceTime(TDuration::MilliSeconds(2));
        b.RequestManager.UpdateStats();

        b.CheckPendingQueueMetrics(1, 2, 13000, 1, 8000);
        b.CheckUnflushedQueueMetrics(0, 2, 11000, 3, 18000);
        b.CheckFlushedQueueMetrics(2, 2, 1);

        b.Evict(2);
        b.Evict(3);
        b.TryProcessPendingRequests();
        b.SetFlushed(4);
        b.Evict(4);
        b.Timer->AdvanceTime(TDuration::MilliSeconds(1));
        b.RequestManager.UpdateStats();

        b.CheckPendingQueueMetrics(0, 2, 13000, 2, 21000);
        b.CheckUnflushedQueueMetrics(0, 2, 11000, 4, 18000);
        b.CheckFlushedQueueMetrics(0, 2, 4);

        // Max value is calculated over a sliding window with 15 buckets
        for (int i = 0; i <= 15; i++) {
            b.RequestManager.UpdateStats();
        }

        b.CheckPendingQueueMetrics(0, 0, 0, 2, 21000);
        b.CheckUnflushedQueueMetrics(0, 0, 0, 4, 18000);
        b.CheckFlushedQueueMetrics(0, 0, 4);
    }
}

}   // namespace NCloud::NFileStore::NFuse::NWriteBackCache
