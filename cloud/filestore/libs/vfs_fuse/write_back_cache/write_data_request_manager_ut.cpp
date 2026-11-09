#include "write_data_request_manager.h"

#include "sequence_id_generator.h"
#include "write_back_cache_stats.h"

#include <cloud/filestore/libs/diagnostics/metrics/metric.h>
#include <cloud/filestore/libs/vfs_fuse/write_back_cache/test/test_persistent_storage.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/timer_test.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NFileStore::NFuse::NWriteBackCache {

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TBootstrap
{
    std::shared_ptr<TTestTimer> Timer;
    IWriteBackCacheStatsPtr Stats;
    TWriteBackCacheMetrics Metrics;
    std::shared_ptr<TTestStorage> Storage;
    std::shared_ptr<TSequenceIdGenerator> SequenceIdGenerator;
    TWriteDataRequestManager RequestManager;

    TMap<ui64, std::unique_ptr<TPendingWriteDataRequest>> PendingRequests;
    TMap<ui64, std::unique_ptr<TCachedWriteDataRequest>> CachedRequests;

    TBootstrap()
        : Timer(std::make_shared<TTestTimer>())
        , Stats(CreateWriteBackCacheStats())
        , Metrics(Stats->CreateMetrics())
        , Storage(CreateTestStorage(Stats))
        , SequenceIdGenerator(std::make_shared<TSequenceIdGenerator>())
        , RequestManager(
              SequenceIdGenerator,
              Storage,
              Timer,
              Stats->GetWriteDataRequestManagerStats())
    {}

    TPendingWriteDataRequest*
    AddWithoutProcessing(ui64 nodeId, ui64 handle, ui64 offset, TString data)
    {
        auto request = std::make_shared<NProto::TWriteDataRequest>();
        request->SetNodeId(nodeId);
        request->SetHandle(handle);
        request->SetOffset(offset);
        *request->MutableBuffer() = std::move(data);

        auto pendingRequest = RequestManager.AddRequest(std::move(request));
        UNIT_ASSERT(pendingRequest);

        auto* result = pendingRequest.get();
        PendingRequests[pendingRequest->GetSequenceId()] =
            std::move(pendingRequest);

        return result;
    }

    auto Add(ui64 nodeId, ui64 handle, ui64 offset, TString data)
        -> NThreading::TFuture<void>
    {
        auto* pendingRequest =
            AddWithoutProcessing(nodeId, handle, offset, std::move(data));
        auto future = pendingRequest->AccessPromise().GetFuture();

        TryProcessPendingRequests();

        return future.IgnoreResult();
    }

    void SetFlushed(ui64 sequenceId)
    {
        UNIT_ASSERT(
            RequestManager.SetFlushed(CachedRequests[sequenceId].get()));
    }

    void Remove(ui64 sequenceId)
    {
        RequestManager.RemoveUnallocated(
            std::move(PendingRequests[sequenceId]));
        PendingRequests.erase(sequenceId);
    }

    void Evict(ui64 sequenceId)
    {
        UNIT_ASSERT(
            RequestManager.Evict(std::move(CachedRequests[sequenceId])));
        CachedRequests.erase(sequenceId);
    }

    bool TryProcessPendingRequests()
    {
        while (RequestManager.HasPendingRequests()) {
            auto allocResult = RequestManager.TryAllocPendingRequest();
            UNIT_ASSERT(!allocResult.Failed);

            auto* pendingRequest = allocResult.Request;
            if (!pendingRequest) {
                return false;
            }

            pendingRequest->SerializeToAllocation();

            auto nextReadyCacheRequest =
                RequestManager.GetNextReadyCachedRequest();

            UNIT_ASSERT(!nextReadyCacheRequest.Failed);

            auto cachedRequest = std::move(nextReadyCacheRequest.Request);
            UNIT_ASSERT(cachedRequest);

            PendingRequests[cachedRequest->GetSequenceId()]
                ->AccessPromise()
                .SetValue({});
            PendingRequests.erase(cachedRequest->GetSequenceId());
            CachedRequests[cachedRequest->GetSequenceId()] =
                std::move(cachedRequest);
        }
        return true;
    }

    ui64 GetAllocationCount() const
    {
        return Metrics.Storage.EntryCount->Get();
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
    Y_UNIT_TEST(ShouldDistinguishUnavailablePendingRequestStates)
    {
        TBootstrap b;

        {
            auto allocResult = b.RequestManager.TryAllocPendingRequest();
            UNIT_ASSERT(!allocResult.Request);
            UNIT_ASSERT(!allocResult.Failed);
            UNIT_ASSERT(!allocResult.StorageIsFull);

            auto readyResult = b.RequestManager.GetNextReadyCachedRequest();
            UNIT_ASSERT(!readyResult.Request);
            UNIT_ASSERT(!readyResult.Failed);
        }

        auto* pendingRequest = b.AddWithoutProcessing(1, 101, 0, "a");
        UNIT_ASSERT(b.RequestManager.SetBackpressureStatusForNode(1));

        {
            auto allocResult = b.RequestManager.TryAllocPendingRequest();
            UNIT_ASSERT(!allocResult.Request);
            UNIT_ASSERT(!allocResult.Failed);
            UNIT_ASSERT(!allocResult.StorageIsFull);
            UNIT_ASSERT(!pendingRequest->HasAllocation());
        }

        UNIT_ASSERT(b.RequestManager.ClearBackpressureStatusForNode(1));

        {
            auto allocResult = b.RequestManager.TryAllocPendingRequest();
            UNIT_ASSERT_VALUES_EQUAL(pendingRequest, allocResult.Request);
            UNIT_ASSERT(!allocResult.Failed);
            UNIT_ASSERT(!allocResult.StorageIsFull);
            UNIT_ASSERT(pendingRequest->HasAllocation());

            auto secondAllocResult = b.RequestManager.TryAllocPendingRequest();
            UNIT_ASSERT(!secondAllocResult.Request);
            UNIT_ASSERT(!secondAllocResult.Failed);
            UNIT_ASSERT(!secondAllocResult.StorageIsFull);

            auto readyResult = b.RequestManager.GetNextReadyCachedRequest();
            UNIT_ASSERT(!readyResult.Request);
            UNIT_ASSERT(!readyResult.Failed);
        }

        pendingRequest->SerializeToAllocation();

        {
            auto readyResult = b.RequestManager.GetNextReadyCachedRequest();
            UNIT_ASSERT(readyResult.Request);
            UNIT_ASSERT(!readyResult.Failed);
        }
    }

    Y_UNIT_TEST(ShouldReportStorageFullSeparately)
    {
        TBootstrap b;
        b.Storage->SetCapacity(1);

        UNIT_ASSERT(b.Add(1, 101, 0, "a").HasValue());
        b.AddWithoutProcessing(2, 202, 0, "b");

        const auto allocResult = b.RequestManager.TryAllocPendingRequest();
        UNIT_ASSERT(!allocResult.Request);
        UNIT_ASSERT(!allocResult.Failed);
        UNIT_ASSERT(allocResult.StorageIsFull);
    }

    Y_UNIT_TEST(ShouldAbortOnSerializationFailure)
    {
        TBootstrap b;

        auto request = std::make_shared<NProto::TWriteDataRequest>();
        request->SetNodeId(1);
        request->SetHandle(101);
        request->SetBuffer("a");

        auto pendingRequest = b.RequestManager.AddRequest(request);
        const auto allocResult = b.RequestManager.TryAllocPendingRequest();
        UNIT_ASSERT_VALUES_EQUAL(pendingRequest.get(), allocResult.Request);

        // Break the invariant by modifying the request buffer length
        request->SetBuffer("ab");
        UNIT_ASSERT_EXCEPTION_CONTAINS(
            pendingRequest->SerializeToAllocation(),
            yexception,
            "memory output stream exhausted");
    }

    Y_UNIT_TEST(RequestShouldPassThroughPendingQueue)
    {
        TBootstrap b;

        auto* request = b.AddWithoutProcessing(1, 101, 1, "a");
        auto future = request->AccessPromise().GetFuture();

        UNIT_ASSERT(!future.HasValue());
        UNIT_ASSERT_VALUES_EQUAL("P[(1:a)],C[]", b.Dump());
        UNIT_ASSERT_VALUES_EQUAL(0, b.GetAllocationCount());
        b.CheckPendingQueueMetrics(1, 1, 0, 0, 0);
        b.CheckUnflushedQueueMetrics(0, 0, 0, 0, 0);

        UNIT_ASSERT(b.TryProcessPendingRequests());

        UNIT_ASSERT(future.HasValue());
        UNIT_ASSERT_VALUES_EQUAL("P[],C[(1:a)]", b.Dump());
        UNIT_ASSERT_VALUES_EQUAL(1, b.GetAllocationCount());
        b.CheckPendingQueueMetrics(0, 1, 0, 1, 0);
        b.CheckUnflushedQueueMetrics(1, 1, 0, 0, 0);
    }

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

        b.CheckPendingQueueMetrics(0, 1, 0, 1, 0);
        b.CheckUnflushedQueueMetrics(1, 1, 0, 0, 0);
        b.CheckFlushedQueueMetrics(0, 0, 0);

        b.Timer->AdvanceTime(TDuration::MilliSeconds(1));

        b.Add(2, 201, 0, "def");    // SequenceId = 2
        b.Add(2, 202, 1, "xyz");    // SequenceId = 3
        b.Add(2, 203, 2, "ijk");    // SequenceId = 4

        b.Timer->AdvanceTime(TDuration::MilliSeconds(2));
        b.RequestManager.UpdateStats();

        b.CheckPendingQueueMetrics(2, 2, 2000, 2, 0);
        b.CheckUnflushedQueueMetrics(2, 2, 3000, 0, 0);
        b.CheckFlushedQueueMetrics(0, 0, 0);

        b.Timer->AdvanceTime(TDuration::MilliSeconds(1));
        b.SetFlushed(1);
        b.Timer->AdvanceTime(TDuration::MilliSeconds(5));
        b.Evict(1);
        b.TryProcessPendingRequests();
        b.Timer->AdvanceTime(TDuration::MilliSeconds(3));
        b.RequestManager.UpdateStats();

        b.CheckPendingQueueMetrics(1, 2, 11000, 3, 8000);
        b.CheckUnflushedQueueMetrics(2, 2, 11000, 1, 4000);
        b.CheckFlushedQueueMetrics(0, 1, 1);

        b.SetFlushed(2);
        b.SetFlushed(3);
        b.Timer->AdvanceTime(TDuration::MilliSeconds(2));
        b.RequestManager.UpdateStats();

        b.CheckPendingQueueMetrics(1, 2, 13000, 3, 8000);
        b.CheckUnflushedQueueMetrics(0, 2, 11000, 3, 18000);
        b.CheckFlushedQueueMetrics(2, 2, 1);

        b.Evict(2);
        b.Evict(3);
        b.TryProcessPendingRequests();
        b.SetFlushed(4);
        b.Evict(4);
        b.Timer->AdvanceTime(TDuration::MilliSeconds(1));
        b.RequestManager.UpdateStats();

        b.CheckPendingQueueMetrics(0, 2, 13000, 4, 21000);
        b.CheckUnflushedQueueMetrics(0, 2, 11000, 4, 18000);
        b.CheckFlushedQueueMetrics(0, 2, 4);

        // Max value is calculated over a sliding window with 15 buckets
        for (int i = 0; i <= 15; i++) {
            b.RequestManager.UpdateStats();
        }

        b.CheckPendingQueueMetrics(0, 0, 0, 4, 21000);
        b.CheckUnflushedQueueMetrics(0, 0, 0, 4, 18000);
        b.CheckFlushedQueueMetrics(0, 0, 4);
    }

    Y_UNIT_TEST(ShouldSupportBackpressure)
    {
        TBootstrap b;

        auto f1 = b.Add(1, 101, 0, "abc");

        UNIT_ASSERT(b.RequestManager.SetBackpressureStatusForNode(2));
        UNIT_ASSERT(b.RequestManager.SetBackpressureStatusForNode(3));

        UNIT_ASSERT(!b.RequestManager.SetBackpressureStatusForNode(2));

        auto f2 = b.Add(2, 202, 0, "def");
        auto f3 = b.Add(3, 303, 0, "ghi");

        UNIT_ASSERT(f1.HasValue());
        UNIT_ASSERT(!f2.HasValue());
        UNIT_ASSERT(!f3.HasValue());

        UNIT_ASSERT(b.RequestManager.ClearBackpressureStatusForNode(3));
        UNIT_ASSERT(!b.RequestManager.ClearBackpressureStatusForNode(3));

        b.TryProcessPendingRequests();

        // Request reordering in the pending queue is not allowed
        UNIT_ASSERT(!f2.HasValue());
        UNIT_ASSERT(!f3.HasValue());

        UNIT_ASSERT(b.RequestManager.ClearBackpressureStatusForNode(2));
        b.TryProcessPendingRequests();

        UNIT_ASSERT(f2.HasValue());
        UNIT_ASSERT(f3.HasValue());
    }
}

}   // namespace NCloud::NFileStore::NFuse::NWriteBackCache
