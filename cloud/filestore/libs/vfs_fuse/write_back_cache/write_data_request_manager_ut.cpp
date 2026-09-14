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

        ProcessPendingRequests();

        return future.IgnoreResult();
    }

    void SetFlushed(ui64 sequenceId)
    {
        UNIT_ASSERT(
            RequestManager.SetFlushed(CachedRequests[sequenceId].get()));
    }

    void Remove(ui64 sequenceId)
    {
        UNIT_ASSERT(
            RequestManager.Remove(std::move(PendingRequests[sequenceId])));
        PendingRequests.erase(sequenceId);
    }

    void Evict(ui64 sequenceId)
    {
        UNIT_ASSERT(
            RequestManager.Evict(std::move(CachedRequests[sequenceId])));
        CachedRequests.erase(sequenceId);

        ProcessPendingRequests();
    }

    bool ClearBackpressureStatusForNode(ui64 nodeId)
    {
        bool res = RequestManager.ClearBackpressureStatusForNode(nodeId);
        ProcessPendingRequests();
        return res;
    }

    void ProcessPendingRequests()
    {
        ProcessCachedRequests();

        while (auto* pendingRequest =
                   RequestManager.GetNextPendingRequestToSerialize())
        {
            pendingRequest->SerializeToAllocation();
            UNIT_ASSERT(
                RequestManager.SetPendingRequestSerialized(pendingRequest));

            ProcessCachedRequests();
        }
    }

    void ProcessCachedRequests()
    {
        while (true) {
            auto res = RequestManager.GetNextReadyCachedRequest();
            UNIT_ASSERT(!res.Failed);
            if (!res.Request) {
                return;
            }
            auto request = std::move(res.Request);

            PendingRequests[request->GetSequenceId()]->AccessPromise().SetValue(
                {});
            PendingRequests.erase(request->GetSequenceId());
            CachedRequests[request->GetSequenceId()] = std::move(request);
        }
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

    void CheckAllocatedQueueMetrics(
        i64 expectedActiveCount,
        i64 expectedActiveMaxCount,
        i64 expectedMaxTime,
        i64 expectedCompletedCount,
        i64 expectedCompletedTime) const
    {
        UNIT_ASSERT_VALUES_EQUAL(
            expectedActiveCount,
            Metrics.AllocatedQueue.Count->Get());

        UNIT_ASSERT_VALUES_EQUAL(
            expectedActiveMaxCount,
            Metrics.AllocatedQueue.MaxCount->Get());

        UNIT_ASSERT_VALUES_EQUAL(
            expectedMaxTime,
            Metrics.AllocatedQueue.MaxTime->Get());

        UNIT_ASSERT_VALUES_EQUAL(
            expectedCompletedCount,
            Metrics.AllocatedQueue.ProcessedCount->Get());

        UNIT_ASSERT_VALUES_EQUAL(
            expectedCompletedTime,
            Metrics.AllocatedQueue.ProcessedTime->Get());
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

Y_UNIT_TEST_SUITE(TWriteDataRequestManagerTest)
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

        UNIT_ASSERT_VALUES_EQUAL("P[],C[(2:b)]", b.Dump());

        b.Evict(2);
        UNIT_ASSERT_VALUES_EQUAL(0, b.GetAllocationCount());

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

        UNIT_ASSERT(b.ClearBackpressureStatusForNode(3));
        UNIT_ASSERT(!b.ClearBackpressureStatusForNode(3));

        // Clearing node 3 cannot bypass the earlier request for node 2 because
        // requests are committed in global FIFO order.
        // requests are committed in global FIFO order.
        UNIT_ASSERT(!f2.HasValue());
        UNIT_ASSERT(!f3.HasValue());

        UNIT_ASSERT(b.ClearBackpressureStatusForNode(2));

        UNIT_ASSERT(f2.HasValue());
        UNIT_ASSERT(f3.HasValue());
    }

    Y_UNIT_TEST(ShouldCommitRequestsInSequenceWhenSerializationCompletesOutOfOrder)
    {
        TBootstrap b;

        auto* request1 = b.AddWithoutProcessing(1, 101, 0, "abc");
        auto* request2 = b.AddWithoutProcessing(2, 202, 3, "def");

        auto future1 = request1->AccessPromise().GetFuture();
        auto future2 = request2->AccessPromise().GetFuture();

        UNIT_ASSERT_VALUES_EQUAL(2, b.GetAllocationCount());
        UNIT_ASSERT_VALUES_EQUAL(
            1,
            b.RequestManager.GetMinPendingOrUnflushedSequenceId());
        UNIT_ASSERT_VALUES_EQUAL(
            2,
            b.RequestManager.GetMaxPendingOrUnflushedSequenceId());
        b.CheckAllocatedQueueMetrics(2, 2, 0, 0, 0);

        auto* requestToSerialize1 =
            b.RequestManager.GetNextPendingRequestToSerialize();
        auto* requestToSerialize2 =
            b.RequestManager.GetNextPendingRequestToSerialize();

        UNIT_ASSERT(request1 == requestToSerialize1);
        UNIT_ASSERT(request2 == requestToSerialize2);
        UNIT_ASSERT(
            b.RequestManager.GetNextPendingRequestToSerialize() == nullptr);

        requestToSerialize2->SerializeToAllocation();
        UNIT_ASSERT(
            b.RequestManager.SetPendingRequestSerialized(requestToSerialize2));

        // A newer request must not be committed before the older request has
        // finished serialization.
        UNIT_ASSERT(!b.RequestManager.GetNextReadyCachedRequest().Request);
        UNIT_ASSERT(!future1.HasValue());
        UNIT_ASSERT(!future2.HasValue());

        b.Timer->AdvanceTime(TDuration::MilliSeconds(3));
        b.RequestManager.UpdateStats();
        b.CheckAllocatedQueueMetrics(2, 2, 3000, 0, 0);

        requestToSerialize1->SerializeToAllocation();
        UNIT_ASSERT(
            b.RequestManager.SetPendingRequestSerialized(requestToSerialize1));
        b.ProcessCachedRequests();

        UNIT_ASSERT(future1.HasValue());
        UNIT_ASSERT(future2.HasValue());
        UNIT_ASSERT_VALUES_EQUAL("P[],C[(1:abc)(2:def)]", b.Dump());
        UNIT_ASSERT(!b.RequestManager.HasPendingRequests());
        b.CheckAllocatedQueueMetrics(0, 2, 3000, 2, 6000);
    }

    Y_UNIT_TEST(ShouldReuseAllocationWhenAllocatedRequestIsRemoved)
    {
        TBootstrap b;
        b.Storage->SetCapacity(1);

        b.AddWithoutProcessing(1, 101, 0, "abc");
        auto* secondRequest = b.AddWithoutProcessing(2, 202, 3, "def");
        auto secondFuture = secondRequest->AccessPromise().GetFuture();

        UNIT_ASSERT_VALUES_EQUAL(1, b.GetAllocationCount());
        UNIT_ASSERT(b.RequestManager.GetStorageIsFull());
        UNIT_ASSERT(!secondFuture.HasValue());

        b.Remove(1);

        UNIT_ASSERT_VALUES_EQUAL(1, b.GetAllocationCount());
        UNIT_ASSERT(!b.RequestManager.GetStorageIsFull());
        UNIT_ASSERT(!secondFuture.HasValue());

        b.ProcessPendingRequests();

        UNIT_ASSERT(secondFuture.HasValue());
        UNIT_ASSERT_VALUES_EQUAL("P[],C[(2:def)]", b.Dump());
    }

    Y_UNIT_TEST(ShouldReuseAllocationWhenBackpressuredRequestIsRemoved)
    {
        TBootstrap b;
        b.Storage->SetCapacity(1);

        UNIT_ASSERT(b.RequestManager.SetBackpressureStatusForNode(1));
        auto firstFuture = b.Add(1, 101, 0, "abc");
        auto secondFuture = b.Add(2, 202, 3, "def");

        UNIT_ASSERT(!firstFuture.HasValue());
        UNIT_ASSERT(!secondFuture.HasValue());
        UNIT_ASSERT_VALUES_EQUAL(1, b.GetAllocationCount());
        UNIT_ASSERT(b.RequestManager.GetStorageIsFull());

        b.Remove(1);

        UNIT_ASSERT_VALUES_EQUAL(1, b.GetAllocationCount());
        UNIT_ASSERT(!b.RequestManager.GetStorageIsFull());
        UNIT_ASSERT(!secondFuture.HasValue());

        b.ProcessPendingRequests();

        UNIT_ASSERT(secondFuture.HasValue());
        UNIT_ASSERT_VALUES_EQUAL("P[],C[(2:def)]", b.Dump());
        UNIT_ASSERT(!b.RequestManager.HasPendingRequests());
        UNIT_ASSERT(
            b.RequestManager.GetNextPendingRequestToSerialize() == nullptr);
    }
}

}   // namespace NCloud::NFileStore::NFuse::NWriteBackCache
