#include "write_back_cache_state.h"

#include "queued_operations.h"
#include "write_back_cache_stats.h"
#include "write_data_request.h"

#include <cloud/filestore/libs/diagnostics/metrics/metric.h>
#include <cloud/filestore/libs/vfs_fuse/write_back_cache/test/test_persistent_storage.h>
#include <cloud/filestore/public/api/protos/data.pb.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/timer.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NProto {

////////////////////////////////////////////////////////////////////////////////

static bool operator == (const TError& lhs, const TError& rhs)
{
    return lhs.GetCode() == rhs.GetCode() &&
        lhs.GetMessage() == rhs.GetMessage();
}

}   // namespace NCloud::NProto

namespace NCloud::NFileStore::NFuse::NWriteBackCache {

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TProcessor: IQueuedOperationsProcessor
{
    TStringBuilder Log;

    void ScheduleFlushNode(ui64 nodeId) override
    {
        if (!Log.empty()) {
            Log << ",";
        }
        Log << nodeId;
    }

    TString RotateLog()
    {
        TString result = Log;
        Log.clear();
        return result;
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TBootstrap
{
    ITimerPtr Timer;
    IWriteBackCacheStatsPtr Stats;
    std::shared_ptr<TTestStorage> Storage;
    TProcessor Processor;
    std::unique_ptr<TWriteBackCacheState> State;
    TWriteBackCacheStateMetrics Metrics;

    TBootstrap()
        : Timer(CreateWallClockTimer())
        , Stats(CreateWriteBackCacheStats())
        , Storage(CreateTestStorage(Stats))
        , Metrics(Stats->CreateMetrics())
    {
        Recreate();
    }

    auto Add(ui64 nodeId, ui64 handle, ui64 offset, TString data)
        -> NThreading::TFuture<bool>
    {
        auto request = std::make_shared<NProto::TWriteDataRequest>();
        request->SetNodeId(nodeId);
        request->SetHandle(handle);
        request->SetOffset(offset);
        *request->MutableBuffer() = std::move(data);

        auto future = State->AddWriteDataRequest(std::move(request));

        return future.Apply([](const auto& result)
                            { return !HasError(result.GetValue()); });
    }

    bool Recreate()
    {
        State = std::make_unique<TWriteBackCacheState>(
            Processor,
            Timer,
            Stats->GetWriteBackCacheStateStats(),
            Stats->GetNodeStateHolderStats(),
            Stats->GetWriteDataRequestManagerStats(),
            "[test]");

        return State->Init(Storage);
    }

    TString DumpEvents()
    {
        return Processor.RotateLog();
    }

    TString VisitCachedData(ui64 nodeId) const
    {
        return VisitCachedData(nodeId, 0, Max<ui64>());
    }

    TString VisitCachedData(ui64 nodeId, ui64 offset, ui64 byteCount) const
    {
        auto cachedData = State->GetCachedData(nodeId, offset, byteCount);

        TStringBuilder out;
        for (const auto& part: cachedData.Parts) {
            if (!out.empty()) {
                out << ", ";
            }
            out << part.RelativeOffset + offset << ":" << part.Data;
        }
        return out;
    }

    TString VisitUnflushedCachedRequests(ui64 nodeId) const
    {
        TStringBuilder out;
        State->VisitUnflushedRequests(
            nodeId,
            [&out](const TCachedWriteDataRequest* entry)
            {
                if (!out.empty()) {
                    out << ", ";
                }
                out << entry->GetOffset() << ":" << entry->GetBuffer();
                return true;
            });
        return out;
    }

    ui64 AcquireBarrier(ui64 nodeId) const
    {
        auto future = State->AcquireBarrier(nodeId);
        while (!future.HasValue()) {
            State->FlushSucceeded(nodeId, 1);
        }
        const auto& result = future.GetValue();
        UNIT_ASSERT(!HasError(result.GetError()));
        return result.GetResult();
    }

    void ReleaseBarrier(ui64 nodeId, ui64 barrierId) const
    {
        State->ReleaseBarrier(nodeId, barrierId);
    }

    void FlushCache(ui64 nodeId) const
    {
        auto flush = State->AddFlushRequest(nodeId);
        while (!flush.HasValue()) {
            State->FlushSucceeded(nodeId, 1);
        }
        UNIT_ASSERT(!HasError(flush.GetValue()));
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TWriteBackCacheStateTest)
{
    Y_UNIT_TEST(Simple)
    {
        TBootstrap b;

        UNIT_ASSERT(b.Add(1, 101, 1, "abc").GetValue());
        UNIT_ASSERT(b.Add(1, 101, 2, "def").GetValue());
        UNIT_ASSERT(b.Add(1, 102, 3, "xyz").GetValue());

        UNIT_ASSERT_VALUES_EQUAL("1:a, 2:d, 3:xy", b.VisitCachedData(1, 1, 4));
        UNIT_ASSERT_VALUES_EQUAL(
            "1:abc, 2:def, 3:xyz",
            b.VisitUnflushedCachedRequests(1));

        b.State->AddFlushRequest(1);
        b.State->FlushSucceeded(1, 1);
        UNIT_ASSERT_VALUES_EQUAL("2:d, 3:xy", b.VisitCachedData(1, 1, 4));
        UNIT_ASSERT_VALUES_EQUAL(
            "2:def, 3:xyz",
            b.VisitUnflushedCachedRequests(1));

        b.State->AddFlushRequest(1);
        b.State->FlushSucceeded(1, 1);
        UNIT_ASSERT_VALUES_EQUAL("3:xy", b.VisitCachedData(1, 1, 4));
        UNIT_ASSERT_VALUES_EQUAL("3:xyz", b.VisitUnflushedCachedRequests(1));

        b.State->AddFlushRequest(1);
        b.State->FlushSucceeded(1, 1);
        UNIT_ASSERT_VALUES_EQUAL("", b.VisitCachedData(1, 1, 4));
        UNIT_ASSERT_VALUES_EQUAL("", b.VisitUnflushedCachedRequests(1));
    }

    Y_UNIT_TEST(PinCachedData)
    {
        TBootstrap b;

        // pin0 references to SequenceId = 0
        auto pin0 = b.State->PinCachedData(1);
        UNIT_ASSERT_VALUES_EQUAL(0, pin0);

        UNIT_ASSERT(b.Add(1, 101, 0, "0").GetValue());  // SequenceId = 1
        b.State->AddFlushRequest(1);    // SequenceId = 2
        b.State->FlushSucceeded(1, 1);  // SequenceId = 1
        UNIT_ASSERT_VALUES_EQUAL("0:0", b.VisitCachedData(1));

        // pin1, pin2 reference to the same SequenceId = 1
        auto pin1 = b.State->PinCachedData(1);
        auto pin2 = b.State->PinCachedData(1);
        UNIT_ASSERT_VALUES_EQUAL(1, pin1);
        UNIT_ASSERT_VALUES_EQUAL(1, pin2);

        // pin3 references to the same SequenceId = 1 because no requests were
        // flushed
        UNIT_ASSERT(b.Add(1, 101, 1, "a").GetValue());  // SequenceId = 3
        auto pin3 = b.State->PinCachedData(1);
        UNIT_ASSERT_VALUES_EQUAL(1, pin3);

        // Evict data pinned by pin0
        UNIT_ASSERT_VALUES_EQUAL("0:0, 1:a", b.VisitCachedData(1));
        b.State->UnpinCachedData(1, pin0);
        UNIT_ASSERT_VALUES_EQUAL("1:a", b.VisitCachedData(1));

        UNIT_ASSERT(b.Add(1, 102, 2, "b").GetValue());  // SequenceId = 4
        UNIT_ASSERT(b.Add(1, 103, 3, "c").GetValue());  // SequenceId = 5
        UNIT_ASSERT_VALUES_EQUAL("1:a, 2:b, 3:c", b.VisitCachedData(1));

        b.State->AddFlushRequest(1);
        b.State->FlushSucceeded(1, 2);  // SequenceId = {3, 4}
        UNIT_ASSERT_VALUES_EQUAL("1:a, 2:b, 3:c", b.VisitCachedData(1));

        // pin4 references to SequenceId = 4
        auto pin4 = b.State->PinCachedData(1);
        UNIT_ASSERT_VALUES_EQUAL(4, pin4);
        UNIT_ASSERT(b.Add(1, 104, 4, "d").GetValue());  // SequenceId = 6

        b.State->UnpinCachedData(1, pin2);
        UNIT_ASSERT_VALUES_EQUAL("1:a, 2:b, 3:c, 4:d", b.VisitCachedData(1));

        b.State->UnpinCachedData(1, pin3);
        UNIT_ASSERT_VALUES_EQUAL("1:a, 2:b, 3:c, 4:d", b.VisitCachedData(1));

        b.State->UnpinCachedData(1, pin1);
        UNIT_ASSERT_VALUES_EQUAL("3:c, 4:d", b.VisitCachedData(1));

        b.State->FlushSucceeded(1, 2);
        UNIT_ASSERT_VALUES_EQUAL("3:c, 4:d", b.VisitCachedData(1));

        b.State->UnpinCachedData(1, pin4);
        UNIT_ASSERT_VALUES_EQUAL("", b.VisitCachedData(1));
    }

    Y_UNIT_TEST(Recreate)
    {
        TBootstrap b;

        UNIT_ASSERT(b.Add(1, 101, 1, "abc").GetValue());
        UNIT_ASSERT(b.Add(1, 102, 2, "def").GetValue());
        UNIT_ASSERT(b.Add(2, 102, 3, "xyz").GetValue());

        UNIT_ASSERT(b.Recreate());

        UNIT_ASSERT_VALUES_EQUAL("1:a, 2:def", b.VisitCachedData(1));
        UNIT_ASSERT_VALUES_EQUAL("3:xyz", b.VisitCachedData(2));
        UNIT_ASSERT_VALUES_EQUAL(
            "1:abc, 2:def",
            b.VisitUnflushedCachedRequests(1));
        UNIT_ASSERT_VALUES_EQUAL("3:xyz", b.VisitUnflushedCachedRequests(2));
    }

    Y_UNIT_TEST(FullStorage)
    {
        TBootstrap b;
        b.Storage->SetCapacity(2);

        auto w1 = b.Add(1, 101, 1, "a");
        auto w2 = b.Add(1, 101, 2, "b");
        auto w3 = b.Add(2, 102, 3, "c");
        auto w4 = b.Add(2, 102, 4, "d");
        auto w5 = b.Add(1, 103, 5, "e");

        UNIT_ASSERT(w1.GetValue());
        UNIT_ASSERT(w2.GetValue());
        UNIT_ASSERT(!w3.HasValue());
        UNIT_ASSERT(!w4.HasValue());
        UNIT_ASSERT(!w5.HasValue());

        UNIT_ASSERT_VALUES_EQUAL("1", b.DumpEvents());

        UNIT_ASSERT_VALUES_EQUAL("1:a, 2:b", b.VisitCachedData(1));
        UNIT_ASSERT_VALUES_EQUAL("", b.VisitCachedData(2));

        b.State->FlushSucceeded(1, 1);

        UNIT_ASSERT(w3.GetValue());
        UNIT_ASSERT(!w4.HasValue());
        UNIT_ASSERT_VALUES_EQUAL("1,2", b.DumpEvents());
        UNIT_ASSERT_VALUES_EQUAL("2:b", b.VisitCachedData(1));
        UNIT_ASSERT_VALUES_EQUAL("3:c", b.VisitCachedData(2));

        b.State->FlushSucceeded(1, 1);

        UNIT_ASSERT(w4.GetValue());
        UNIT_ASSERT(!w5.HasValue());
        UNIT_ASSERT_VALUES_EQUAL("", b.DumpEvents());
        UNIT_ASSERT_VALUES_EQUAL("", b.VisitCachedData(1));
        UNIT_ASSERT_VALUES_EQUAL("3:c, 4:d", b.VisitCachedData(2));

        b.State->FlushSucceeded(2, 2);

        UNIT_ASSERT(w5.GetValue());
        // Automatic flush does not affect pending requests
        UNIT_ASSERT_VALUES_EQUAL("", b.DumpEvents());
        UNIT_ASSERT_VALUES_EQUAL("5:e", b.VisitCachedData(1));
        UNIT_ASSERT_VALUES_EQUAL("", b.VisitCachedData(2));
    }

    Y_UNIT_TEST(FlushShouldAffectPendingRequests)
    {
        TBootstrap b;
        b.Storage->SetCapacity(2);

        auto w1 = b.Add(1, 101, 1, "a");
        auto w2 = b.Add(1, 101, 2, "b");
        auto w3 = b.Add(2, 102, 3, "c");

        auto f1 = b.State->AddFlushRequest(2);

        UNIT_ASSERT(w1.GetValue());
        UNIT_ASSERT(w2.GetValue());
        UNIT_ASSERT(!w3.HasValue());

        UNIT_ASSERT(!f1.HasValue());

        UNIT_ASSERT_VALUES_EQUAL("1", b.DumpEvents());

        b.State->FlushSucceeded(1, 2);

        UNIT_ASSERT(w3.GetValue());

        UNIT_ASSERT_VALUES_EQUAL("2", b.DumpEvents());
    }

    Y_UNIT_TEST(HandleFlushFailures)
    {
        TBootstrap b;

        UNIT_ASSERT(b.Add(1, 101, 1, "abc").GetValue());
        UNIT_ASSERT(b.Add(1, 101, 2, "123").GetValue());

        auto f1 = b.State->AddFlushRequest(1);

        UNIT_ASSERT(b.Add(1, 101, 3, "xyz").GetValue());

        auto f2 = b.State->AddFlushRequest(1);

        UNIT_ASSERT(b.Add(2, 201, 10, "789").GetValue());

        auto f3 = b.State->AddFlushAllRequest();

        auto error = MakeError(E_FAIL, "Flush failed");

        b.State->FlushFailed(2, error);

        UNIT_ASSERT(!f1.HasValue());
        UNIT_ASSERT(!f2.HasValue());
        UNIT_ASSERT_VALUES_EQUAL(error, f3.GetValue());

        auto f4 = b.State->AddFlushAllRequest();

        UNIT_ASSERT(!f4.HasValue());

        b.State->FlushFailed(1, error);

        UNIT_ASSERT_VALUES_EQUAL(error, f1.GetValue());
        UNIT_ASSERT_VALUES_EQUAL(error, f2.GetValue());
        UNIT_ASSERT_VALUES_EQUAL(error, f3.GetValue());

        b.State->FlushSucceeded(1, 3);
        b.State->FlushSucceeded(2, 1);
        UNIT_ASSERT(!b.State->HasUnflushedRequests());
        UNIT_ASSERT_VALUES_EQUAL(
            0,
            b.Metrics.WriteDataRequestDroppedCount->Get());
    }

    Y_UNIT_TEST(HandleReleaseFailures)
    {
        TBootstrap b;
        b.Storage->SetCapacity(2);

        UNIT_ASSERT(b.Add(1, 101, 1, "abc").GetValue());
        UNIT_ASSERT(b.Add(1, 102, 2, "123").GetValue());

        auto w1 = b.Add(1, 102, 4, "def");
        auto w2 = b.Add(1, 103, 5, "456");

        UNIT_ASSERT(!w1.HasValue());
        UNIT_ASSERT(!w2.HasValue());

        // There are no requests with handle=100 - it immediately succeeds
        auto c0 = b.State->AddReleaseHandleRequest(1, 100);

        UNIT_ASSERT(c0.HasValue());
        UNIT_ASSERT(!HasError(c0.GetValue()));

        auto c1 = b.State->AddReleaseHandleRequest(1, 101);
        UNIT_ASSERT(!c1.HasValue());

        auto c3 = b.State->AddReleaseHandleRequest(1, 103);
        UNIT_ASSERT(!c3.HasValue());

        // Flush the first request in the queue with handle=101
        b.State->FlushSucceeded(1, 1);

        UNIT_ASSERT(c1.HasValue());
        UNIT_ASSERT(!HasError(c1.GetValue()));
        UNIT_ASSERT(!c3.HasValue());

        UNIT_ASSERT(w1.HasValue());
        UNIT_ASSERT(w1.GetValue());
        UNIT_ASSERT(!w2.HasValue());

        auto error = MakeError(E_FAIL, "Flush failed");

        // Nothing happens until all handles are requested to be released
        b.State->FlushFailed(1, error);

        UNIT_ASSERT(!c3.HasValue());

        auto c2 = b.State->AddReleaseHandleRequest(1, 102);

        UNIT_ASSERT(!c2.HasValue());

        // Cache should be emptied and pending requests should be failed
        // Two requests are dropped
        b.State->FlushFailed(1, error);

        UNIT_ASSERT(w2.HasValue());
        UNIT_ASSERT(!w2.GetValue());

        UNIT_ASSERT(c2.HasValue());
        UNIT_ASSERT_VALUES_EQUAL(error, c2.GetValue());
        UNIT_ASSERT(c3.HasValue());
        UNIT_ASSERT_VALUES_EQUAL(error, c3.GetValue());

        UNIT_ASSERT(!b.State->HasUnflushedRequests());
        UNIT_ASSERT_VALUES_EQUAL(
            2,
            b.Metrics.WriteDataRequestDroppedCount->Get());
    }

    Y_UNIT_TEST(ShouldSupportBarriers)
    {
        {
            // Simple
            TBootstrap b;
            auto barrier = b.State->AcquireBarrier(1);
            UNIT_ASSERT(barrier.HasValue());
            UNIT_ASSERT(!HasError(barrier.GetValue().GetError()));

            UNIT_ASSERT(b.Add(1, 101, 1, "abc").GetValue());
            UNIT_ASSERT_VALUES_EQUAL("", b.DumpEvents());
            UNIT_ASSERT_VALUES_EQUAL("", b.VisitUnflushedCachedRequests(1));

            auto flush = b.State->AddFlushRequest(1);
            UNIT_ASSERT_VALUES_EQUAL("", b.DumpEvents());
            UNIT_ASSERT_VALUES_EQUAL("", b.VisitUnflushedCachedRequests(1));

            b.State->ReleaseBarrier(1, barrier.GetValue().GetResult());
            UNIT_ASSERT_VALUES_EQUAL("1", b.DumpEvents());
            UNIT_ASSERT_VALUES_EQUAL(
                "1:abc",
                b.VisitUnflushedCachedRequests(1));
        }

        {
            // Different nodes
            TBootstrap b;
            auto barrier = b.State->AcquireBarrier(2);
            UNIT_ASSERT(barrier.HasValue());
            UNIT_ASSERT(!HasError(barrier.GetValue().GetError()));

            UNIT_ASSERT(b.Add(1, 101, 1, "abc").GetValue());
            UNIT_ASSERT_VALUES_EQUAL("", b.DumpEvents());
            UNIT_ASSERT_VALUES_EQUAL(
                "1:abc",
                b.VisitUnflushedCachedRequests(1));

            auto flush = b.State->AddFlushRequest(1);
            UNIT_ASSERT_VALUES_EQUAL("1", b.DumpEvents());
            UNIT_ASSERT_VALUES_EQUAL(
                "1:abc",
                b.VisitUnflushedCachedRequests(1));

            b.State->ReleaseBarrier(2, barrier.GetValue().GetResult());
            UNIT_ASSERT_VALUES_EQUAL("", b.DumpEvents());
            UNIT_ASSERT_VALUES_EQUAL(
                "1:abc",
                b.VisitUnflushedCachedRequests(1));
        }

        {
            // Multiple barriers
            TBootstrap b;
            auto barrier1 = b.State->AcquireBarrier(1);
            auto barrier2 = b.State->AcquireBarrier(1);
            UNIT_ASSERT(barrier1.HasValue());
            UNIT_ASSERT(barrier2.HasValue());

            UNIT_ASSERT(b.Add(1, 101, 1, "abc").GetValue());
            auto barrier3 = b.State->AcquireBarrier(1);
            auto barrier4 = b.State->AcquireBarrier(1);
            UNIT_ASSERT(!barrier3.HasValue());
            UNIT_ASSERT(!barrier4.HasValue());
            UNIT_ASSERT_VALUES_EQUAL("", b.DumpEvents());
            UNIT_ASSERT_VALUES_EQUAL("", b.VisitUnflushedCachedRequests(1));

            b.State->ReleaseBarrier(1, barrier1.GetValue().GetResult());
            UNIT_ASSERT_VALUES_EQUAL("", b.DumpEvents());
            UNIT_ASSERT_VALUES_EQUAL("", b.VisitUnflushedCachedRequests(1));

            b.State->ReleaseBarrier(1, barrier2.GetValue().GetResult());
            UNIT_ASSERT_VALUES_EQUAL("1", b.DumpEvents());
            UNIT_ASSERT_VALUES_EQUAL(
                "1:abc",
                b.VisitUnflushedCachedRequests(1));

            b.State->FlushSucceeded(1, 1);
            UNIT_ASSERT(barrier3.HasValue());
            UNIT_ASSERT(barrier4.HasValue());
        }

        {
            // Flush failure
            TBootstrap b;
            UNIT_ASSERT(b.Add(1, 101, 1, "abc").GetValue());
            auto barrier = b.State->AcquireBarrier(1);
            UNIT_ASSERT(b.Add(1, 101, 4, "def").GetValue());

            UNIT_ASSERT_VALUES_EQUAL("1", b.DumpEvents());
            UNIT_ASSERT_VALUES_EQUAL(
                "1:abc",
                b.VisitUnflushedCachedRequests(1));

            b.State->FlushFailed(1, MakeError(E_FAIL, "Flush failed"));
            UNIT_ASSERT(barrier.HasValue());
            UNIT_ASSERT(HasError(barrier.GetValue().GetError()));

            UNIT_ASSERT_VALUES_EQUAL("", b.DumpEvents());
            UNIT_ASSERT_VALUES_EQUAL(
                "1:abc, 4:def",
                b.VisitUnflushedCachedRequests(1));
        }

        {
            // Complex scenario with pins
            TBootstrap b;
            b.Storage->SetCapacity(3);

            // unflushed: abc def ghi
            // pin: abc
            // barrier: def
            UNIT_ASSERT(b.Add(1, 101, 1, "abc").GetValue());
            auto pin = b.State->PinCachedData(1);
            auto flush = b.State->AddFlushRequest(1);
            UNIT_ASSERT(b.Add(1, 101, 4, "def").GetValue());
            auto barrier = b.State->AcquireBarrier(1);
            UNIT_ASSERT(b.Add(1, 101, 7, "ghi").GetValue());
            UNIT_ASSERT_VALUES_EQUAL("1", b.DumpEvents());
            UNIT_ASSERT_VALUES_EQUAL(
                "1:abc, 4:def",
                b.VisitUnflushedCachedRequests(1));

            UNIT_ASSERT(!flush.HasValue());
            UNIT_ASSERT(!barrier.HasValue());

            // flushed: abc
            // unflushed: def ghi
            // pin: abc
            // barrier: def
            b.State->FlushSucceeded(1, 1);
            UNIT_ASSERT(flush.HasValue());
            UNIT_ASSERT(!barrier.HasValue());
            // Flush is requested because of AcquireBarrier
            UNIT_ASSERT_VALUES_EQUAL("1", b.DumpEvents());
            UNIT_ASSERT_VALUES_EQUAL(
                "4:def",
                b.VisitUnflushedCachedRequests(1));

            // flushed: abc def
            // unflushed: ghi
            // pin: abc
            // barrier: def
            b.State->FlushSucceeded(1, 1);
            UNIT_ASSERT(flush.HasValue());
            // Barrier is not acquired because of pin
            UNIT_ASSERT(!barrier.HasValue());
            // Flush is no more requested because of barrier acquisition request
            UNIT_ASSERT_VALUES_EQUAL("", b.DumpEvents());
            UNIT_ASSERT_VALUES_EQUAL("", b.VisitUnflushedCachedRequests(1));

            // unflushed: ghi
            // barrier: def
            b.State->UnpinCachedData(1, pin);
            UNIT_ASSERT(barrier.HasValue());
            UNIT_ASSERT(!HasError(barrier.GetValue().GetError()));
            UNIT_ASSERT_VALUES_EQUAL("", b.DumpEvents());
            UNIT_ASSERT_VALUES_EQUAL("", b.VisitUnflushedCachedRequests(1));

            // pending: l
            // unflushed: ghi j k
            // barrier: def
            UNIT_ASSERT(b.Add(1, 101, 10, "j").GetValue());
            UNIT_ASSERT(b.Add(1, 101, 11, "k").GetValue());
            UNIT_ASSERT(!b.Add(1, 101, 12, "l").HasValue());
            UNIT_ASSERT_VALUES_EQUAL("", b.DumpEvents());
            UNIT_ASSERT_VALUES_EQUAL("", b.VisitUnflushedCachedRequests(1));

            // Flush is requested after barrier release
            b.State->ReleaseBarrier(1, barrier.GetValue().GetResult());
            UNIT_ASSERT_VALUES_EQUAL("1", b.DumpEvents());
            UNIT_ASSERT_VALUES_EQUAL(
                "7:ghi, 10:j, 11:k",
                b.VisitUnflushedCachedRequests(1));
        }
    }

    Y_UNIT_TEST(ShouldReportNodeSize)
    {
        TBootstrap b;

        // The node is not cached
        UNIT_ASSERT_VALUES_EQUAL(0, b.State->GetMaxWrittenOffset(1));

        // The node is cached but not referenced
        UNIT_ASSERT(b.Add(1, 101, 0, "abc").GetValue());
        UNIT_ASSERT_VALUES_EQUAL(3, b.State->GetMaxWrittenOffset(1));
        UNIT_ASSERT(b.Add(1, 101, 2, "def").GetValue());
        UNIT_ASSERT_VALUES_EQUAL(5, b.State->GetMaxWrittenOffset(1));
        UNIT_ASSERT(b.Add(1, 101, 1, "123").GetValue());
        UNIT_ASSERT_VALUES_EQUAL(5, b.State->GetMaxWrittenOffset(1));
        b.FlushCache(1);
        UNIT_ASSERT_VALUES_EQUAL(0, b.State->GetMaxWrittenOffset(1));

        // Single reference - flush before release
        UNIT_ASSERT(b.Add(1, 101, 0, "abc").GetValue());
        auto ref1 = b.State->PinNodeStates();
        b.FlushCache(1);
        UNIT_ASSERT_VALUES_EQUAL(3, b.State->GetMaxWrittenOffset(1));
        b.State->UnpinNodeStates(ref1);
        UNIT_ASSERT_VALUES_EQUAL(0, b.State->GetMaxWrittenOffset(1));

        // Single reference - flush after release
        UNIT_ASSERT(b.Add(1, 101, 0, "abc").GetValue());
        auto ref2 = b.State->PinNodeStates();
        UNIT_ASSERT_VALUES_EQUAL(3, b.State->GetMaxWrittenOffset(1));
        b.State->UnpinNodeStates(ref2);
        UNIT_ASSERT_VALUES_EQUAL(3, b.State->GetMaxWrittenOffset(1));
        b.FlushCache(1);
        UNIT_ASSERT_VALUES_EQUAL(0, b.State->GetMaxWrittenOffset(1));

        // Single reference - resurrect node state
        UNIT_ASSERT(b.Add(1, 101, 0, "abc").GetValue());
        auto ref3 = b.State->PinNodeStates();
        UNIT_ASSERT_VALUES_EQUAL(3, b.State->GetMaxWrittenOffset(1));
        b.FlushCache(1);
        UNIT_ASSERT(b.Add(1, 101, 0, "abcd").GetValue());
        b.State->UnpinNodeStates(ref3);
        UNIT_ASSERT_VALUES_EQUAL(4, b.State->GetMaxWrittenOffset(1));
        b.FlushCache(1);
        UNIT_ASSERT_VALUES_EQUAL(0, b.State->GetMaxWrittenOffset(1));

        // Multiple references
        UNIT_ASSERT(b.Add(1, 101, 0, "abc").GetValue());
        auto ref4 = b.State->PinNodeStates();
        auto ref5 = b.State->PinNodeStates();
        b.FlushCache(1);
        UNIT_ASSERT_VALUES_EQUAL(3, b.State->GetMaxWrittenOffset(1));
        b.State->UnpinNodeStates(ref5);
        UNIT_ASSERT_VALUES_EQUAL(3, b.State->GetMaxWrittenOffset(1));
        b.State->UnpinNodeStates(ref4);
        UNIT_ASSERT_VALUES_EQUAL(0, b.State->GetMaxWrittenOffset(1));

        // Newer references don't affect deleted node states
        UNIT_ASSERT(b.Add(1, 101, 0, "abc").GetValue());
        auto ref6 = b.State->PinNodeStates();
        b.FlushCache(1);
        auto ref7 = b.State->PinNodeStates();
        b.State->UnpinNodeStates(ref6);
        UNIT_ASSERT_VALUES_EQUAL(0, b.State->GetMaxWrittenOffset(1));
        b.State->UnpinNodeStates(ref7);

        // Combine with barriers
        UNIT_ASSERT(b.Add(1, 101, 2, "abc").GetValue());
        ui64 barrierId = b.AcquireBarrier(1);
        UNIT_ASSERT_VALUES_EQUAL(5, b.State->GetMaxWrittenOffset(1));
        UNIT_ASSERT(b.Add(1, 101, 0, "def").GetValue());
        UNIT_ASSERT_VALUES_EQUAL(5, b.State->GetMaxWrittenOffset(1));
        b.State->ResetMaxWrittenOffset(1);
        UNIT_ASSERT_VALUES_EQUAL(3, b.State->GetMaxWrittenOffset(1));
        b.ReleaseBarrier(1, barrierId);
        UNIT_ASSERT_VALUES_EQUAL(3, b.State->GetMaxWrittenOffset(1));
        b.FlushCache(1);
        UNIT_ASSERT_VALUES_EQUAL(0, b.State->GetMaxWrittenOffset(1));
    }
}

}   // namespace NCloud::NFileStore::NFuse::NWriteBackCache
