#include "write_back_cache_state.h"

#include "queued_operations.h"
#include "write_data_request.h"

#include <cloud/filestore/libs/vfs_fuse/write_back_cache/test/test_persistent_storage.h>
#include <cloud/filestore/libs/vfs_fuse/write_back_cache/test/test_write_back_cache_stats.h>
#include <cloud/filestore/public/api/protos/data.pb.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/timer.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NFileStore::NFuse::NWriteBackCache {

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TProcessor: IQueuedOperationsProcessor
{
    TStringBuilder Log;

    void ScheduleFlushNode(ui64 nodeId) override
    {
        if (!Log.Empty()) {
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
    std::shared_ptr<TTestWriteBackCacheStats> Stats;
    std::shared_ptr<TTestStorage> Storage;
    TProcessor Processor;
    TWriteBackCacheState State;

    TBootstrap()
        : Timer(CreateWallClockTimer())
        , Stats(std::make_shared<TTestWriteBackCacheStats>())
        , Storage(std::make_shared<TTestStorage>(Stats))
        , State(Storage, Processor, Timer, Stats)
    {}

    auto Add(ui64 nodeId, ui64 handle, ui64 offset, TString data)
        -> NThreading::TFuture<bool>
    {
        auto request = std::make_shared<NProto::TWriteDataRequest>();
        request->SetNodeId(nodeId);
        request->SetHandle(handle);
        request->SetOffset(offset);
        *request->MutableBuffer() = std::move(data);

        auto future = State.AddWriteDataRequest(std::move(request));

        return future.Apply([](const auto& result)
                            { return !HasError(result.GetValue()); });
    }

    bool Recreate()
    {
        State = TWriteBackCacheState(Storage, Processor, Timer, Stats);
        return State.Init();
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
        auto cachedData = State.GetCachedData(nodeId, offset, byteCount);

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
        State.VisitUnflushedCachedRequests(
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

        b.State.FlushSucceeded(1, 1);
        UNIT_ASSERT_VALUES_EQUAL("2:d, 3:xy", b.VisitCachedData(1, 1, 4));
        UNIT_ASSERT_VALUES_EQUAL(
            "2:def, 3:xyz",
            b.VisitUnflushedCachedRequests(1));

        b.State.FlushSucceeded(1, 1);
        UNIT_ASSERT_VALUES_EQUAL("3:xy", b.VisitCachedData(1, 1, 4));
        UNIT_ASSERT_VALUES_EQUAL("3:xyz", b.VisitUnflushedCachedRequests(1));

        b.State.FlushSucceeded(1, 1);
        UNIT_ASSERT_VALUES_EQUAL("", b.VisitCachedData(1, 1, 4));
        UNIT_ASSERT_VALUES_EQUAL("", b.VisitUnflushedCachedRequests(1));

        UNIT_ASSERT_VALUES_EQUAL("", b.DumpEvents());
    }

    Y_UNIT_TEST(PinCachedData)
    {
        TBootstrap b;

        // pin1, pin2, pin3 reference to the same data (1)
        auto pin1 = b.State.PinCachedData(1);
        UNIT_ASSERT(b.Add(1, 101, 1, "a").GetValue());
        auto pin2 = b.State.PinCachedData(1);
        UNIT_ASSERT(b.Add(1, 102, 2, "b").GetValue());
        UNIT_ASSERT(b.Add(1, 103, 3, "c").GetValue());
        auto pin3 = b.State.PinCachedData(1);

        b.State.FlushSucceeded(1, 2);
        UNIT_ASSERT_VALUES_EQUAL("1:a, 2:b, 3:c", b.VisitCachedData(1));

        // pin4 references to (3)
        auto pin4 = b.State.PinCachedData(1);
        UNIT_ASSERT(b.Add(1, 104, 4, "d").GetValue());

        b.State.UnpinCachedData(1, pin2);
        UNIT_ASSERT_VALUES_EQUAL("1:a, 2:b, 3:c, 4:d", b.VisitCachedData(1));

        b.State.UnpinCachedData(1, pin3);
        UNIT_ASSERT_VALUES_EQUAL("1:a, 2:b, 3:c, 4:d", b.VisitCachedData(1));

        b.State.UnpinCachedData(1, pin1);
        UNIT_ASSERT_VALUES_EQUAL("3:c, 4:d", b.VisitCachedData(1));

        b.State.FlushSucceeded(1, 2);
        UNIT_ASSERT_VALUES_EQUAL("3:c, 4:d", b.VisitCachedData(1));

        b.State.UnpinCachedData(1, pin4);
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

        b.State.FlushSucceeded(1, 1);

        UNIT_ASSERT(w3.GetValue());
        UNIT_ASSERT(!w4.HasValue());
        UNIT_ASSERT_VALUES_EQUAL("1,2", b.DumpEvents());
        UNIT_ASSERT_VALUES_EQUAL("2:b", b.VisitCachedData(1));
        UNIT_ASSERT_VALUES_EQUAL("3:c", b.VisitCachedData(2));

        b.State.FlushSucceeded(1, 1);

        UNIT_ASSERT(w4.GetValue());
        UNIT_ASSERT(!w5.HasValue());
        UNIT_ASSERT_VALUES_EQUAL("", b.DumpEvents());
        UNIT_ASSERT_VALUES_EQUAL("", b.VisitCachedData(1));
        UNIT_ASSERT_VALUES_EQUAL("3:c, 4:d", b.VisitCachedData(2));

        b.State.FlushSucceeded(2, 2);

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

        auto f1 = b.State.AddFlushRequest(2);

        UNIT_ASSERT(w1.GetValue());
        UNIT_ASSERT(w2.GetValue());
        UNIT_ASSERT(!w3.HasValue());

        UNIT_ASSERT(!f1.HasValue());

        UNIT_ASSERT_VALUES_EQUAL("1", b.DumpEvents());

        b.State.FlushSucceeded(1, 2);

        UNIT_ASSERT(w3.GetValue());

        UNIT_ASSERT_VALUES_EQUAL("2", b.DumpEvents());
    }
}

}   // namespace NCloud::NFileStore::NFuse::NWriteBackCache
