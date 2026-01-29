#include "write_back_cache_state.h"

#include "write_back_cache_state_listener.h"
#include "cached_write_data_request.h"
#include "persistent_storage.h"

#include <cloud/filestore/public/api/protos/data.pb.h>

#include <cloud/storage/core/libs/common/error.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NFileStore::NFuse::NWriteBackCache {

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TStorage: IPersistentStorage
{
    TMap<const void*, TString> Data;
    size_t Capacity = 0;

    bool Empty() const override
    {
        return Data.empty();
    }

    void Visit(const TVisitor& visitor) override
    {
        for (const auto& [key, value]: Data) {
            visitor(value);
        }
    }

    ui64 GetMaxSupportedAllocationByteCount() const override
    {
        return Max<ui64>();
    }

    const void* Alloc(const TAllocationWriter& writer, size_t size) override
    {
        if (Capacity && Data.size() >= Capacity) {
            return nullptr;
        }

        auto str = TString::Uninitialized(size);
        writer(str.begin(), size);
        auto [it, _] = Data.insert({str.data(), std::move(str)});
        return it->first;
    }

    // Frees a previously allocated buffer.
    void Free(const void* ptr) override
    {
        Data.erase(ptr);
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TListener: IWriteBackCacheStateListener
{
    TStringBuilder Log;

    void ShouldFlushNode(ui64 nodeId) override
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
    TStorage Storage;
    TListener Listener;
    TWriteBackCacheState State;

    TBootstrap()
        : State(Storage, Listener)
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
        State = TWriteBackCacheState(Storage, Listener);
        return State.Init();
    }

    TString DumpEvents()
    {
        return Listener.RotateLog();
    }

    TString VisitCachedData(ui64 nodeId) const
    {
        return VisitCachedData(nodeId, 0, Max<ui64>());
    }

    TString VisitCachedData(ui64 nodeId, ui64 offset, ui64 byteCount) const
    {
        TStringBuilder out;
        State.VisitCachedData(
            nodeId,
            offset,
            byteCount,
            [&out](ui64 offset, TStringBuf data)
            {
                if (!out.empty()) {
                    out << ", ";
                }
                out << offset << ":" << data;
                return true;
            });
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
                out << entry->GetOffset() << ":" << entry->GetCachedData();
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
        b.Storage.Capacity = 2;

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
        // Flush for [1] is requested because pending requests became cached
        UNIT_ASSERT_VALUES_EQUAL("1", b.DumpEvents());
        UNIT_ASSERT_VALUES_EQUAL("5:e", b.VisitCachedData(1));
        UNIT_ASSERT_VALUES_EQUAL("", b.VisitCachedData(2));
    }


    /* Y_UNIT_TEST(Simple)
    {
        TBootstrap b;

        UNIT_ASSERT(b.Add(1, 101, 1, "abc").GetValue());
        UNIT_ASSERT_VALUES_EQUAL("A(1,101);N(1,1)", b.DumpEvents());

        UNIT_ASSERT(b.Add(1, 101, 2, "def").GetValue());
        UNIT_ASSERT_VALUES_EQUAL("", b.DumpEvents());

        UNIT_ASSERT(b.Add(1, 102, 3, "xyz").GetValue());
        UNIT_ASSERT_VALUES_EQUAL("A(1,102)", b.DumpEvents());
        UNIT_ASSERT_VALUES_EQUAL("1:a, 2:d, 3:xy", b.VisitCachedData(1, 1, 4));
        UNIT_ASSERT_VALUES_EQUAL(
            "1:abc, 2:def, 3:xyz",
            b.VisitUnflushedCachedRequests(1));

        b.State.FlushSucceeded(1, 1);
        UNIT_ASSERT_VALUES_EQUAL("N(1,2)", b.DumpEvents());
        UNIT_ASSERT_VALUES_EQUAL("2:d, 3:xy", b.VisitCachedData(1, 1, 4));
        UNIT_ASSERT_VALUES_EQUAL(
            "2:def, 3:xyz",
            b.VisitUnflushedCachedRequests(1));

        b.State.FlushSucceeded(1, 1);
        UNIT_ASSERT_VALUES_EQUAL("R(1,101);N(1,3)", b.DumpEvents());
        UNIT_ASSERT_VALUES_EQUAL("3:xy", b.VisitCachedData(1, 1, 4));
        UNIT_ASSERT_VALUES_EQUAL("3:xyz", b.VisitUnflushedCachedRequests(1));

        b.State.FlushSucceeded(1, 1);
        UNIT_ASSERT_VALUES_EQUAL("R(1,102)", b.DumpEvents());
        UNIT_ASSERT_VALUES_EQUAL("", b.VisitCachedData(1, 1, 4));
        UNIT_ASSERT_VALUES_EQUAL("", b.VisitUnflushedCachedRequests(1));
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

    Y_UNIT_TEST(RequestsFlushed)
    {
        TBootstrap b;

        UNIT_ASSERT(b.Add(1, 1, 1, "a").GetValue());
        UNIT_ASSERT(b.Add(2, 1, 1, "b").GetValue());
        UNIT_ASSERT(b.Add(1, 1, 2, "c").GetValue());
        UNIT_ASSERT(b.Add(2, 1, 2, "d").GetValue());

        auto flush1 = b.State.FlushSucceeded(2, 1);
        UNIT_ASSERT_VALUES_EQUAL(4, flush1.MinNodeUnflushedSequenceId);
        UNIT_ASSERT_VALUES_EQUAL(1, flush1.MinGlobalUnflushedSequenceId);

        auto flush2 = b.State.FlushSucceeded(1, 1);
        UNIT_ASSERT_VALUES_EQUAL(3, flush2.MinNodeUnflushedSequenceId);
        UNIT_ASSERT_VALUES_EQUAL(3, flush2.MinGlobalUnflushedSequenceId);

        auto flush3 = b.State.FlushSucceeded(1, 1);
        UNIT_ASSERT_VALUES_EQUAL(0, flush3.MinNodeUnflushedSequenceId);
        UNIT_ASSERT_VALUES_EQUAL(4, flush3.MinGlobalUnflushedSequenceId);

        auto flush4 = b.State.FlushSucceeded(2, 1);
        UNIT_ASSERT_VALUES_EQUAL(0, flush4.MinNodeUnflushedSequenceId);
        UNIT_ASSERT_VALUES_EQUAL(0, flush4.MinGlobalUnflushedSequenceId);
    }


     */
}

}   // namespace NCloud::NFileStore::NFuse::NWriteBackCache
