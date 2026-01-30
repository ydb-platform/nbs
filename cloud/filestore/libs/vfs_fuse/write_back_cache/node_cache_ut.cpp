#include "node_cache.h"

#include "cached_write_data_request.h"
#include "persistent_storage_impl.h"

#include <cloud/filestore/public/api/protos/data.pb.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NFileStore::NFuse::NWriteBackCache {

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TBootstrap
{
    TTestStorage Storage;
    ui64 SequenceId = 0;
    TVector<std::unique_ptr<TCachedWriteDataRequest>> Requests;
    NWriteBackCache::TNodeCache Cache;

    void PushUnflushed(ui64 offset, TString data)
    {
        Cache.PushUnflushed(MakeRequest(offset, std::move(data)));
    }

    std::unique_ptr<TCachedWriteDataRequest> MakeRequest(
        ui64 offset,
        TString data)
    {
        NProto::TWriteDataRequest request;
        request.SetOffset(offset);
        *request.MutableBuffer() = std::move(data);

        auto serialized = TCachedWriteDataRequestSerializer::TrySerialize(
            ++SequenceId,
            request,
            Storage);

        Y_ABORT_UNLESS(serialized);
        return serialized;
    }

    TString VisitCachedData(ui64 offset, ui64 byteCount) const
    {
        TStringBuilder out;
        Cache.VisitCachedData(
            offset,
            byteCount,
            [&out](TCachedDataPart part)
            {
                if (!out.empty()) {
                    out << ", ";
                }
                out << part.Offset << ":" << part.Data;
                return true;
            });
        return out;
    }

    TString VisitUnflushedRequests() const
    {
        TStringBuilder out;
        Cache.VisitUnflushedRequests(
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

Y_UNIT_TEST_SUITE(TNodeCacheTest)
{
    Y_UNIT_TEST(Simple)
    {
        TBootstrap b;

        b.PushUnflushed(3, "abc");
        b.PushUnflushed(1, "def");
        b.PushUnflushed(7, "xyz");

        UNIT_ASSERT(!b.Cache.Empty());
        UNIT_ASSERT(b.Cache.HasUnflushedRequests());
        UNIT_ASSERT_VALUES_EQUAL(b.Cache.GetMinUnflushedSequenceId(), 1);
        UNIT_ASSERT(!b.Cache.HasFlushedRequests());
        UNIT_ASSERT_VALUES_EQUAL(b.Cache.GetMinFlushedSequenceId(), 0);
        UNIT_ASSERT_VALUES_EQUAL("2:ef, 4:bc, 7:xy", b.VisitCachedData(2, 7));
        UNIT_ASSERT_VALUES_EQUAL(
            "3:abc, 1:def, 7:xyz",
            b.VisitUnflushedRequests());
    }

    Y_UNIT_TEST(SimpleWithFlush)
    {
        TBootstrap b;

        b.PushUnflushed(3, "abc");
        b.PushUnflushed(1, "def");
        b.PushUnflushed(7, "xyz");

        b.Cache.SetFrontFlushed();

        UNIT_ASSERT(!b.Cache.Empty());
        UNIT_ASSERT(b.Cache.HasUnflushedRequests());
        UNIT_ASSERT_VALUES_EQUAL(b.Cache.GetMinUnflushedSequenceId(), 2);
        UNIT_ASSERT(b.Cache.HasFlushedRequests());
        UNIT_ASSERT_VALUES_EQUAL(b.Cache.GetMinFlushedSequenceId(), 1);
        UNIT_ASSERT_VALUES_EQUAL("2:ef, 4:bc, 7:xy", b.VisitCachedData(2, 7));
        UNIT_ASSERT_VALUES_EQUAL("1:def, 7:xyz", b.VisitUnflushedRequests());
    }

    Y_UNIT_TEST(SimpleWithFlushAndEvict)
    {
        TBootstrap b;

        b.PushUnflushed(3, "abc");
        b.PushUnflushed(1, "def");
        b.PushUnflushed(7, "xyz");

        b.Cache.SetFrontFlushed();
        b.Cache.PopFlushed();

        UNIT_ASSERT(!b.Cache.Empty());
        UNIT_ASSERT(b.Cache.HasUnflushedRequests());
        UNIT_ASSERT_VALUES_EQUAL(b.Cache.GetMinUnflushedSequenceId(), 2);
        UNIT_ASSERT(!b.Cache.HasFlushedRequests());
        UNIT_ASSERT_VALUES_EQUAL(b.Cache.GetMinFlushedSequenceId(), 0);
        UNIT_ASSERT_VALUES_EQUAL("2:ef, 7:xy", b.VisitCachedData(2, 7));
        UNIT_ASSERT_VALUES_EQUAL("1:def, 7:xyz", b.VisitUnflushedRequests());
    }

    Y_UNIT_TEST(SimpleWithFlushAll)
    {
        TBootstrap b;

        b.PushUnflushed(3, "abc");
        b.PushUnflushed(1, "def");
        b.PushUnflushed(7, "xyz");

        b.Cache.SetFrontFlushed();
        b.Cache.SetFrontFlushed();
        b.Cache.SetFrontFlushed();

        UNIT_ASSERT(!b.Cache.Empty());
        UNIT_ASSERT(!b.Cache.HasUnflushedRequests());
        UNIT_ASSERT_VALUES_EQUAL(b.Cache.GetMinUnflushedSequenceId(), 0);
        UNIT_ASSERT(b.Cache.HasFlushedRequests());
        UNIT_ASSERT_VALUES_EQUAL(b.Cache.GetMinFlushedSequenceId(), 1);
        UNIT_ASSERT_VALUES_EQUAL("2:ef, 4:bc, 7:xy", b.VisitCachedData(2, 7));
        UNIT_ASSERT_VALUES_EQUAL("", b.VisitUnflushedRequests());
    }

    Y_UNIT_TEST(SimpleWithFlushAllAndEvict)
    {
        TBootstrap b;

        b.PushUnflushed(3, "abc");
        b.PushUnflushed(1, "def");
        b.PushUnflushed(7, "xyz");

        b.Cache.SetFrontFlushed();
        b.Cache.SetFrontFlushed();
        b.Cache.SetFrontFlushed();

        b.Cache.PopFlushed();

        UNIT_ASSERT(!b.Cache.Empty());
        UNIT_ASSERT(!b.Cache.HasUnflushedRequests());
        UNIT_ASSERT_VALUES_EQUAL(b.Cache.GetMinUnflushedSequenceId(), 0);
        UNIT_ASSERT(b.Cache.HasFlushedRequests());
        UNIT_ASSERT_VALUES_EQUAL(b.Cache.GetMinFlushedSequenceId(), 2);
        UNIT_ASSERT_VALUES_EQUAL("2:ef, 7:xy", b.VisitCachedData(2, 7));
        UNIT_ASSERT_VALUES_EQUAL("", b.VisitUnflushedRequests());
    }

    Y_UNIT_TEST(SimpleWithFlushAllAndEvictAll)
    {
        TBootstrap b;

        b.PushUnflushed(3, "abc");
        b.PushUnflushed(1, "def");
        b.PushUnflushed(7, "xyz");

        b.Cache.SetFrontFlushed();
        b.Cache.SetFrontFlushed();
        b.Cache.SetFrontFlushed();

        b.Cache.PopFlushed();
        b.Cache.PopFlushed();
        b.Cache.PopFlushed();

        UNIT_ASSERT(b.Cache.Empty());
        UNIT_ASSERT(!b.Cache.HasUnflushedRequests());
        UNIT_ASSERT_VALUES_EQUAL(b.Cache.GetMinUnflushedSequenceId(), 0);
        UNIT_ASSERT(!b.Cache.HasFlushedRequests());
        UNIT_ASSERT_VALUES_EQUAL(b.Cache.GetMinFlushedSequenceId(), 0);
        UNIT_ASSERT_VALUES_EQUAL("", b.VisitCachedData(2, 7));
        UNIT_ASSERT_VALUES_EQUAL("", b.VisitUnflushedRequests());
    }
}

}   // namespace NCloud::NFileStore::NFuse::NWriteBackCache
