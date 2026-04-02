#include "read_response_builder.h"
#include "write_back_cache_state.h"

#include <cloud/filestore/libs/vfs_fuse/write_back_cache/test/test_persistent_storage.h>
#include <cloud/filestore/libs/vfs_fuse/write_back_cache/test/test_write_back_cache_stats.h>

#include <cloud/storage/core/libs/common/timer_test.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NFileStore::NFuse::NWriteBackCache {

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TTestData
{
    ui64 Offset = 0;
    ui64 Length = 0;
};

////////////////////////////////////////////////////////////////////////////////

class TBootstrap: private IQueuedOperationsProcessor
{
private:
    std::shared_ptr<TTestWriteBackCacheStats> Stats;
    TWriteBackCacheState State;

    const TString Flushed = TString("\0\0ABCD\0\0\0\0IJKL", 14);
    const TString Expected = TString("\0\0ABSPQR\0\0IXYL\0Z", 16);

public:
    TBootstrap()
        : Stats(std::make_shared<TTestWriteBackCacheStats>())
        , State(
              *this,
              std::make_shared<TTestTimer>(),
              Stats->GetWriteBackCacheStateStats(),
              Stats->GetNodeStateHolderStats(),
              Stats->GetWriteDataRequestManagerStats(),
              "[tag]")
    {
        State.Init(std::make_shared<TTestStorage>(Stats));

        Write(2, "ABCD");
        Write(10, "IJKL");

        auto future = State.AddFlushRequest(1);
        State.FlushSucceeded(1, 2);
        Y_ABORT_UNLESS(future.HasValue());

        Write(4, "SPQR");
        Write(11, "XY");
        Write(15, "Z");
    }

    TString ReadData(ui64 offset, ui64 length, TVector<TTestData> chunks)
    {
        auto result = TString(length, 'x');

        auto request = std::make_shared<NProto::TReadDataRequest>();
        request->SetNodeId(1);
        request->SetOffset(offset);
        request->SetLength(length);

        NProto::TReadDataResponse response;
        response.SetLength(
            Min(offset + length, Flushed.size()) - Min(offset, Flushed.size()));

        for (const auto& chunk: chunks) {
            auto* iovec = request->AddIovecs();
            iovec->SetBase(
                reinterpret_cast<ui64>(result.begin() + chunk.Offset));
            iovec->SetLength(chunk.Length);

            ui64 end = Min(Flushed.size(), offset + chunk.Length);
            if (offset < end) {
                Flushed.substr(offset, end - offset)
                    .copy(result.begin() + chunk.Offset, end - offset);
                offset = end;
            }
        }

        TReadResponseBuilder builder(*request);
        builder.AugmentResponseWithCachedData(response, State);

        return result.substr(0, response.GetLength());
    }

    TString GetExpected(ui64 offset, ui64 length)
    {
        ui64 begin = Min(offset, Expected.size());
        ui64 end = Min(offset + length, Expected.size());
        return Expected.substr(begin, end - begin);
    }

private:
    void ScheduleFlushNode(ui64 nodeId) override
    {
        Y_UNUSED(nodeId);
    }

    void Write(ui64 offset, TString data)
    {
        auto request = std::make_shared<NProto::TWriteDataRequest>();
        request->SetNodeId(1);
        request->SetOffset(offset);
        request->SetBuffer(std::move(data));

        auto future = State.AddWriteDataRequest(std::move(request));

        UNIT_ASSERT(future.HasValue());
    }

    void Flush()
    {}
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TReadResponseBuilderTest)
{
    Y_UNIT_TEST(Simple)
    {
        TBootstrap b;

        UNIT_ASSERT_VALUES_EQUAL("ABSP", b.ReadData(2, 4, {{0, 4}}));
    }

    Y_UNIT_TEST(Single)
    {
        TBootstrap b;

        for (ui64 begin = 0; begin < 17; begin++) {
            for (ui64 end = begin + 1; end <= 17; end++) {
                auto expected = b.GetExpected(begin, end - begin);
                auto actual =
                    b.ReadData(begin, end - begin, {{0, end - begin}});
                UNIT_ASSERT_VALUES_EQUAL_C(
                    expected,
                    actual,
                    "Wrong data at [" << begin << ", " << end << ")");
            }
        }
    }

    Y_UNIT_TEST(Double)
    {
        TBootstrap b;

        for (ui64 len1 = 1; len1 <= 16; len1++) {
            for (ui64 len2 = 1; len2 <= 17 - len1; len2++) {
                for (ui64 ofs = 0; ofs <= 17 - len1 - len2; ofs++) {
                    auto expected = b.GetExpected(ofs, len1 + len2);
                    auto actual =
                        b.ReadData(ofs, len1 + len2, {{0, len1}, {len1, len2}});
                    UNIT_ASSERT_VALUES_EQUAL_C(
                        expected,
                        actual,
                        "Wrong data at [" << ofs << ", " << ofs + len1 << "), ["
                                          << ofs + len1 << ", "
                                          << ofs + len1 + len2 << ")");
                }
            }
        }
    }

    Y_UNIT_TEST(Multiple)
    {
        TBootstrap b;

        auto expected = b.GetExpected(0, 16);
        auto actual =
            b.ReadData(0, 16, {{0, 1}, {1, 2}, {3, 4}, {7, 4}, {11, 5}});
        UNIT_ASSERT_VALUES_EQUAL(expected, actual);
    }
}

}   // namespace NCloud::NFileStore::NFuse::NWriteBackCache
