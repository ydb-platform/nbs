#include "flusher.h"

#include "write_data_request_builder_impl.h"

#include <cloud/filestore/libs/storage/core/helpers.h>
#include <cloud/filestore/libs/vfs_fuse/write_back_cache/test/persistent_storage_test.h>
#include <cloud/filestore/libs/vfs_fuse/write_back_cache/test/write_back_cache_stats_test.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/timer.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NFileStore::NFuse::NWriteBackCache {

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr TWriteDataRequestBuilderConfig DefaultRequestBuilderConfig = {
    .MaxWriteRequestSize = Max<ui32>(),
    .MaxWriteRequestsCount = Max<ui32>(),
    .MaxSumWriteRequestsSize = Max<ui32>(),
    .ZeroCopyWriteEnabled = false,
};

////////////////////////////////////////////////////////////////////////////////

struct TRequestLogger
{
    TStringBuilder Log;

    void Add(const NProto::TWriteDataRequest& request)
    {
        const ui64 byteCount =
            NStorage::CalculateByteCount(request) - request.GetBufferOffset();

        Log << "(" << request.GetNodeId() << ", " << request.GetHandle() << ", "
            << request.GetOffset() << ", " << byteCount << ")";
    }

    TString Dump()
    {
        TString res = Log;
        Log.clear();
        return res;
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TBootstrap: public IWriteDataRequestExecutor
{
    ITimerPtr Timer;
    TTestWriteBackCacheStats Stats;
    TTestStorage Storage;
    TWriteBackCacheState State;
    TWriteDataRequestBuilder RequestBuilder;
    TFlusher Flusher;
    TRequestLogger Logger;

    TBootstrap()
        : Timer(CreateWallClockTimer())
        , Storage(Stats)
        , State(Storage, Flusher, *Timer, Stats, {.EnableFlushFailure = false})
        , RequestBuilder(DefaultRequestBuilderConfig)
        , Flusher(State, RequestBuilder, *this, Stats, "test_fs")
    {}

    void WriteData(ui64 nodeId, ui64 handle, ui64 offset, TString data)
    {
        auto request = std::make_shared<NProto::TWriteDataRequest>();
        request->SetNodeId(nodeId);
        request->SetHandle(handle);
        request->SetOffset(offset);
        request->SetBuffer(std::move(data));

        auto future = State.AddWriteDataRequest(std::move(request));
        UNIT_ASSERT(!HasError(future.GetValue()));
    }

    void ExecuteWriteDataRequest(
        std::shared_ptr<NProto::TWriteDataRequest> request,
        std::function<void(const NProto::TWriteDataResponse&)> callback)
        override
    {
        Logger.Add(*request);
        callback(NProto::TWriteDataResponse{});
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TFlusherTest)
{
    Y_UNIT_TEST(Simple)
    {
        TBootstrap b;

        b.WriteData(1, 101, 2, "def");
        b.WriteData(2, 201, 1, "xyz");
        b.WriteData(1, 102, 0, "abc");

        b.Flusher.ScheduleFlushNode(1);
        UNIT_ASSERT_VALUES_EQUAL("(1, 101, 0, 5)", b.Logger.Dump());

        b.Flusher.ScheduleFlushNode(2);
        UNIT_ASSERT_VALUES_EQUAL("(2, 201, 1, 3)", b.Logger.Dump());
    }
}

}   // namespace NCloud::NFileStore::NFuse::NWriteBackCache
