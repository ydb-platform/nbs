#include "flusher.h"
#include "persistent_storage_impl.h"
#include "write_data_request_builder_impl.h"

#include <cloud/filestore/libs/service/filestore_test.h>
#include <cloud/filestore/libs/storage/core/helpers.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NFileStore::NFuse::NWriteBackCache {

using IFileStoreServicePtr = std::shared_ptr<TFileStoreTest>;

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

struct TBootstrap
{
    TTestStorage Storage;
    IFileStoreServicePtr Session;
    TWriteBackCacheState State;
    TWriteDataRequestBuilder RequestBuilder;
    TFlusher Flusher;
    TRequestLogger Logger;

    TBootstrap()
        : Session(std::make_shared<TFileStoreTest>())
        , State(Storage, Flusher)
        , RequestBuilder(DefaultRequestBuilderConfig)
        , Flusher(State, RequestBuilder, Session, "test_fs")
    {
        Session->WriteDataHandler = [&](const auto&, const auto& request)
        {
            Logger.Add(*request);
            return NThreading::MakeFuture(NProto::TWriteDataResponse{});
        };
    }

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

        b.Flusher.ShouldFlushNode(1);
        UNIT_ASSERT_VALUES_EQUAL("(1, 101, 0, 5)", b.Logger.Dump());

        b.Flusher.ShouldFlushNode(2);
        UNIT_ASSERT_VALUES_EQUAL("(2, 201, 1, 3)", b.Logger.Dump());
    }
}

}   // namespace NCloud::NFileStore::NFuse::NWriteBackCache
