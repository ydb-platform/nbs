#include "side_channel.h"

#include <cloud/filestore/libs/storage/fastshard/client/async_client.h>
#include <cloud/filestore/libs/storage/model/utils.h>

#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NFileStore {

using namespace NThreading;
using namespace NStorage::NFastShard;

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr TStringBuf FileSystemId = "fs";

////////////////////////////////////////////////////////////////////////////////

struct TTestAsyncEndpoint: public IAsyncEndpoint
{
    TPromise<NProtoSrv::TResponse> Response;
    NProtoSrv::TRequest Req;
    bool RequestReceived = false;
    TAdaptiveLock Lock;

    TFuture<NProtoSrv::TResponse> Send(NProtoSrv::TRequest req) override
    {
        auto g = Guard(Lock);
        UNIT_ASSERT(!RequestReceived);
        Req = std::move(req);
        RequestReceived = true;
        Response = NewPromise<NProtoSrv::TResponse>();
        return Response;
    }

    bool Reply(NProtoSrv::TResponse r)
    {
        auto g = Guard(Lock);
        if (!RequestReceived) {
            return false;
        }
        RequestReceived = false;
        UNIT_ASSERT(Response.Initialized());
        Response.SetValue(std::move(r));
        return true;
    }
};

struct TConnInfo
{
    TString Host;
    ui16 Port = 0;
};

struct TTestAsyncClient: public IAsyncClient
{
    TDeque<TPromise<IAsyncEndpointPtr>> Endpoints;
    TDeque<TConnInfo> ConnInfos;
    TAdaptiveLock Lock;

    TFuture<IAsyncEndpointPtr> Connect(const TString& host, ui16 port) override
    {
        auto g = Guard(Lock);
        Endpoints.push_back(NewPromise<IAsyncEndpointPtr>());
        ConnInfos.push_back({host, port});
        return Endpoints.back();
    }

    std::shared_ptr<TTestAsyncEndpoint> CompleteConnection(TConnInfo* connInfo)
    {
        auto g = Guard(Lock);
        if (Endpoints.empty()) {
            return nullptr;
        }

        UNIT_ASSERT(Endpoints.front().Initialized());
        auto e = std::make_shared<TTestAsyncEndpoint>();
        Endpoints.front().SetValue(e);
        Endpoints.pop_front();
        *connInfo = std::move(ConnInfos.front());
        ConnInfos.pop_front();

        return e;
    }

    void FailConnection(TConnInfo* connInfo)
    {
        auto g = Guard(Lock);
        if (Endpoints.empty()) {
            return;
        }

        UNIT_ASSERT(Endpoints.front().Initialized());
        Endpoints.front().SetValue(nullptr);
        Endpoints.pop_front();
        *connInfo = std::move(ConnInfos.front());
        ConnInfos.pop_front();
    }
};

////////////////////////////////////////////////////////////////////////////////

auto CC()
{
    return MakeIntrusive<TCallContext>();
}

auto ReadReq(ui64 handle, ui64 offset, ui64 len)
{
    auto r = std::make_shared<NProto::TReadDataRequest>();
    r->SetFileSystemId(TString(FileSystemId));
    r->SetHandle(handle);
    r->SetOffset(offset);
    r->SetLength(len);
    return r;
}

auto WriteReq(ui64 handle, ui64 offset, TString data)
{
    auto r = std::make_shared<NProto::TWriteDataRequest>();
    r->SetFileSystemId(TString(FileSystemId));
    r->SetHandle(handle);
    r->SetOffset(offset);
    r->SetBuffer(std::move(data));
    return r;
}

auto WriteResp(NProto::TError e)
{
    NProtoSrv::TResponse r;
    auto* wd = r.MutableWriteData();
    *wd->MutableError() = std::move(e);
    return r;
}

auto ReadResp(NProto::TError e, TString data)
{
    NProtoSrv::TResponse r;
    auto* rd = r.MutableReadData();
    *rd->MutableError() = std::move(e);
    rd->SetBuffer(std::move(data));
    return r;
}

auto BackendInfo()
{
    NProto::TBackendInfo backendInfo;
    backendInfo.SetFastShardHost("h1");
    backendInfo.SetFastShardPort(111);
    backendInfo.SetActualFileSystemId(TString(FileSystemId));
    return backendInfo;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TSideChannelTest)
{
    Y_UNIT_TEST(ShouldReadWriteAfterUpdate)
    {
        auto logging = CreateLoggingService("console", { TLOG_DEBUG });
        auto client = std::make_shared<TTestAsyncClient>();
        auto sideChannel = CreateTCPSideChannel(*logging, client);

        auto writeResponse = NewPromise<NProto::TWriteDataResponse>();
        bool success = sideChannel->ExecuteRequest(
            CC(),
            WriteReq(1, 0, TString(1_KB, 'a')),
            writeResponse);
        UNIT_ASSERT(!success);

        auto readResponse = NewPromise<NProto::TReadDataResponse>();
        success = sideChannel->ExecuteRequest(
            CC(),
            ReadReq(1, 0, 1_KB),
            readResponse);
        UNIT_ASSERT(!success);

        sideChannel->Update(BackendInfo());

        TConnInfo connInfo;
        auto e = client->CompleteConnection(&connInfo);
        UNIT_ASSERT(e);
        UNIT_ASSERT_VALUES_EQUAL("h1", connInfo.Host);
        UNIT_ASSERT_VALUES_EQUAL(111, connInfo.Port);

        UNIT_ASSERT(!client->CompleteConnection(&connInfo));

        success = sideChannel->ExecuteRequest(
            CC(),
            WriteReq(1, 0, TString(1_KB, 'a')),
            writeResponse);
        UNIT_ASSERT(success);
        UNIT_ASSERT(e->RequestReceived);
        UNIT_ASSERT_VALUES_EQUAL(
            TString(1_KB, 'a'),
            e->Req.GetWriteData().GetBuffer());
        UNIT_ASSERT(!writeResponse.HasValue());

        UNIT_ASSERT(e->Reply(WriteResp(MakeError(S_ALREADY))));
        UNIT_ASSERT(writeResponse.HasValue());
        UNIT_ASSERT_VALUES_EQUAL(
            S_ALREADY,
            writeResponse.GetValue().GetError().GetCode());

        success = sideChannel->ExecuteRequest(
            CC(),
            ReadReq(1, 0, 1_KB),
            readResponse);
        UNIT_ASSERT(success);

        UNIT_ASSERT(e->RequestReceived);
        UNIT_ASSERT_VALUES_EQUAL(1_KB, e->Req.GetReadData().GetLength());
        UNIT_ASSERT(!readResponse.HasValue());

        UNIT_ASSERT(e->Reply(ReadResp(MakeError(S_OK), TString(1_KB, 'a'))));
        UNIT_ASSERT(readResponse.HasValue());
        UNIT_ASSERT_VALUES_EQUAL(
            S_OK,
            readResponse.GetValue().GetError().GetCode());
        UNIT_ASSERT_VALUES_EQUAL(
            TString(1_KB, 'a'),
            readResponse.GetValue().GetBuffer());
    }

    Y_UNIT_TEST(ShouldProcessRequestsBeforeConnectionCompletes)
    {
        auto logging = CreateLoggingService("console", { TLOG_DEBUG });
        auto client = std::make_shared<TTestAsyncClient>();
        auto sideChannel = CreateTCPSideChannel(*logging, client);

        sideChannel->Update(BackendInfo());

        auto writeResponse = NewPromise<NProto::TWriteDataResponse>();
        bool success = sideChannel->ExecuteRequest(
            CC(),
            WriteReq(1, 0, TString(1_KB, 'a')),
            writeResponse);
        UNIT_ASSERT(success);

        TConnInfo connInfo;
        auto e = client->CompleteConnection(&connInfo);
        UNIT_ASSERT(e);
        UNIT_ASSERT_VALUES_EQUAL("h1", connInfo.Host);
        UNIT_ASSERT_VALUES_EQUAL(111, connInfo.Port);

        e = client->CompleteConnection(&connInfo);
        UNIT_ASSERT_VALUES_EQUAL("h1", connInfo.Host);
        UNIT_ASSERT_VALUES_EQUAL(111, connInfo.Port);

        UNIT_ASSERT(!client->CompleteConnection(&connInfo));

        UNIT_ASSERT(e->RequestReceived);
        UNIT_ASSERT_VALUES_EQUAL(
            TString(1_KB, 'a'),
            e->Req.GetWriteData().GetBuffer());
        UNIT_ASSERT(!writeResponse.HasValue());

        UNIT_ASSERT(e->Reply(WriteResp(MakeError(S_ALREADY))));
        UNIT_ASSERT(writeResponse.HasValue());
        UNIT_ASSERT_VALUES_EQUAL(
            S_ALREADY,
            writeResponse.GetValue().GetError().GetCode());
    }

    Y_UNIT_TEST(ShouldProcessConcurrentRequests)
    {
        auto logging = CreateLoggingService("console", { TLOG_DEBUG });
        auto client = std::make_shared<TTestAsyncClient>();
        auto sideChannel = CreateTCPSideChannel(*logging, client);

        sideChannel->Update(BackendInfo());

        TConnInfo connInfo;
        auto e = client->CompleteConnection(&connInfo);
        UNIT_ASSERT(e);
        UNIT_ASSERT_VALUES_EQUAL("h1", connInfo.Host);
        UNIT_ASSERT_VALUES_EQUAL(111, connInfo.Port);
        TVector<std::shared_ptr<TTestAsyncEndpoint>> es;
        es.push_back(std::move(e));

        UNIT_ASSERT(!client->CompleteConnection(&connInfo));

        TVector<TPromise<NProto::TWriteDataResponse>> writeResponses;
        const ui32 writeCount = 10;
        for (ui32 i = 0; i < writeCount; ++i) {
            writeResponses.emplace_back(
                NewPromise<NProto::TWriteDataResponse>());
        }

        for (ui32 i = 0; i < writeCount; ++i) {
            bool success = sideChannel->ExecuteRequest(
                CC(),
                WriteReq(1, i * 1_KB, TString(1_KB, 'a' + i)),
                writeResponses[i]);
            UNIT_ASSERT(success);
            UNIT_ASSERT(!writeResponses[i].HasValue());
        }

        for (ui32 i = 0; i < writeCount - 1; ++i) {
            e = client->CompleteConnection(&connInfo);
            UNIT_ASSERT(e);
            UNIT_ASSERT_VALUES_EQUAL("h1", connInfo.Host);
            UNIT_ASSERT_VALUES_EQUAL(111, connInfo.Port);
            es.push_back(std::move(e));
        }

        UNIT_ASSERT(!client->CompleteConnection(&connInfo));

        for (ui32 i = 0; i < writeCount; ++i) {
            UNIT_ASSERT(!writeResponses[i].HasValue());
        }

        for (ui32 i = 0; i < writeCount; ++i) {
            UNIT_ASSERT(es[i]->Reply(WriteResp(MakeError(S_ALREADY))));
            UNIT_ASSERT(writeResponses[i].HasValue());
            UNIT_ASSERT_VALUES_EQUAL(
                S_ALREADY,
                writeResponses[i].GetValue().GetError().GetCode());
        }
    }

    Y_UNIT_TEST(ShouldHandleConnectionFailure)
    {
        auto logging = CreateLoggingService("console", { TLOG_DEBUG });
        auto client = std::make_shared<TTestAsyncClient>();
        auto sideChannel = CreateTCPSideChannel(*logging, client);

        sideChannel->Update(BackendInfo());

        TConnInfo connInfo;
        client->FailConnection(&connInfo);
        UNIT_ASSERT_VALUES_EQUAL("h1", connInfo.Host);
        UNIT_ASSERT_VALUES_EQUAL(111, connInfo.Port);

        UNIT_ASSERT(!client->CompleteConnection(&connInfo));

        auto writeResponse = NewPromise<NProto::TWriteDataResponse>();
        bool success = sideChannel->ExecuteRequest(
            CC(),
            WriteReq(1, 1_KB, TString(1_KB, 'a')),
            writeResponse);
        UNIT_ASSERT(success);

        client->FailConnection(&connInfo);
        UNIT_ASSERT_VALUES_EQUAL("h1", connInfo.Host);
        UNIT_ASSERT_VALUES_EQUAL(111, connInfo.Port);

        UNIT_ASSERT(!client->CompleteConnection(&connInfo));

        UNIT_ASSERT(writeResponse.HasValue());
        UNIT_ASSERT_VALUES_EQUAL_C(
            E_UNAVAILABLE,
            writeResponse.GetValue().GetError().GetCode(),
            writeResponse.GetValue().GetError().GetMessage());
    }

    Y_UNIT_TEST(ShouldSendRequestsToCorrectEndpoint)
    {
        auto logging = CreateLoggingService("console", { TLOG_RESOURCES });
        auto client = std::make_shared<TTestAsyncClient>();
        auto sideChannel = CreateTCPSideChannel(*logging, client);

        sideChannel->Update(BackendInfo());

        TConnInfo connInfo;
        auto e = client->CompleteConnection(&connInfo);
        UNIT_ASSERT(e);
        UNIT_ASSERT_VALUES_EQUAL("h1", connInfo.Host);
        UNIT_ASSERT_VALUES_EQUAL(111, connInfo.Port);
        TVector<std::shared_ptr<TTestAsyncEndpoint>> es;
        es.push_back(std::move(e));

        UNIT_ASSERT(!client->CompleteConnection(&connInfo));

        constexpr ui32 Sid1 = 10;
        constexpr ui32 Sid2 = 15;

        auto w1 = NewPromise<NProto::TWriteDataResponse>();
        bool success = sideChannel->ExecuteRequest(
            CC(),
            WriteReq(NStorage::ShardedId(1, Sid1), 1_KB, TString(1_KB, 'a')),
            w1);
        UNIT_ASSERT(!success);

        auto backendInfo1 = BackendInfo();
        backendInfo1.SetFastShardHost("h2");
        backendInfo1.SetActualFileSystemId(
            TStringBuilder() << FileSystemId << "_s10");
        sideChannel->Update(backendInfo1);

        success = sideChannel->ExecuteRequest(
            CC(),
            WriteReq(NStorage::ShardedId(1, Sid1), 1_KB, TString(1_KB, 'a')),
            w1);
        UNIT_ASSERT(success);
        UNIT_ASSERT(!w1.HasValue());

        auto e11 = client->CompleteConnection(&connInfo);
        UNIT_ASSERT(e11);
        UNIT_ASSERT_VALUES_EQUAL("h2", connInfo.Host);
        auto e12 = client->CompleteConnection(&connInfo);
        UNIT_ASSERT(e12);
        UNIT_ASSERT_VALUES_EQUAL("h2", connInfo.Host);
        UNIT_ASSERT(e12->Reply(WriteResp(MakeError(S_ALREADY))));
        UNIT_ASSERT(w1.HasValue());
        UNIT_ASSERT_VALUES_EQUAL(S_ALREADY, w1.GetValue().GetError().GetCode());

        w1 = NewPromise<NProto::TWriteDataResponse>();
        success = sideChannel->ExecuteRequest(
            CC(),
            WriteReq(NStorage::ShardedId(1, Sid1), 1_KB, TString(1_KB, 'a')),
            w1);
        UNIT_ASSERT(success);
        UNIT_ASSERT(!w1.HasValue());

        UNIT_ASSERT(!client->CompleteConnection(&connInfo));
        UNIT_ASSERT(e11->Reply(WriteResp(MakeError(S_FALSE))));
        UNIT_ASSERT(w1.HasValue());
        UNIT_ASSERT_VALUES_EQUAL(S_FALSE, w1.GetValue().GetError().GetCode());

        auto w2 = NewPromise<NProto::TWriteDataResponse>();
        success = sideChannel->ExecuteRequest(
            CC(),
            WriteReq(NStorage::ShardedId(1, Sid2), 1_KB, TString(1_KB, 'a')),
            w2);
        UNIT_ASSERT(!success);

        auto backendInfo2 = BackendInfo();
        backendInfo2.SetFastShardHost("h3");
        backendInfo2.SetActualFileSystemId(
            TStringBuilder() << FileSystemId << "_s15");
        sideChannel->Update(backendInfo2);

        success = sideChannel->ExecuteRequest(
            CC(),
            WriteReq(NStorage::ShardedId(1, Sid2), 1_KB, TString(1_KB, 'a')),
            w2);
        UNIT_ASSERT(success);
        UNIT_ASSERT(!w2.HasValue());

        auto e21 = client->CompleteConnection(&connInfo);
        UNIT_ASSERT(e21);
        UNIT_ASSERT_VALUES_EQUAL("h3", connInfo.Host);
        auto e22 = client->CompleteConnection(&connInfo);
        UNIT_ASSERT(e22);
        UNIT_ASSERT_VALUES_EQUAL("h3", connInfo.Host);
        UNIT_ASSERT(e22->Reply(WriteResp(MakeError(E_IO))));
        UNIT_ASSERT(w2.HasValue());
        UNIT_ASSERT_VALUES_EQUAL(E_IO, w2.GetValue().GetError().GetCode());
    }
}

}   // namespace NCloud::NFileStore
