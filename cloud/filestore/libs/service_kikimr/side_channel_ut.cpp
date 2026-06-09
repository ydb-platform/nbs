#include "side_channel.h"

#include <cloud/filestore/libs/storage/fastshard/client/async_client.h>

#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NFileStore {

using namespace NThreading;
using namespace NStorage::NFastShard;

namespace {

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

    void Reply(NProtoSrv::TResponse r)
    {
        auto g = Guard(Lock);
        UNIT_ASSERT(RequestReceived);
        RequestReceived = false;
        UNIT_ASSERT(Response.Initialized());
        Response.SetValue(std::move(r));
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
    r->SetHandle(handle);
    r->SetOffset(offset);
    r->SetLength(len);
    return r;
}

auto WriteReq(ui64 handle, ui64 offset, TString data)
{
    auto r = std::make_shared<NProto::TWriteDataRequest>();
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

        NProto::TBackendInfo backendInfo;
        backendInfo.SetFastShardHost("h1");
        backendInfo.SetFastShardPort(111);
        sideChannel->Update(backendInfo);

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

        e->Reply(WriteResp(MakeError(S_ALREADY)));
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

        e->Reply(ReadResp(MakeError(S_OK), TString(1_KB, 'a')));
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

        NProto::TBackendInfo backendInfo;
        backendInfo.SetFastShardHost("h1");
        backendInfo.SetFastShardPort(111);
        sideChannel->Update(backendInfo);

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

        e->Reply(WriteResp(MakeError(S_ALREADY)));
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

        NProto::TBackendInfo backendInfo;
        backendInfo.SetFastShardHost("h1");
        backendInfo.SetFastShardPort(111);
        sideChannel->Update(backendInfo);

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
            es[i]->Reply(WriteResp(MakeError(S_ALREADY)));
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

        NProto::TBackendInfo backendInfo;
        backendInfo.SetFastShardHost("h1");
        backendInfo.SetFastShardPort(111);
        sideChannel->Update(backendInfo);

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
}

}   // namespace NCloud::NFileStore
