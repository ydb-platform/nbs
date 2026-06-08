#include "side_channel.h"

#include <cloud/filestore/libs/storage/fastshard/client/async_client.h>

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
    TPromise<IAsyncEndpointPtr> Endpoint;
    TVector<TConnInfo> ConnInfos;
    TAdaptiveLock Lock;

    TFuture<IAsyncEndpointPtr> Connect(const TString& host, ui16 port) override
    {
        auto g = Guard(Lock);
        Endpoint = NewPromise<IAsyncEndpointPtr>();
        ConnInfos.push_back({host, port});
        return Endpoint;
    }

    void CompleteConnection(IAsyncEndpointPtr e)
    {
        auto g = Guard(Lock);
        UNIT_ASSERT(Endpoint.Initialized());
        Endpoint.SetValue(std::move(e));
    }

    auto GetConnInfos() const
    {
        auto g = Guard(Lock);
        return ConnInfos;
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TSideChannelTest)
{
    Y_UNIT_TEST(ShouldReadWriteAfterUpdate)
    {
        auto client = std::make_shared<TTestAsyncClient>();
        auto sideChannel = CreateTCPSideChannel(client);
        NProto::TBackendInfo backendInfo;
        backendInfo.SetFastShardHost("h1");
        backendInfo.SetFastShardPort(111);
        sideChannel->Update(backendInfo);

        auto connInfos = client->GetConnInfos();
        UNIT_ASSERT_VALUES_EQUAL(1, connInfos.size());
        UNIT_ASSERT_VALUES_EQUAL("h1", connInfos[0].Host);
        UNIT_ASSERT_VALUES_EQUAL(111, connInfos[0].Port);

        auto cc = [] {
            return MakeIntrusive<TCallContext>();
        };

        auto readReq = [] (ui64 handle, ui64 offset, ui64 len) {
            auto r = std::make_shared<NProto::TReadDataRequest>();
            r->SetHandle(handle);
            r->SetOffset(offset);
            r->SetLength(len);
            return r;
        };

        auto writeReq = [] (ui64 handle, ui64 offset, TString data) {
            auto r = std::make_shared<NProto::TWriteDataRequest>();
            r->SetHandle(handle);
            r->SetOffset(offset);
            r->SetBuffer(std::move(data));
            return r;
        };

        auto writeResponse = NewPromise<NProto::TWriteDataResponse>();
        bool success = sideChannel->ExecuteRequest(
            cc(),
            writeReq(1, 0, TString(1_KB, 'a')),
            writeResponse);
        UNIT_ASSERT(!success);

        auto readResponse = NewPromise<NProto::TReadDataResponse>();
        success = sideChannel->ExecuteRequest(
            cc(),
            readReq(1, 0, 1_KB),
            readResponse);
        UNIT_ASSERT(!success);

        auto e = std::make_shared<TTestAsyncEndpoint>();
        client->CompleteConnection(e);

        success = sideChannel->ExecuteRequest(
            cc(),
            writeReq(1, 0, TString(1_KB, 'a')),
            writeResponse);
        UNIT_ASSERT(success);
        UNIT_ASSERT(e->RequestReceived);
        UNIT_ASSERT_VALUES_EQUAL(
            TString(1_KB, 'a'),
            e->Req.GetWriteData().GetBuffer());
        UNIT_ASSERT(!writeResponse.HasValue());

        auto writeResp = [] (NProto::TError e) {
            NProtoSrv::TResponse r;
            auto* wd = r.MutableWriteData();
            *wd->MutableError() = std::move(e);
            return r;
        };

        e->Reply(writeResp(MakeError(S_ALREADY)));
        UNIT_ASSERT(writeResponse.HasValue());
        UNIT_ASSERT_VALUES_EQUAL(
            S_ALREADY,
            writeResponse.GetValue().GetError().GetCode());

        success = sideChannel->ExecuteRequest(
            cc(),
            readReq(1, 0, 1_KB),
            readResponse);
        UNIT_ASSERT(success);

        UNIT_ASSERT(e->RequestReceived);
        UNIT_ASSERT_VALUES_EQUAL(1_KB, e->Req.GetReadData().GetLength());
        UNIT_ASSERT(!readResponse.HasValue());

        auto readResp = [] (NProto::TError e, TString data) {
            NProtoSrv::TResponse r;
            auto* rd = r.MutableReadData();
            *rd->MutableError() = std::move(e);
            rd->SetBuffer(std::move(data));
            return r;
        };

        e->Reply(readResp(MakeError(S_OK), TString(1_KB, 'a')));
        UNIT_ASSERT(readResponse.HasValue());
        UNIT_ASSERT_VALUES_EQUAL(
            S_OK,
            readResponse.GetValue().GetError().GetCode());
        UNIT_ASSERT_VALUES_EQUAL(
            TString(1_KB, 'a'),
            readResponse.GetValue().GetBuffer());
    }
}

}   // namespace NCloud::NFileStore
