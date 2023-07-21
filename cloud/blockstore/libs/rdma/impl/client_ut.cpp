#include "client.h"
#include "test_verbs.h"

#include <cloud/blockstore/libs/rdma/iface/protocol.h>
#include <cloud/blockstore/libs/service/context.h>

#include <cloud/storage/core/libs/diagnostics/logging.h>
#include <cloud/storage/core/libs/diagnostics/monitoring.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/stream/printf.h>

namespace NCloud::NBlockStore::NRdma {

////////////////////////////////////////////////////////////////////////////////

struct TRequestContext: public NRdma::TNullContext
{
    std::function<void(
        TStringBuf requestBuffer,
        TStringBuf responseBuffer,
        ui32 status,
        size_t responseBytes)> Handler;
};

struct TClientHandler
    : IClientHandler
{
    void HandleResponse(
        TClientRequestPtr req,
        ui32 status,
        size_t responseBytes) override
    {
        auto* rc = static_cast<TRequestContext*>(req->Context.get());

        if (rc->Handler) {
            rc->Handler(
                req->RequestBuffer,
                req->ResponseBuffer,
                status,
                responseBytes);
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

// TODO: use custom timer

Y_UNIT_TEST_SUITE(TRdmaClientTest)
{
    Y_UNIT_TEST(ShouldStartEndpoint)
    {
        auto verbs =
            NVerbs::CreateTestVerbs(MakeIntrusive<NVerbs::TTestContext>());
        auto monitoring = CreateMonitoringServiceStub();
        auto clientConfig = std::make_shared<TClientConfig>();

        auto logging = CreateLoggingService(
            "console",
            TLogSettings{TLOG_RESOURCES});

        auto client = CreateClient(
            verbs,
            logging,
            monitoring,
            clientConfig);

        client->Start();
        Y_DEFER {
            client->Stop();
        };

        auto clientEndpoint = client->StartEndpoint(
            "::",
            10020);

        Y_UNUSED(clientEndpoint);
    }

    Y_UNIT_TEST(ShouldReturnErrorUponStartEndpointTimeout)
    {
        auto verbs =
            NVerbs::CreateTestVerbs(MakeIntrusive<NVerbs::TTestContext>());
        auto monitoring = CreateMonitoringServiceStub();
        auto clientConfig = std::make_shared<TClientConfig>();
        clientConfig->MaxReconnectDelay = TDuration::Seconds(5);

        auto logging = CreateLoggingService(
            "console",
            TLogSettings{TLOG_RESOURCES});

        auto client = CreateClient(
            verbs,
            logging,
            monitoring,
            clientConfig);

        client->Start();
        Y_DEFER {
            client->Stop();
        };

        auto clientEndpoint = client->StartEndpoint(
            "::",
            10020);

        try {
            clientEndpoint.GetValue(TDuration::Seconds(10));
            UNIT_ASSERT(false);
        } catch (const TServiceError& e) {
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_RDMA_CONNECT_FAILED,
                e.GetCode(),
                e.GetMessage());
        }
    }

    Y_UNIT_TEST(ShouldProcessRequests)
    {
        auto testContext = MakeIntrusive<NVerbs::TTestContext>();
        testContext->AllowConnect = true;

        auto verbs = NVerbs::CreateTestVerbs(testContext);
        auto monitoring = CreateMonitoringServiceStub();
        auto clientConfig = std::make_shared<TClientConfig>();
        clientConfig->MaxReconnectDelay = TDuration::Seconds(1);
        clientConfig->MaxResponseDelay = TDuration::Seconds(1);

        auto logging = CreateLoggingService(
            "console",
            TLogSettings{TLOG_RESOURCES});

        auto client = CreateClient(
            verbs,
            logging,
            monitoring,
            clientConfig);

        client->Start();
        Y_DEFER {
            client->Stop();
        };

        auto clientEndpoint = client->StartEndpoint(
            "::",
            10020);

        auto ep = clientEndpoint.GetValue(TDuration::Seconds(5));

        struct TResponse
        {
            bool Received = false;
            TStringBuf Buffer;
            ui32 Status = 0;
            size_t Bytes = 0;
        };

        TManualEvent ev;
        TResponse response;

        auto makeContext = [&]()
        {
            auto ctx = std::make_unique<TRequestContext>();
            ctx->Handler = [&](TStringBuf requestBuffer,
                               TStringBuf responseBuffer,
                               ui32 status,
                               size_t responseBytes)
            {
                Y_UNUSED(requestBuffer);

                response =
                    TResponse{true, responseBuffer, status, responseBytes};
                ev.Signal();
            };
            return ctx;
        };

        size_t requestBytes = 1024;
        size_t responseBytes = 1024;
        auto r = ep->AllocateRequest(
            std::make_shared<TClientHandler>(),
            makeContext(),
            requestBytes,
            responseBytes);
        auto request = r.ExtractResult();
        auto callContext = MakeIntrusive<TCallContext>();

        // make sure that time spent on request processing before SendRequest
        // won't be counted towards rdma timeout
        auto retryDelay =
            DurationToCyclesSafe(clientConfig->MaxResponseDelay) + 1;

        callContext->SetRequestStartedCycles(GetCycleCount() - retryDelay);
        ep->SendRequest(std::move(request), callContext);

        // handle the request
        while (true) {
            with_lock (testContext->CompletionLock) {
                if (testContext->RecvEvents && testContext->ReqIds) {
                    auto* re = testContext->RecvEvents.front();
                    auto* responseMsg = reinterpret_cast<TResponseMessage*>(
                        re->sg_list[0].addr);
                    Zero(*responseMsg);
                    InitMessageHeader(responseMsg, RDMA_PROTO_VERSION);
                    responseMsg->ReqId = testContext->ReqIds.front();

                    testContext->ReqIds.pop_front();
                    testContext->RecvEvents.pop_front();
                    testContext->ProcessedRecvEvents.push_back(re);
                    testContext->CompletionHandle.Set();
                    break;
                }
            }
        }

        ev.WaitT(TDuration::Seconds(5));
        UNIT_ASSERT(response.Received);
        UNIT_ASSERT_VALUES_EQUAL(0, response.Status);

        response.Received = false;
        response.Status = 0;
        ev.Reset();

        r = ep->AllocateRequest(
            std::make_shared<TClientHandler>(),
            makeContext(),
            requestBytes,
            responseBytes);
        request = r.ExtractResult();
        callContext = MakeIntrusive<TCallContext>();
        callContext->SetRequestStartedCycles(GetCycleCount());
        ep->SendRequest(std::move(request), callContext);

        // we didn't handle the request in time
        ev.WaitT(TDuration::Seconds(5));
        UNIT_ASSERT(response.Received);
        UNIT_ASSERT_VALUES_EQUAL((ui32)RDMA_PROTO_FAIL, response.Status);

        NProto::TError error;
        bool parsed = error.ParseFromArray(
            response.Buffer.Head(response.Bytes).data(),
            response.Bytes);

        UNIT_ASSERT_VALUES_EQUAL(parsed, true);
        UNIT_ASSERT_VALUES_EQUAL(E_TIMEOUT, error.GetCode());
    }

    Y_UNIT_TEST(ShouldReconnect)
    {
        auto testContext = MakeIntrusive<NVerbs::TTestContext>();
        testContext->AllowConnect = true;

        auto verbs = NVerbs::CreateTestVerbs(testContext);
        auto monitoring = CreateMonitoringServiceStub();
        auto clientConfig = std::make_shared<TClientConfig>();
        clientConfig->MaxReconnectDelay = TDuration::Seconds(1);
        clientConfig->MaxResponseDelay = TDuration::Seconds(1);

        auto logging = CreateLoggingService(
            "console",
            TLogSettings{TLOG_RESOURCES});

        auto client = CreateClient(
            verbs,
            logging,
            monitoring,
            clientConfig);

        client->Start();
        Y_DEFER {
            client->Stop();
        };

        auto clientEndpoint = client->StartEndpoint(
            "::",
            10020);

        auto ep = clientEndpoint.GetValue(TDuration::Seconds(5));

        Disconnect(testContext);

        // wait for receive queue to initialize 2nd time after reconnect
        ui64 recv;
        do {
            recv = AtomicGet(testContext->PostRecv);
        } while (recv != 2 * clientConfig->QueueSize);

        struct TResponse
        {
            bool Received = false;
            TStringBuf Buffer;
            ui32 Status = 0;
            size_t Bytes = 0;
        };

        TManualEvent ev;
        TResponse response;

        auto makeContext = [&]()
        {
            auto ctx = std::make_unique<TRequestContext>();
            ctx->Handler = [&](TStringBuf requestBuffer,
                               TStringBuf responseBuffer,
                               ui32 status,
                               size_t responseBytes)
            {
                Y_UNUSED(requestBuffer);

                response =
                    TResponse{true, responseBuffer, status, responseBytes};
                ev.Signal();
            };
            return ctx;
        };

        size_t requestBytes = 1024;
        size_t responseBytes = 1024;
        auto r = ep->AllocateRequest(
            std::make_shared<TClientHandler>(),
            makeContext(),
            requestBytes,
            responseBytes);
        auto request = r.ExtractResult();
        auto callContext = MakeIntrusive<TCallContext>();
        auto retryDelay =
            DurationToCyclesSafe(clientConfig->MaxResponseDelay) + 1;
        callContext->SetRequestStartedCycles(GetCycleCount() - retryDelay);
        ep->SendRequest(std::move(request), callContext);

        while (true) {
            with_lock (testContext->CompletionLock) {
                if (testContext->RecvEvents && testContext->ReqIds) {
                    auto* re = testContext->RecvEvents.front();
                    auto* responseMsg = reinterpret_cast<TResponseMessage*>(
                        re->sg_list[0].addr);
                    Zero(*responseMsg);
                    InitMessageHeader(responseMsg, RDMA_PROTO_VERSION);
                    responseMsg->ReqId = testContext->ReqIds.front();

                    testContext->ReqIds.pop_front();
                    testContext->RecvEvents.pop_front();
                    testContext->ProcessedRecvEvents.push_back(re);
                    testContext->CompletionHandle.Set();
                    break;
                }
            }
        }

        ev.WaitT(TDuration::Seconds(5));
        UNIT_ASSERT(response.Received);
        UNIT_ASSERT_VALUES_EQUAL(0, response.Status);
    }
};

}   // namespace NCloud::NBlockStore::NRdma
