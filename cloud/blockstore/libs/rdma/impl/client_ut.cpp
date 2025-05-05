#include "client.h"
#include "test_verbs.h"

#include <cloud/blockstore/libs/rdma/iface/protocol.h>
#include <cloud/blockstore/libs/service/context.h>

#include <cloud/storage/core/libs/diagnostics/logging.h>
#include <cloud/storage/core/libs/diagnostics/monitoring.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/stream/printf.h>

namespace NCloud::NBlockStore::NRdma {

using namespace std::chrono_literals;

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

        auto clientEndpoint = client->StartEndpoint("::", 10020);

        Y_UNUSED(clientEndpoint);
    }

    Y_UNIT_TEST(ShouldDetachFromPoller)
    {
        auto testContext = MakeIntrusive<NVerbs::TTestContext>();
        testContext->AllowConnect = true;

        auto verbs = NVerbs::CreateTestVerbs(testContext);
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

        auto shared = client->StartEndpoint("::", 10020).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL(2, shared.use_count());

        shared->Stop().Wait();
        auto deadline = GetCycleCount() + DurationToCycles(TDuration::Seconds(10));
        while (deadline > GetCycleCount() && shared.use_count() > 1) {
            SpinLockPause();
        }
        UNIT_ASSERT_VALUES_EQUAL(1, shared.use_count());
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
                E_RDMA_UNAVAILABLE,
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

    Y_UNIT_TEST(ShouldCancelRequests)
    {
        auto testContext = MakeIntrusive<NVerbs::TTestContext>();
        testContext->AllowConnect = true;

        auto verbs = NVerbs::CreateTestVerbs(testContext);
        auto monitoring = CreateMonitoringServiceStub();
        auto clientConfig = std::make_shared<TClientConfig>();
        clientConfig->MaxReconnectDelay = 1s;
        clientConfig->MaxResponseDelay = 1s;

        auto logging =
            CreateLoggingService("console", TLogSettings{TLOG_RESOURCES});

        auto client = CreateClient(verbs, logging, monitoring, clientConfig);

        client->Start();
        Y_DEFER
        {
            client->Stop();
        };

        auto clientEndpoint = client->StartEndpoint("::", 10020);

        auto ep = clientEndpoint.GetValue(5s);

        struct TResponse
        {
            bool Received = false;
            TStringBuf Buffer;
            ui32 Status = 0;
            size_t Bytes = 0;
        };

        auto makeContext = [](TResponse& response, TManualEvent& ev)
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

        TManualEvent ev1;
        TResponse response1;

        const size_t requestBytes = 1024;
        const size_t responseBytes = 1024;
        auto r1 = ep->AllocateRequest(
            std::make_shared<TClientHandler>(),
            makeContext(response1, ev1),
            requestBytes,
            responseBytes);
        auto request1 = r1.ExtractResult();
        auto callContext = MakeIntrusive<TCallContext>();

        // make sure that time spent on request processing before SendRequest
        // won't be counted towards rdma timeout
        auto retryDelay =
            DurationToCyclesSafe(clientConfig->MaxResponseDelay) + 1;

        callContext->SetRequestStartedCycles(GetCycleCount() - retryDelay);
        auto reqId1 = ep->SendRequest(std::move(request1), callContext);

        TManualEvent ev2;
        TResponse response2;

        auto r2 = ep->AllocateRequest(
            std::make_shared<TClientHandler>(),
            makeContext(response2, ev2),
            requestBytes,
            responseBytes);
        auto request2 = r2.ExtractResult();

        auto reqId2 = ep->SendRequest(std::move(request2), callContext);

        ep->CancelRequest(reqId2);

        ev2.WaitT(5s);
        UNIT_ASSERT(response2.Received);
        UNIT_ASSERT_VALUES_EQUAL((ui32)RDMA_PROTO_FAIL, response2.Status);

        NProto::TError error;
        bool parsed = error.ParseFromArray(
            response2.Buffer.Head(response2.Bytes).data(),
            response2.Bytes);

        UNIT_ASSERT(parsed);
        UNIT_ASSERT_VALUES_EQUAL(E_CANCELLED, error.GetCode());

        UNIT_ASSERT(!response1.Received);

        ep->CancelRequest(reqId1);

        ev1.WaitT(5s);
        UNIT_ASSERT(response1.Received);
        UNIT_ASSERT_VALUES_EQUAL((ui32)RDMA_PROTO_FAIL, response1.Status);

        parsed = error.ParseFromArray(
            response1.Buffer.Head(response1.Bytes).data(),
            response1.Bytes);

        UNIT_ASSERT(parsed);
        UNIT_ASSERT_VALUES_EQUAL(E_CANCELLED, error.GetCode());
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
            recv = AtomicGet(testContext->PostRecvCounter);
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

    Y_UNIT_TEST(ShouldForceReconnect)
    {
        auto testContext = MakeIntrusive<NVerbs::TTestContext>();
        testContext->AllowConnect = true;

        auto verbs = NVerbs::CreateTestVerbs(testContext);
        auto monitoring = CreateMonitoringServiceStub();
        auto clientConfig = std::make_shared<TClientConfig>();
        clientConfig->MaxReconnectDelay = TDuration::Seconds(1);
        clientConfig->MaxResponseDelay = TDuration::Seconds(1);

        auto logging =
            CreateLoggingService("console", TLogSettings{TLOG_RESOURCES});

        auto client = CreateClient(verbs, logging, monitoring, clientConfig);

        client->Start();
        Y_DEFER
        {
            client->Stop();
        };

        auto clientEndpoint = client->StartEndpoint("::", 10020);
        auto ep = clientEndpoint.GetValue(TDuration::Seconds(5));

        // Increase reconnect timer delay up to "MaxReconnectDelay".
        for (int i = 0; i < 10; i++) {
            Disconnect(testContext);
        }

        const auto now = TInstant::Now();
        ep->TryForceReconnect();

        // wait for receive queue to initialize 2nd time after reconnect
        ui64 recv;
        do {
            recv = AtomicGet(testContext->PostRecvCounter);
        } while (recv != 2 * clientConfig->QueueSize);

        // Force reconnect should be less than "MaxReconnectDelay".
        const auto elapsed = now - TInstant::Now();
        UNIT_ASSERT_LT(elapsed, clientConfig->MaxReconnectDelay);
    }

    Y_UNIT_TEST(ShouldHandleErrors)
    {
        auto context = MakeIntrusive<NVerbs::TTestContext>();
        context->AllowConnect = true;

        auto verbs = NVerbs::CreateTestVerbs(context);
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

        auto endpoint = client->StartEndpoint("::", 10020)
            .GetValue(TDuration::Seconds(5));

        auto counters = monitoring
            ->GetCounters()
            ->GetSubgroup("counters", "blockstore")
            ->GetSubgroup("component", "rdma_client");

        auto unexpected = counters->GetCounter("UnexpectedCompletions");
        auto active = counters->GetCounter("ActiveRecv");
        auto errors = counters->GetCounter("RecvErrors");
        auto unexpectedOld = unexpected->Val();
        auto errorsOld = errors->Val();

        ibv_recv_wr* wr = context->RecvEvents.back();
        auto completion = TVector<ibv_wc>();

        with_lock(context->CompletionLock) {
            // emulate IBV_QPS_ERR
            context->PostRecv = [](auto* qp, auto* wr) {
                Y_UNUSED(qp);
                Y_UNUSED(wr);
                throw TServiceError(ENODEV) << "ibv_post_recv error";
            };
            // good id, good opcode, error status
            completion.push_back({
                .wr_id = wr->wr_id,
                .status = IBV_WC_RETRY_EXC_ERR,
                .opcode = IBV_WC_RECV,
            });
            // good id, good opcode, success status, bad message
            completion.push_back({
                .wr_id = wr->wr_id,
                .opcode = IBV_WC_RECV,
            });
            // bad id, good opcode
            completion.push_back({
                .wr_id = Max<ui64>(),
                .opcode = IBV_WC_RECV,
            });
            // good id, bad opcode
            completion.push_back({
                .wr_id = wr->wr_id,
                .opcode = IBV_WC_RECV_RDMA_WITH_IMM,
            });
            context->HandleCompletionEvent = [&](ibv_wc* wc) {
                static int i = 0;
                *wc = completion[i++];
            };
            // HandleCompletionEvent will override completions, but we still
            // need to pass a valid request pointer here
            for (size_t i = 0; i < completion.size(); i++) {
                context->ProcessedRecvEvents.push_back(wr);
            }
            context->CompletionHandle.Set();
        }

        auto wait = [](auto& counter, auto value) {
            auto start = GetCycleCount();
            while (counter->Val() != value) {
                auto now = GetCycleCount();
                if (CyclesToDurationSafe(now - start) > TDuration::Seconds(5)) {
                    UNIT_FAIL("timed out waiting for counter");
                }
                SpinLockPause();
            }
        };

        wait(unexpected, unexpectedOld + 2);
        wait(errors, errorsOld + 2);
        wait(active, 8);
    }
};

}   // namespace NCloud::NBlockStore::NRdma
