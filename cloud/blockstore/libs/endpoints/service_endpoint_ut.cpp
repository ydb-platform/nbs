#include "service_endpoint.h"

#include "endpoint_events.h"
#include "endpoint_listener.h"
#include "endpoint_manager.h"
#include "session_manager.h"

#include <cloud/blockstore/libs/diagnostics/critical_events.h>
#include <cloud/blockstore/libs/diagnostics/request_stats.h>
#include <cloud/blockstore/libs/diagnostics/server_stats.h>
#include <cloud/blockstore/libs/diagnostics/volume_stats.h>
#include <cloud/blockstore/libs/service/context.h>
#include <cloud/blockstore/libs/service/request_helpers.h>
#include <cloud/blockstore/libs/service/service.h>
#include <cloud/blockstore/libs/service/service_test.h>
#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/scheduler.h>
#include <cloud/storage/core/libs/common/timer.h>
#include <cloud/storage/core/libs/coroutine/executor.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>
#include <cloud/storage/core/libs/keyring/endpoints.h>
#include <cloud/storage/core/libs/keyring/endpoints_test.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <library/cpp/testing/unittest/registar.h>

#include <google/protobuf/util/message_differencer.h>

#include <util/generic/guid.h>
#include <util/generic/scope.h>

namespace NCloud::NBlockStore::NServer {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TTestEndpointListener final
    : public IEndpointListener
{
public:
    NThreading::TFuture<NProto::TError> StartEndpoint(
        const NProto::TStartEndpointRequest& request,
        const NProto::TVolume& volume,
        NClient::ISessionPtr session) override
    {
        Y_UNUSED(request);
        Y_UNUSED(volume);
        Y_UNUSED(session);
        return MakeFuture(NProto::TError());
    }

    NThreading::TFuture<NProto::TError> AlterEndpoint(
        const NProto::TStartEndpointRequest& request,
        const NProto::TVolume& volume,
        NClient::ISessionPtr session) override
    {
        Y_UNUSED(request);
        Y_UNUSED(volume);
        Y_UNUSED(session);
        return MakeFuture(NProto::TError());
    }

    NThreading::TFuture<NProto::TError> StopEndpoint(
        const TString& socketPath) override
    {
        Y_UNUSED(socketPath);
        return MakeFuture(NProto::TError());
    }

    NProto::TError RefreshEndpoint(
        const TString& socketPath,
        const NProto::TVolume& volume) override
    {
        Y_UNUSED(socketPath);
        Y_UNUSED(volume);
        return NProto::TError();
    }

    NThreading::TFuture<NProto::TError> SwitchEndpoint(
        const NProto::TStartEndpointRequest& request,
        const NProto::TVolume& volume,
        NClient::ISessionPtr session) override
    {
        Y_UNUSED(request);
        Y_UNUSED(volume);
        Y_UNUSED(session);
        return MakeFuture(NProto::TError());
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TTestSessionManager final
    : public ISessionManager
{
    using TCreateSessionHandler
        = std::function<NThreading::TFuture<TSessionOrError>()>;

    using TRemoveSessionHandler
        = std::function<NThreading::TFuture<NProto::TError>()>;

    using TAlterSessionHandler
        = std::function<NThreading::TFuture<NProto::TError>()>;

    using TGetSessionHandler
        = std::function<NThreading::TFuture<TSessionOrError>()>;

    using TGetProfileHandler
        = std::function<TResultOrError<NProto::TClientPerformanceProfile>()>;

    TCreateSessionHandler CreateSessionHandler;
    TRemoveSessionHandler RemoveSessionHandler;
    TAlterSessionHandler AlterSessionHandler;
    TGetSessionHandler GetSessionHandler;
    TGetProfileHandler GetProfileHandler;

    NThreading::TFuture<TSessionOrError> CreateSession(
        TCallContextPtr callContext,
        const NProto::TStartEndpointRequest& request) override
    {
        Y_UNUSED(callContext);
        Y_UNUSED(request);
        return CreateSessionHandler();
    }

    NThreading::TFuture<NProto::TError> RemoveSession(
        TCallContextPtr callContext,
        const TString& socketPath,
        const NProto::THeaders& headers) override
    {
        Y_UNUSED(callContext);
        Y_UNUSED(socketPath);
        Y_UNUSED(headers);
        return RemoveSessionHandler();
    }

    NThreading::TFuture<NProto::TError> AlterSession(
        TCallContextPtr callContext,
        const TString& socketPath,
        NProto::EVolumeAccessMode accessMode,
        NProto::EVolumeMountMode mountMode,
        ui64 mountSeqNumber,
        const NProto::THeaders& headers) override
    {
        Y_UNUSED(callContext);
        Y_UNUSED(socketPath);
        Y_UNUSED(accessMode);
        Y_UNUSED(mountMode);
        Y_UNUSED(mountSeqNumber);
        Y_UNUSED(headers);
        return AlterSessionHandler();
    }

    NThreading::TFuture<TSessionOrError> GetSession(
        TCallContextPtr callContext,
        const TString& socketPath,
        const NProto::THeaders& headers) override
    {
        Y_UNUSED(callContext);
        Y_UNUSED(socketPath);
        Y_UNUSED(headers);
        return GetSessionHandler();
    }

    TResultOrError<NProto::TClientPerformanceProfile> GetProfile(
        const TString& socketPath) override
    {
        Y_UNUSED(socketPath);
        return GetProfileHandler();
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TTestEndpointManager final
    : public IEndpointManager
{
#define ENDPOINT_IMPLEMENT_METHOD(name, ...)                                   \
    using T##name##Handler = std::function<                                    \
        NThreading::TFuture<NProto::T##name##Response>(                        \
            std::shared_ptr<NProto::T##name##Request> request)                 \
        >;                                                                     \
                                                                               \
    T##name##Handler name##Handler;                                            \
    TFuture<NProto::T##name##Response> name(                                   \
        TCallContextPtr ctx,                                                   \
        std::shared_ptr<NProto::T##name##Request> request) override            \
    {                                                                          \
        Y_UNUSED(ctx);                                                         \
        return name##Handler(std::move(request));                              \
    }                                                                          \
// ENDPOINT_IMPLEMENT_METHOD

    ENDPOINT_IMPLEMENT_METHOD(StartEndpoint)
    ENDPOINT_IMPLEMENT_METHOD(StopEndpoint)
    ENDPOINT_IMPLEMENT_METHOD(ListEndpoints)
    ENDPOINT_IMPLEMENT_METHOD(DescribeEndpoint)
    ENDPOINT_IMPLEMENT_METHOD(RefreshEndpoint)

#undef ENDPOINT_IMPLEMENT_METHOD
};

////////////////////////////////////////////////////////////////////////////////

NProto::TKickEndpointResponse KickEndpoint(
    IBlockStore& service,
    ui32 keyringId,
    ui32 requestId = 42)
{
    auto request = std::make_shared<NProto::TKickEndpointRequest>();
    request->MutableHeaders()->SetRequestId(requestId);
    request->SetKeyringId(keyringId);

    auto future = service.KickEndpoint(
        MakeIntrusive<TCallContext>(),
        std::move(request));

     return future.GetValue(TDuration::Seconds(5));
}


}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TServiceEndpointTest)
{
    Y_UNIT_TEST(ShouldHandleKickEndpoint)
    {
        auto keyringId = 13;
        const TString dirPath = "./" + CreateGuidAsString();
        auto endpointStorage = CreateFileEndpointStorage(dirPath);
        auto mutableStorage = CreateFileMutableEndpointStorage(dirPath);

        auto initError = mutableStorage->Init();
        UNIT_ASSERT_C(!HasError(initError), initError);

        Y_DEFER {
            auto error = mutableStorage->Remove();
            UNIT_ASSERT_C(!HasError(error), error);
        };

        TString unixSocket = "testSocket";
        TString diskId = "testDiskId";
        auto ipcType = NProto::IPC_GRPC;

        TVector<std::shared_ptr<NProto::TStartEndpointRequest>> endpoints;
        auto endpointManager = std::make_shared<TTestEndpointManager>();
        endpointManager->StartEndpointHandler = [&] (
            std::shared_ptr<NProto::TStartEndpointRequest> request)
        {
            endpoints.push_back(std::move(request));
            return MakeFuture(NProto::TStartEndpointResponse());
        };

        auto executor = TExecutor::Create("TestService");
        executor->Start();

        auto endpointService = CreateMultipleEndpointService(
            std::make_shared<TTestService>(),
            CreateWallClockTimer(),
            CreateSchedulerStub(),
            CreateLoggingService("console"),
            CreateRequestStatsStub(),
            CreateVolumeStatsStub(),
            CreateServerStatsStub(),
            executor,
            endpointStorage,
            endpointManager,
            {});

        NProto::TStartEndpointRequest request;
        request.SetUnixSocketPath(unixSocket);
        request.SetDiskId(diskId);
        request.SetClientId("testClientId");
        request.SetIpcType(ipcType);

        auto strOrError = SerializeEndpoint(request);
        UNIT_ASSERT_C(!HasError(strOrError), strOrError.GetError());

        auto error = endpointStorage->AddEndpoint(
            ToString(keyringId),
            strOrError.GetResult());
        UNIT_ASSERT_C(!HasError(error), error);

        {
            ui32 requestId = 325;
            auto response = KickEndpoint(
                *endpointService,
                keyringId,
                requestId);
            UNIT_ASSERT(!HasError(response));

            UNIT_ASSERT_VALUES_EQUAL(1, endpoints.size());
            const auto& endpoint = *endpoints[0];

            google::protobuf::util::MessageDifferencer comparator;
            request.MutableHeaders()->SetRequestId(requestId);
            UNIT_ASSERT(comparator.Equals(endpoint, request));
        }

        {
            auto wrongKeyringId = keyringId + 42;
            auto response = KickEndpoint(*endpointService, wrongKeyringId);
            UNIT_ASSERT(HasError(response)
                && response.GetError().GetCode() == E_INVALID_STATE);
        }

        executor->Stop();
    }

    Y_UNIT_TEST(ShouldTimeoutFrozenRequest)
    {
        const TString dirPath = "./" + CreateGuidAsString();
        auto endpointStorage = CreateFileEndpointStorage(dirPath);
        auto mutableStorage = CreateFileMutableEndpointStorage(dirPath);

        auto initError = mutableStorage->Init();
        UNIT_ASSERT_C(!HasError(initError), initError);

        Y_DEFER {
            auto error = mutableStorage->Remove();
            UNIT_ASSERT_C(!HasError(error), error);
        };

        auto logging = CreateLoggingService("console");

        auto scheduler = CreateScheduler();
        scheduler->Start();
        Y_DEFER {
            scheduler->Stop();
        };

        auto startEndpointPromise = NewPromise<ISessionManager::TSessionOrError>();
        auto stopEndpointPromise = NewPromise<NProto::TError>();

        auto sessionManager = std::make_shared<TTestSessionManager>();
        sessionManager->CreateSessionHandler = [&] () {
            return startEndpointPromise;
        };
        sessionManager->RemoveSessionHandler = [&] () {
            return stopEndpointPromise;
        };

        auto executor = TExecutor::Create("TestService");
        executor->Start();

        auto endpointManager = CreateEndpointManager(
            logging,
            CreateServerStatsStub(),
            executor,
            CreateEndpointEventProxy(),
            sessionManager,
            {{ NProto::IPC_GRPC, std::make_shared<TTestEndpointListener>() }},
            "");

        auto endpointService = CreateMultipleEndpointService(
            std::make_shared<TTestService>(),
            CreateWallClockTimer(),
            scheduler,
            logging,
            CreateRequestStatsStub(),
            CreateVolumeStatsStub(),
            CreateServerStatsStub(),
            executor,
            endpointStorage,
            endpointManager,
            {});

        const TString diskId = "testDiskId";
        const TString socketPath = "testSocket";

        {
            auto request = std::make_shared<NProto::TStartEndpointRequest>();
            request->MutableHeaders()->SetRequestTimeout(100);
            request->SetDiskId(diskId);
            request->SetUnixSocketPath(socketPath);
            request->SetClientId("testClientId");

            auto future = endpointService->StartEndpoint(
                MakeIntrusive<TCallContext>(),
                request);

            auto response = future.GetValue(TDuration::Seconds(3));
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_TIMEOUT,
                response.GetError().GetCode(),
                response);
        }

        {
            auto request = std::make_shared<NProto::TStopEndpointRequest>();
            request->MutableHeaders()->SetRequestTimeout(100);
            request->SetUnixSocketPath(socketPath);

            auto future = endpointService->StopEndpoint(
                MakeIntrusive<TCallContext>(),
                request);

            auto response = future.GetValue(TDuration::Seconds(3));
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_REJECTED,
                response.GetError().GetCode(),
                response);

            startEndpointPromise.SetValue(NProto::TError{});

            auto future2 = endpointService->StopEndpoint(
                MakeIntrusive<TCallContext>(),
                request);

            auto response2 = future2.GetValue(TDuration::Seconds(3));
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_TIMEOUT,
                response2.GetError().GetCode(),
                response2);
        }

        {
            startEndpointPromise = NewPromise<ISessionManager::TSessionOrError>();

            NProto::TStartEndpointRequest startRequest;
            startRequest.SetDiskId(diskId);
            startRequest.SetUnixSocketPath(socketPath);
            startRequest.SetClientId("testClientId");

            auto strOrError = SerializeEndpoint(startRequest);
            UNIT_ASSERT_C(!HasError(strOrError), strOrError.GetError());

            auto keyringId = 13;
            auto error = endpointStorage->AddEndpoint(
                ToString(keyringId),
                strOrError.GetResult());
            UNIT_ASSERT_C(!HasError(error), error);

            auto request = std::make_shared<NProto::TKickEndpointRequest>();
            request->MutableHeaders()->SetRequestTimeout(100);
            request->SetKeyringId(keyringId);

            auto future = endpointService->KickEndpoint(
                MakeIntrusive<TCallContext>(),
                request);

            auto response = future.GetValue(TDuration::Seconds(3));
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_REJECTED,
                response.GetError().GetCode(),
                response);

            stopEndpointPromise.SetValue({});

            auto future2 = endpointService->KickEndpoint(
                MakeIntrusive<TCallContext>(),
                request);

            auto response2 = future2.GetValue(TDuration::Seconds(3));
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_TIMEOUT,
                response2.GetError().GetCode(),
                response2);

            startEndpointPromise.SetValue(NProto::TError{});

            request->MutableHeaders()->SetRequestTimeout(3000);
            auto future3 = endpointService->KickEndpoint(
                MakeIntrusive<TCallContext>(),
                request);

            auto response3 = future3.GetValue(TDuration::Seconds(3));
            UNIT_ASSERT_C(!HasError(response3), response3);
        }

        executor->Stop();
    }

    Y_UNIT_TEST(ShouldThrowCriticalEventIfFailedToRestoreEndpoint)
    {
        const TString wrongSocketPath = "wrong.socket";
        size_t startedEndpointCount = 0;

        auto endpointManager = std::make_shared<TTestEndpointManager>();
        endpointManager->StartEndpointHandler = [&] (
            std::shared_ptr<NProto::TStartEndpointRequest> request)
        {
            if (request->GetUnixSocketPath() == wrongSocketPath) {
                return MakeFuture<NProto::TStartEndpointResponse>(
                    TErrorResponse(E_FAIL));
            }

            ++startedEndpointCount;
            return MakeFuture(NProto::TStartEndpointResponse());
        };

        NMonitoring::TDynamicCountersPtr counters = new NMonitoring::TDynamicCounters();
        InitCriticalEventsCounter(counters);
        auto configCounter =
            counters->GetCounter("AppCriticalEvents/EndpointRestoringError", true);

        UNIT_ASSERT_VALUES_EQUAL(0, static_cast<int>(*configCounter));

        const TString dirPath = "./" + CreateGuidAsString();
        auto endpointStorage = CreateFileEndpointStorage(dirPath);
        auto mutableStorage = CreateFileMutableEndpointStorage(dirPath);

        auto initError = mutableStorage->Init();
        UNIT_ASSERT_C(!HasError(initError), initError);

        Y_DEFER {
            auto error = mutableStorage->Remove();
            UNIT_ASSERT_C(!HasError(error), error);
        };

        size_t counter = 0;
        size_t wrongDataCount = 3;
        size_t wrongSocketCount = 4;
        size_t correctCount = 5;

        for (size_t i = 0; i < wrongDataCount; ++i) {
            auto error = endpointStorage->AddEndpoint(
                ToString(++counter),
                "invalid proto request data");
            UNIT_ASSERT_C(!HasError(error), error);
        }

        for (size_t i = 0; i < wrongSocketCount; ++i) {
            NProto::TStartEndpointRequest request;
            request.SetUnixSocketPath(wrongSocketPath);

            auto strOrError = SerializeEndpoint(request);
            UNIT_ASSERT_C(!HasError(strOrError), strOrError.GetError());

            auto error = endpointStorage->AddEndpoint(
                ToString(++counter),
                strOrError.GetResult());
            UNIT_ASSERT_C(!HasError(error), error);
        }

        for (size_t i = 0; i < correctCount; ++i) {
            NProto::TStartEndpointRequest request;
            request.SetUnixSocketPath("endpoint.sock");

            auto strOrError = SerializeEndpoint(request);
            UNIT_ASSERT_C(!HasError(strOrError), strOrError.GetError());

            auto error = endpointStorage->AddEndpoint(
                ToString(++counter),
                strOrError.GetResult());
            UNIT_ASSERT_C(!HasError(error), error);
        }

        auto executor = TExecutor::Create("TestService");
        executor->Start();

        auto endpointService = CreateMultipleEndpointService(
            std::make_shared<TTestService>(),
            CreateWallClockTimer(),
            CreateSchedulerStub(),
            CreateLoggingService("console"),
            CreateRequestStatsStub(),
            CreateVolumeStatsStub(),
            CreateServerStatsStub(),
            executor,
            endpointStorage,
            endpointManager,
            {});

        endpointService->RestoreEndpoints().Wait();

        UNIT_ASSERT_VALUES_EQUAL(
            wrongDataCount + wrongSocketCount,
            static_cast<int>(*configCounter));
        UNIT_ASSERT_VALUES_EQUAL(correctCount, startedEndpointCount);

        executor->Stop();
    }

    Y_UNIT_TEST(ShouldThrowCriticalEventIfNotFoundEndpointStorage)
    {
        NMonitoring::TDynamicCountersPtr counters = new NMonitoring::TDynamicCounters();
        InitCriticalEventsCounter(counters);
        auto configCounter =
            counters->GetCounter("AppCriticalEvents/EndpointRestoringError", true);

        UNIT_ASSERT_VALUES_EQUAL(0, static_cast<int>(*configCounter));

        const TString dirPath = "./invalidEndpointStoragePath";
        auto endpointStorage = CreateFileEndpointStorage(dirPath);

        auto executor = TExecutor::Create("TestService");
        executor->Start();

        auto endpointService = CreateMultipleEndpointService(
            std::make_shared<TTestService>(),
            CreateWallClockTimer(),
            CreateSchedulerStub(),
            CreateLoggingService("console"),
            CreateRequestStatsStub(),
            CreateVolumeStatsStub(),
            CreateServerStatsStub(),
            executor,
            endpointStorage,
            std::make_shared<TTestEndpointManager>(),
            {});

        endpointService->RestoreEndpoints().Wait();

        UNIT_ASSERT_VALUES_EQUAL(1, static_cast<int>(*configCounter));

        executor->Stop();
    }

    Y_UNIT_TEST(ShouldNotThrowCriticalEventIfKeyringDescIsEmpty)
    {
        NMonitoring::TDynamicCountersPtr counters = new NMonitoring::TDynamicCounters();
        InitCriticalEventsCounter(counters);
        auto configCounter =
            counters->GetCounter("AppCriticalEvents/EndpointRestoringError", true);

        UNIT_ASSERT_VALUES_EQUAL(0, static_cast<int>(*configCounter));

        auto endpointStorage = CreateKeyringEndpointStorage("nbs", "");

        auto executor = TExecutor::Create("TestService");
        executor->Start();

        auto endpointService = CreateMultipleEndpointService(
            std::make_shared<TTestService>(),
            CreateWallClockTimer(),
            CreateSchedulerStub(),
            CreateLoggingService("console"),
            CreateRequestStatsStub(),
            CreateVolumeStatsStub(),
            CreateServerStatsStub(),
            executor,
            endpointStorage,
            std::make_shared<TTestEndpointManager>(),
            {});

        endpointService->RestoreEndpoints().Wait();

        UNIT_ASSERT_VALUES_EQUAL(0, static_cast<int>(*configCounter));

        executor->Stop();
    }

    Y_UNIT_TEST(ShouldHandleRestoreFlagInListEndpointsResponse)
    {
        auto trigger = NewPromise<NProto::TStartEndpointResponse>();

        auto endpointManager = std::make_shared<TTestEndpointManager>();
        endpointManager->StartEndpointHandler = [&] (
            std::shared_ptr<NProto::TStartEndpointRequest> request)
        {
            Y_UNUSED(request);
            return trigger;
        };

        endpointManager->ListEndpointsHandler = [&] (
            std::shared_ptr<NProto::TListEndpointsRequest> request)
        {
            Y_UNUSED(request);
            return MakeFuture(NProto::TListEndpointsResponse());
        };

        const TString dirPath = "./" + CreateGuidAsString();
        auto endpointStorage = CreateFileEndpointStorage(dirPath);
        auto mutableStorage = CreateFileMutableEndpointStorage(dirPath);

        auto initError = mutableStorage->Init();
        UNIT_ASSERT_C(!HasError(initError), initError);

        Y_DEFER {
            auto error = mutableStorage->Remove();
            UNIT_ASSERT_C(!HasError(error), error);
        };

        size_t endpointCount = 5;

        for (size_t i = 0; i < endpointCount; ++i) {
            NProto::TStartEndpointRequest request;
            request.SetUnixSocketPath("endpoint.sock");

            auto strOrError = SerializeEndpoint(request);
            UNIT_ASSERT_C(!HasError(strOrError), strOrError.GetError());

            auto error = endpointStorage->AddEndpoint(
                ToString(i),
                strOrError.GetResult());
            UNIT_ASSERT_C(!HasError(error), error);
        }

        auto executor = TExecutor::Create("TestService");
        executor->Start();

        auto endpointService = CreateMultipleEndpointService(
            std::make_shared<TTestService>(),
            CreateWallClockTimer(),
            CreateSchedulerStub(),
            CreateLoggingService("console"),
            CreateRequestStatsStub(),
            CreateVolumeStatsStub(),
            CreateServerStatsStub(),
            executor,
            endpointStorage,
            endpointManager,
            {});

        auto restoreFuture = endpointService->RestoreEndpoints();
        UNIT_ASSERT(!restoreFuture.HasValue());

        {
            auto future = endpointService->ListEndpoints(
                MakeIntrusive<TCallContext>(),
                std::make_shared<NProto::TListEndpointsRequest>());
            auto response = future.GetValue(TDuration::Seconds(1));
            UNIT_ASSERT(!response.GetEndpointsWereRestored());
        }

        trigger.SetValue(NProto::TStartEndpointResponse());
        restoreFuture.Wait();

        {
            auto future = endpointService->ListEndpoints(
                MakeIntrusive<TCallContext>(),
                std::make_shared<NProto::TListEndpointsRequest>());
            auto response = future.GetValue(TDuration::Seconds(1));
            UNIT_ASSERT(response.GetEndpointsWereRestored());
        }

        executor->Stop();
    }

    Y_UNIT_TEST(ShouldListKeyrings)
    {
        auto endpointManager = std::make_shared<TTestEndpointManager>();
        endpointManager->StartEndpointHandler = [&] (
            std::shared_ptr<NProto::TStartEndpointRequest> request)
        {
            Y_UNUSED(request);
            return MakeFuture(NProto::TStartEndpointResponse());
        };
        endpointManager->StopEndpointHandler = [&] (
            std::shared_ptr<NProto::TStopEndpointRequest> request)
        {
            Y_UNUSED(request);
            return MakeFuture(NProto::TStopEndpointResponse());
        };

        const TString dirPath = "./" + CreateGuidAsString();
        auto endpointStorage = CreateFileEndpointStorage(dirPath);
        auto mutableStorage = CreateFileMutableEndpointStorage(dirPath);

        auto initError = mutableStorage->Init();
        UNIT_ASSERT_C(!HasError(initError), initError);

        Y_DEFER {
            auto error = mutableStorage->Remove();
            UNIT_ASSERT_C(!HasError(error), error);
        };

        ui32 endpointCount = 42;
        THashMap<TString, NProto::TStartEndpointRequest> requests;

        for (size_t i = 0; i < endpointCount; ++i) {
            NProto::TStartEndpointRequest request;
            request.SetUnixSocketPath("testSocket" + ToString(i + 1));
            request.SetDiskId("testDiskId" + ToString(i + 1));
            request.SetIpcType(NProto::IPC_GRPC);

            auto strOrError = SerializeEndpoint(request);
            UNIT_ASSERT_C(!HasError(strOrError), strOrError.GetError());

            auto error = endpointStorage->AddEndpoint(
                ToString(i),
                strOrError.GetResult());
            UNIT_ASSERT_C(!HasError(error), error);

            requests.emplace(ToString(i), request);
        }

        auto executor = TExecutor::Create("TestService");
        executor->Start();

        auto endpointService = CreateMultipleEndpointService(
            std::make_shared<TTestService>(),
            CreateWallClockTimer(),
            CreateSchedulerStub(),
            CreateLoggingService("console"),
            CreateRequestStatsStub(),
            CreateVolumeStatsStub(),
            CreateServerStatsStub(),
            executor,
            endpointStorage,
            endpointManager,
            {});

        auto future = endpointService->ListKeyrings(
            MakeIntrusive<TCallContext>(),
            std::make_shared<NProto::TListKeyringsRequest>());

        auto response = future.GetValue(TDuration::Seconds(5));
        UNIT_ASSERT_C(!HasError(response), response);

        auto& endpoints = response.GetEndpoints();
        UNIT_ASSERT_VALUES_EQUAL(endpointCount, endpoints.size());

        for (const auto& endpoint: endpoints) {
            auto it = requests.find(endpoint.GetKeyringId());
            UNIT_ASSERT(it != requests.end());

            google::protobuf::util::MessageDifferencer comparator;
            UNIT_ASSERT(comparator.Equals(it->second, endpoint.GetRequest()));
        }

        for (size_t i = 0; i < 5; ++i) {
            auto request = std::make_shared<NProto::TStartEndpointRequest>();
            request->SetUnixSocketPath("testPersistentSocket" + ToString(i + 1));
            request->SetDiskId("testPersistentDiskId" + ToString(i + 1));
            request->SetIpcType(NProto::IPC_GRPC);
            request->SetPersistent(true);
            ++endpointCount;

            auto future = endpointService->StartEndpoint(
                MakeIntrusive<TCallContext>(),
                request);
            auto response = future.GetValue(TDuration::Seconds(5));
            UNIT_ASSERT(!HasError(response));
        }

        {
            auto future = endpointService->ListKeyrings(
                MakeIntrusive<TCallContext>(),
                std::make_shared<NProto::TListKeyringsRequest>());

            auto response = future.GetValue(TDuration::Seconds(5));
            UNIT_ASSERT_C(!HasError(response), response);
            UNIT_ASSERT_VALUES_EQUAL(endpointCount, response.GetEndpoints().size());
        }

        for (size_t i = 0; i < 5; ++i) {
            auto request = std::make_shared<NProto::TStartEndpointRequest>();
            request->SetUnixSocketPath("testTemporarySocket" + ToString(i + 1));
            request->SetDiskId("testTemporaryDiskId" + ToString(i + 1));
            request->SetIpcType(NProto::IPC_GRPC);
            request->SetPersistent(false);

            auto future = endpointService->StartEndpoint(
                MakeIntrusive<TCallContext>(),
                request);
            auto response = future.GetValue(TDuration::Seconds(5));
            UNIT_ASSERT(!HasError(response));
        }

        {
            auto future = endpointService->ListKeyrings(
                MakeIntrusive<TCallContext>(),
                std::make_shared<NProto::TListKeyringsRequest>());

            auto response = future.GetValue(TDuration::Seconds(5));
            UNIT_ASSERT_C(!HasError(response), response);
            UNIT_ASSERT_VALUES_EQUAL(endpointCount, response.GetEndpoints().size());
        }

        for (size_t i = 0; i < 3; ++i) {
            auto request = std::make_shared<NProto::TStopEndpointRequest>();
            request->SetUnixSocketPath("testPersistentSocket" + ToString(i + 1));
            --endpointCount;

            auto future = endpointService->StopEndpoint(
                MakeIntrusive<TCallContext>(),
                request);
            auto response = future.GetValue(TDuration::Seconds(5));
            UNIT_ASSERT(!HasError(response));
        }

        {
            auto future = endpointService->ListKeyrings(
                MakeIntrusive<TCallContext>(),
                std::make_shared<NProto::TListKeyringsRequest>());

            auto response = future.GetValue(TDuration::Seconds(5));
            UNIT_ASSERT_C(!HasError(response), response);
            UNIT_ASSERT_VALUES_EQUAL(endpointCount, response.GetEndpoints().size());
        }

        for (size_t i = 0; i < 3; ++i) {
            auto request = std::make_shared<NProto::TStopEndpointRequest>();
            request->SetUnixSocketPath("testTemporarySocket" + ToString(i + 1));

            auto future = endpointService->StopEndpoint(
                MakeIntrusive<TCallContext>(),
                request);
            auto response = future.GetValue(TDuration::Seconds(5));
            UNIT_ASSERT(!HasError(response));
        }

        {
            auto future = endpointService->ListKeyrings(
                MakeIntrusive<TCallContext>(),
                std::make_shared<NProto::TListKeyringsRequest>());

            auto response = future.GetValue(TDuration::Seconds(5));
            UNIT_ASSERT_C(!HasError(response), response);
            UNIT_ASSERT_VALUES_EQUAL(endpointCount, response.GetEndpoints().size());
        }

        executor->Stop();
    }

    Y_UNIT_TEST(ShouldHandleParallelStartStopEndpoints)
    {
        auto startPromise = NewPromise<ISessionManager::TSessionOrError>();
        auto stopPromise = NewPromise<NProto::TError>();

        auto sessionManager = std::make_shared<TTestSessionManager>();
        sessionManager->CreateSessionHandler = [&] () {
            return startPromise.GetFuture();
        };
        sessionManager->RemoveSessionHandler = [&] () {
            return stopPromise.GetFuture();
        };

        const TString dirPath = "./" + CreateGuidAsString();
        auto endpointStorage = CreateFileEndpointStorage(dirPath);

        auto logging = CreateLoggingService("console");

        auto executor = TExecutor::Create("TestService");
        executor->Start();

        auto endpointManager = CreateEndpointManager(
            logging,
            CreateServerStatsStub(),
            executor,
            CreateEndpointEventProxy(),
            sessionManager,
            {{ NProto::IPC_GRPC, std::make_shared<TTestEndpointListener>() }},
            "");

        auto endpointService = CreateMultipleEndpointService(
            std::make_shared<TTestService>(),
            CreateWallClockTimer(),
            CreateSchedulerStub(),
            logging,
            CreateRequestStatsStub(),
            CreateVolumeStatsStub(),
            CreateServerStatsStub(),
            executor,
            endpointStorage,
            endpointManager,
            {});

        auto unixSocket = "testSocket";

        auto startRequest = std::make_shared<NProto::TStartEndpointRequest>();
        startRequest->SetUnixSocketPath(unixSocket);
        startRequest->SetDiskId("testDiskId");
        startRequest->SetClientId("testClientId");
        startRequest->SetIpcType(NProto::IPC_GRPC);

        auto otherStartRequest = std::make_shared<NProto::TStartEndpointRequest>(
            *startRequest);
        otherStartRequest->SetIpcType(NProto::IPC_VHOST);

        auto stopRequest = std::make_shared<NProto::TStopEndpointRequest>();
        stopRequest->SetUnixSocketPath(unixSocket);

        auto ctx = MakeIntrusive<TCallContext>();

        {
            auto future1 = endpointService->StartEndpoint(ctx, startRequest);
            auto future2 = endpointService->StartEndpoint(ctx, startRequest);
            UNIT_ASSERT(!future1.HasValue());
            UNIT_ASSERT(!future2.HasValue());

            auto future3 = endpointService->StartEndpoint(ctx, otherStartRequest);
            auto response3 = future3.GetValue(TDuration::Seconds(5));
            UNIT_ASSERT_C(response3.GetError().GetCode() == E_REJECTED, response3.GetError());

            auto future = endpointService->StopEndpoint(ctx, stopRequest);
            auto response = future.GetValue(TDuration::Seconds(5));
            UNIT_ASSERT(response.GetError().GetCode() == E_REJECTED);

            startPromise.SetValue(NProto::TError{});

            auto response1 = future1.GetValue(TDuration::Seconds(5));
            auto response2 = future2.GetValue(TDuration::Seconds(5));
            UNIT_ASSERT(response1.GetError().GetCode() == S_OK);
            UNIT_ASSERT(response2.GetError().GetCode() == S_OK);
        }

        {
            auto future1 = endpointService->StopEndpoint(ctx, stopRequest);
            auto future2 = endpointService->StopEndpoint(ctx, stopRequest);
            UNIT_ASSERT(!future1.HasValue());
            UNIT_ASSERT(!future2.HasValue());

            auto future3 = endpointService->StartEndpoint(ctx, otherStartRequest);
            auto response3 = future3.GetValue(TDuration::Seconds(5));
            UNIT_ASSERT(response3.GetError().GetCode() == E_REJECTED);

            stopPromise.SetValue({});

            auto response1 = future1.GetValue(TDuration::Seconds(5));
            auto response2 = future2.GetValue(TDuration::Seconds(5));
            UNIT_ASSERT(response1.GetError().GetCode() == S_OK);
            UNIT_ASSERT(response2.GetError().GetCode() == S_OK);
        }

        executor->Stop();
    }

    Y_UNIT_TEST(ShouldWaitForRestoredEndpoints)
    {
        auto keyringId = 13;
        const TString dirPath = "./" + CreateGuidAsString();
        auto endpointStorage = CreateFileEndpointStorage(dirPath);
        auto mutableStorage = CreateFileMutableEndpointStorage(dirPath);

        auto initError = mutableStorage->Init();
        UNIT_ASSERT_C(!HasError(initError), initError);

        Y_DEFER {
            auto error = mutableStorage->Remove();
            UNIT_ASSERT_C(!HasError(error), error);
        };

        TAtomic trigger = 0;
        TManualEvent event;

        auto sessionManager = std::make_shared<TTestSessionManager>();
        sessionManager->CreateSessionHandler = [&] () {
            NProto::TError error;
            if (!AtomicGet(trigger)) {
                error.SetCode(E_TIMEOUT);
            }
            event.Signal();
            return MakeFuture(ISessionManager::TSessionOrError(error));
        };
        sessionManager->RemoveSessionHandler = [&] () {
            return MakeFuture(NProto::TError());
        };

        auto scheduler = CreateScheduler();
        scheduler->Start();
        Y_DEFER {
            scheduler->Stop();
        };

        auto logging = CreateLoggingService("console");

        auto executor = TExecutor::Create("TestService");
        executor->Start();

        auto endpointManager = CreateEndpointManager(
            logging,
            CreateServerStatsStub(),
            executor,
            CreateEndpointEventProxy(),
            sessionManager,
            {{ NProto::IPC_GRPC, std::make_shared<TTestEndpointListener>() }},
            "");

        auto endpointService = CreateMultipleEndpointService(
            std::make_shared<TTestService>(),
            CreateWallClockTimer(),
            scheduler,
            logging,
            CreateRequestStatsStub(),
            CreateVolumeStatsStub(),
            CreateServerStatsStub(),
            executor,
            endpointStorage,
            endpointManager,
            {});

        endpointService->Start();
        Y_DEFER {
            endpointService->Stop();
        };

        auto unixSocket = "testSocket";

        auto startRequest = std::make_shared<NProto::TStartEndpointRequest>();
        startRequest->SetUnixSocketPath(unixSocket);
        startRequest->SetDiskId("testDiskId");
        startRequest->SetClientId("testClientId");
        startRequest->SetIpcType(NProto::IPC_GRPC);

        auto stopRequest = std::make_shared<NProto::TStopEndpointRequest>();
        stopRequest->SetUnixSocketPath(unixSocket);

        auto ctx = MakeIntrusive<TCallContext>();

        auto strOrError = SerializeEndpoint(*startRequest);
        UNIT_ASSERT_C(!HasError(strOrError), strOrError.GetError());

        auto error = endpointStorage->AddEndpoint(
            ToString(keyringId),
            strOrError.GetResult());
        UNIT_ASSERT_C(!HasError(error), error);

        auto restoreFuture = endpointService->RestoreEndpoints();

        auto stopFuture = endpointService->StopEndpoint(ctx, stopRequest);
        auto stopResponse = stopFuture.GetValue(TDuration::Seconds(5));
        UNIT_ASSERT(stopResponse.GetError().GetCode() == E_REJECTED);

        event.Wait();
        UNIT_ASSERT(!restoreFuture.HasValue());

        AtomicSet(trigger, 1);
        UNIT_ASSERT(restoreFuture.Wait(TDuration::Seconds(5)));
        UNIT_ASSERT(restoreFuture.HasValue());

        auto future = endpointService->StopEndpoint(ctx, stopRequest);
        auto response = future.GetValue(TDuration::Seconds(5));
        UNIT_ASSERT(response.GetError().GetCode() == S_OK);

        executor->Stop();
    }
}

}   // namespace NCloud::NBlockStore::NServer
