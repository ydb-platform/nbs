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
#include <cloud/storage/core/libs/endpoints/keyring/keyring_endpoints.h>
#include <cloud/storage/core/libs/endpoints/fs/fs_endpoints.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <library/cpp/testing/unittest/registar.h>

#include <google/protobuf/util/message_differencer.h>

#include <util/folder/path.h>
#include <util/folder/tempdir.h>
#include <util/generic/guid.h>
#include <util/generic/scope.h>

namespace NCloud::NBlockStore::NServer {

using namespace NThreading;

using namespace std::chrono_literals;

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TTestEndpointListener final
    : public IEndpointListener
{
    using TStartEndpointHandler = std::function<TFuture<NProto::TError>(
        const NProto::TStartEndpointRequest& request)>;
    using TStopEndpointHandler = std::function<TFuture<NProto::TError>(
        const TString& socketPath)>;

    TStartEndpointHandler StartEndpointHandler = [] (
        const NProto::TStartEndpointRequest& request)
    {
        TFsPath(request.GetUnixSocketPath()).Touch();
        return MakeFuture(NProto::TError{});
    };

    TStopEndpointHandler StopEndpointHandler = [] (const TString& socketPath)
    {
        TFsPath(socketPath).DeleteIfExists();
        return MakeFuture(NProto::TError{});
    };

    TFuture<NProto::TError> StartEndpoint(
        const NProto::TStartEndpointRequest& request,
        const NProto::TVolume& volume,
        NClient::ISessionPtr session) override
    {
        Y_UNUSED(volume);
        Y_UNUSED(session);
        return StartEndpointHandler(request);
    }

    TFuture<NProto::TError> AlterEndpoint(
        const NProto::TStartEndpointRequest& request,
        const NProto::TVolume& volume,
        NClient::ISessionPtr session) override
    {
        Y_UNUSED(request);
        Y_UNUSED(volume);
        Y_UNUSED(session);
        return MakeFuture(NProto::TError());
    }

    TFuture<NProto::TError> StopEndpoint(
        const TString& socketPath) override
    {
        return StopEndpointHandler(socketPath);
    }

    NProto::TError RefreshEndpoint(
        const TString& socketPath,
        const NProto::TVolume& volume) override
    {
        Y_UNUSED(socketPath);
        Y_UNUSED(volume);
        return NProto::TError();
    }

    TFuture<NProto::TError> SwitchEndpoint(
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
        = std::function<TFuture<TSessionOrError>()>;

    using TRemoveSessionHandler
        = std::function<TFuture<NProto::TError>()>;

    using TAlterSessionHandler
        = std::function<TFuture<NProto::TError>()>;

    using TGetSessionHandler
        = std::function<TFuture<TSessionOrError>()>;

    using TGetProfileHandler
        = std::function<TResultOrError<NProto::TClientPerformanceProfile>()>;

    TCreateSessionHandler CreateSessionHandler = [] () {
        return MakeFuture<TSessionOrError>(TSessionInfo());
    };

    TRemoveSessionHandler RemoveSessionHandler = [] () {
        return MakeFuture(NProto::TError());
    };

    TAlterSessionHandler AlterSessionHandler = [] () {
        return MakeFuture(NProto::TError());
    };

    TGetSessionHandler GetSessionHandler = [] () {
        return MakeFuture<TSessionOrError>(TSessionInfo());
    };

    TGetProfileHandler GetProfileHandler = [] () {
        return NProto::TClientPerformanceProfile();
    };

    TFuture<TSessionOrError> CreateSession(
        TCallContextPtr callContext,
        const NProto::TStartEndpointRequest& request) override
    {
        Y_UNUSED(callContext);
        Y_UNUSED(request);
        return CreateSessionHandler();
    }

    TFuture<NProto::TError> RemoveSession(
        TCallContextPtr callContext,
        const TString& socketPath,
        const NProto::THeaders& headers) override
    {
        Y_UNUSED(callContext);
        Y_UNUSED(socketPath);
        Y_UNUSED(headers);
        return RemoveSessionHandler();
    }

    TFuture<NProto::TError> AlterSession(
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

    TFuture<TSessionOrError> GetSession(
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

NProto::TKickEndpointResponse KickEndpoint(
    IEndpointManager& service,
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

        TTempDir endpointsDir(dirPath);
        TTempDir dir;
        TString unixSocket = (dir.Path() / "testSocket").GetPath();
        TString diskId = "testDiskId";
        auto ipcType = NProto::IPC_GRPC;

        TMap<TString, NProto::TStartEndpointRequest> endpoints;
        auto listener = std::make_shared<TTestEndpointListener>();
        listener->StartEndpointHandler = [&] (
            const NProto::TStartEndpointRequest& request)
        {
            endpoints.emplace(request.GetUnixSocketPath(), request);
            TFsPath(request.GetUnixSocketPath()).Touch();
            return MakeFuture(NProto::TError{});
        };

        auto executor = TExecutor::Create("TestService");
        executor->Start();

        auto endpointManager = CreateEndpointManager(
            CreateWallClockTimer(),
            CreateSchedulerStub(),
            CreateLoggingService("console"),
            CreateRequestStatsStub(),
            CreateVolumeStatsStub(),
            CreateServerStatsStub(),
            executor,
            CreateEndpointEventProxy(),
            std::make_shared<TTestSessionManager>(),
            endpointStorage,
            {{ ipcType, listener }},
            nullptr,    // nbdDeviceFactory
            {}          // options
        );
        endpointManager->RestoreEndpoints().Wait(5s);

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
                *endpointManager,
                keyringId,
                requestId);
            UNIT_ASSERT(!HasError(response));

            UNIT_ASSERT_VALUES_EQUAL(1, endpoints.size());
            const auto& endpoint = endpoints.begin()->second;

            google::protobuf::util::MessageDifferencer comparator;
            request.MutableHeaders()->SetRequestId(requestId);
            UNIT_ASSERT(comparator.Equals(endpoint, request));
        }

        {
            auto wrongKeyringId = keyringId + 42;
            auto response = KickEndpoint(*endpointManager, wrongKeyringId);
            UNIT_ASSERT(HasError(response)
                && response.GetError().GetCode() == E_INVALID_STATE);
        }

        executor->Stop();
    }

    Y_UNIT_TEST(ShouldTimeoutFrozenRequest)
    {
        const TString dirPath = "./" + CreateGuidAsString();
        auto endpointStorage = CreateFileEndpointStorage(dirPath);
        TTempDir endpointsDir(dirPath);
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
            CreateWallClockTimer(),
            scheduler,
            CreateLoggingService("console"),
            CreateRequestStatsStub(),
            CreateVolumeStatsStub(),
            CreateServerStatsStub(),
            executor,
            CreateEndpointEventProxy(),
            sessionManager,
            endpointStorage,
            {{ NProto::IPC_GRPC, std::make_shared<TTestEndpointListener>() }},
            nullptr,    // nbdDeviceFactory
            {}          // options
        );
        endpointManager->RestoreEndpoints().Wait(5s);

        auto endpointService = CreateMultipleEndpointService(
            std::make_shared<TTestService>(),
            CreateWallClockTimer(),
            scheduler,
            endpointManager);

        TTempDir dir;
        const TString socketPath = (dir.Path() / "testSocket").GetPath();
        const TString diskId = "testDiskId";

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
        const TString wrongSocketPath = "wrongSocket";
        size_t startedEndpointCount = 0;

        auto listener = std::make_shared<TTestEndpointListener>();
        listener->StartEndpointHandler = [&] (
            const NProto::TStartEndpointRequest& request)
        {
            if (request.GetUnixSocketPath().StartsWith(wrongSocketPath)) {
                return MakeFuture(MakeError(E_FAIL));
            }

            ++startedEndpointCount;
            TFsPath(request.GetUnixSocketPath()).Touch();
            return MakeFuture(NProto::TError{});
        };

        NMonitoring::TDynamicCountersPtr counters = new NMonitoring::TDynamicCounters();
        InitCriticalEventsCounter(counters);
        auto configCounter =
            counters->GetCounter("AppCriticalEvents/EndpointRestoringError", true);

        UNIT_ASSERT_VALUES_EQUAL(0, static_cast<int>(*configCounter));

        const TString dirPath = "./" + CreateGuidAsString();
        auto endpointStorage = CreateFileEndpointStorage(dirPath);
        TTempDir endpointsDir(dirPath);

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
            request.SetUnixSocketPath(wrongSocketPath + ToString(i));

            auto strOrError = SerializeEndpoint(request);
            UNIT_ASSERT_C(!HasError(strOrError), strOrError.GetError());

            auto error = endpointStorage->AddEndpoint(
                ToString(++counter),
                strOrError.GetResult());
            UNIT_ASSERT_C(!HasError(error), error);
        }

        TTempDir dir;

        for (size_t i = 0; i < correctCount; ++i) {
            NProto::TStartEndpointRequest request;
            const TString socket = "testSocket" + ToString(i + 1);
            request.SetUnixSocketPath((dir.Path() / socket).GetPath());

            auto strOrError = SerializeEndpoint(request);
            UNIT_ASSERT_C(!HasError(strOrError), strOrError.GetError());

            auto error = endpointStorage->AddEndpoint(
                ToString(++counter),
                strOrError.GetResult());
            UNIT_ASSERT_C(!HasError(error), error);
        }

        auto executor = TExecutor::Create("TestService");
        executor->Start();

        auto endpointManager = CreateEndpointManager(
            CreateWallClockTimer(),
            CreateSchedulerStub(),
            CreateLoggingService("console"),
            CreateRequestStatsStub(),
            CreateVolumeStatsStub(),
            CreateServerStatsStub(),
            executor,
            CreateEndpointEventProxy(),
            std::make_shared<TTestSessionManager>(),
            endpointStorage,
            {{ NProto::IPC_GRPC, listener }},
            nullptr,    // nbdDeviceFactory
            {}          // options
        );

        endpointManager->RestoreEndpoints().Wait();

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

        auto endpointManager = CreateEndpointManager(
            CreateWallClockTimer(),
            CreateSchedulerStub(),
            CreateLoggingService("console"),
            CreateRequestStatsStub(),
            CreateVolumeStatsStub(),
            CreateServerStatsStub(),
            executor,
            CreateEndpointEventProxy(),
            std::make_shared<TTestSessionManager>(),
            endpointStorage,
            {},         // listeners
            nullptr,    // nbdDeviceFactory
            {}          // options
        );

        endpointManager->RestoreEndpoints().Wait();

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

        auto endpointStorage = CreateKeyringEndpointStorage("nbs", "", false);

        auto executor = TExecutor::Create("TestService");
        executor->Start();

        auto endpointManager = CreateEndpointManager(
            CreateWallClockTimer(),
            CreateSchedulerStub(),
            CreateLoggingService("console"),
            CreateRequestStatsStub(),
            CreateVolumeStatsStub(),
            CreateServerStatsStub(),
            executor,
            CreateEndpointEventProxy(),
            std::make_shared<TTestSessionManager>(),
            endpointStorage,
            {},         // listeners
            nullptr,    // nbdDeviceFactory
            {}          // options
        );

        endpointManager->RestoreEndpoints().Wait();

        UNIT_ASSERT_VALUES_EQUAL(0, static_cast<int>(*configCounter));

        executor->Stop();
    }

    Y_UNIT_TEST(ShouldHandleRestoreFlagInListEndpointsResponse)
    {
        auto trigger = NewPromise<NProto::TError>();

        auto listener = std::make_shared<TTestEndpointListener>();
        listener->StartEndpointHandler = [&] (
            const NProto::TStartEndpointRequest& request)
        {
            TFsPath(request.GetUnixSocketPath()).Touch();
            return trigger;
        };

        const TString dirPath = "./" + CreateGuidAsString();
        auto endpointStorage = CreateFileEndpointStorage(dirPath);
        TTempDir endpointsDir(dirPath);

        TTempDir dir;
        size_t endpointCount = 5;

        for (size_t i = 0; i < endpointCount; ++i) {
            NProto::TStartEndpointRequest request;
            const TString socket = "testSocket" + ToString(i + 1);
            request.SetUnixSocketPath((dir.Path() / socket).GetPath());

            auto strOrError = SerializeEndpoint(request);
            UNIT_ASSERT_C(!HasError(strOrError), strOrError.GetError());

            auto error = endpointStorage->AddEndpoint(
                ToString(i),
                strOrError.GetResult());
            UNIT_ASSERT_C(!HasError(error), error);
        }

        auto executor = TExecutor::Create("TestService");
        executor->Start();

        auto endpointManager = CreateEndpointManager(
            CreateWallClockTimer(),
            CreateSchedulerStub(),
            CreateLoggingService("console"),
            CreateRequestStatsStub(),
            CreateVolumeStatsStub(),
            CreateServerStatsStub(),
            executor,
            CreateEndpointEventProxy(),
            std::make_shared<TTestSessionManager>(),
            endpointStorage,
            {{ NProto::IPC_GRPC, listener }},
            nullptr,    // nbdDeviceFactory
            {}          // options
        );

        auto restoreFuture = endpointManager->RestoreEndpoints();
        UNIT_ASSERT(!restoreFuture.HasValue());

        {
            auto future = endpointManager->ListEndpoints(
                MakeIntrusive<TCallContext>(),
                std::make_shared<NProto::TListEndpointsRequest>());
            auto response = future.GetValue(TDuration::Seconds(1));
            UNIT_ASSERT(!response.GetEndpointsWereRestored());
        }

        trigger.SetValue(NProto::TError());
        restoreFuture.Wait();

        {
            auto future = endpointManager->ListEndpoints(
                MakeIntrusive<TCallContext>(),
                std::make_shared<NProto::TListEndpointsRequest>());
            auto response = future.GetValue(TDuration::Seconds(1));
            UNIT_ASSERT(response.GetEndpointsWereRestored());
        }

        executor->Stop();
    }

    Y_UNIT_TEST(ShouldListKeyrings)
    {
        const TString dirPath = "./" + CreateGuidAsString();
        auto endpointStorage = CreateFileEndpointStorage(dirPath);
        TTempDir endpointsDir(dirPath);

        TTempDir dir;
        ui32 endpointCount = 42;
        THashMap<TString, NProto::TStartEndpointRequest> requests;

        for (size_t i = 0; i < endpointCount; ++i) {
            NProto::TStartEndpointRequest request;
            const TString socket = "testSocket" + ToString(i + 1);
            request.SetUnixSocketPath((dir.Path() / socket).GetPath());
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

        auto endpointManager = CreateEndpointManager(
            CreateWallClockTimer(),
            CreateSchedulerStub(),
            CreateLoggingService("console"),
            CreateRequestStatsStub(),
            CreateVolumeStatsStub(),
            CreateServerStatsStub(),
            executor,
            CreateEndpointEventProxy(),
            std::make_shared<TTestSessionManager>(),
            endpointStorage,
            {{ NProto::IPC_GRPC, std::make_shared<TTestEndpointListener>() }},
            nullptr,    // nbdDeviceFactory
            {}          // options
        );
        endpointManager->RestoreEndpoints().Wait(5s);

        auto future = endpointManager->ListKeyrings(
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
            const TString socket = "testPersistentSocket" + ToString(i + 1);
            request->SetUnixSocketPath((dir.Path() / socket).GetPath());
            request->SetDiskId("testPersistentDiskId" + ToString(i + 1));
            request->SetIpcType(NProto::IPC_GRPC);
            request->SetPersistent(true);
            ++endpointCount;

            auto future = endpointManager->StartEndpoint(
                MakeIntrusive<TCallContext>(),
                request);
            auto response = future.GetValue(TDuration::Seconds(5));
            UNIT_ASSERT(!HasError(response));
        }

        {
            auto future = endpointManager->ListKeyrings(
                MakeIntrusive<TCallContext>(),
                std::make_shared<NProto::TListKeyringsRequest>());

            auto response = future.GetValue(TDuration::Seconds(5));
            UNIT_ASSERT_C(!HasError(response), response);
            UNIT_ASSERT_VALUES_EQUAL(endpointCount, response.GetEndpoints().size());
        }

        for (size_t i = 0; i < 5; ++i) {
            auto request = std::make_shared<NProto::TStartEndpointRequest>();
            const TString socket = "testTemporarySocket" + ToString(i + 1);
            request->SetUnixSocketPath((dir.Path() / socket).GetPath());
            request->SetDiskId("testTemporaryDiskId" + ToString(i + 1));
            request->SetIpcType(NProto::IPC_GRPC);
            request->SetPersistent(false);

            auto future = endpointManager->StartEndpoint(
                MakeIntrusive<TCallContext>(),
                request);
            auto response = future.GetValue(TDuration::Seconds(5));
            UNIT_ASSERT(!HasError(response));
        }

        {
            auto future = endpointManager->ListKeyrings(
                MakeIntrusive<TCallContext>(),
                std::make_shared<NProto::TListKeyringsRequest>());

            auto response = future.GetValue(TDuration::Seconds(5));
            UNIT_ASSERT_C(!HasError(response), response);
            UNIT_ASSERT_VALUES_EQUAL(endpointCount, response.GetEndpoints().size());
        }

        for (size_t i = 0; i < 3; ++i) {
            auto request = std::make_shared<NProto::TStopEndpointRequest>();
            const TString socket = "testPersistentSocket" + ToString(i + 1);
            request->SetUnixSocketPath((dir.Path() / socket).GetPath());
            --endpointCount;

            auto future = endpointManager->StopEndpoint(
                MakeIntrusive<TCallContext>(),
                request);
            auto response = future.GetValue(TDuration::Seconds(5));
            UNIT_ASSERT(!HasError(response));
        }

        {
            auto future = endpointManager->ListKeyrings(
                MakeIntrusive<TCallContext>(),
                std::make_shared<NProto::TListKeyringsRequest>());

            auto response = future.GetValue(TDuration::Seconds(5));
            UNIT_ASSERT_C(!HasError(response), response);
            UNIT_ASSERT_VALUES_EQUAL(endpointCount, response.GetEndpoints().size());
        }

        for (size_t i = 0; i < 3; ++i) {
            auto request = std::make_shared<NProto::TStopEndpointRequest>();
            const TString socket = "testTemporarySocket" + ToString(i + 1);
            request->SetUnixSocketPath((dir.Path() / socket).GetPath());

            auto future = endpointManager->StopEndpoint(
                MakeIntrusive<TCallContext>(),
                request);
            auto response = future.GetValue(TDuration::Seconds(5));
            UNIT_ASSERT(!HasError(response));
        }

        {
            auto future = endpointManager->ListKeyrings(
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
        auto startPromise = NewPromise<NProto::TError>();
        auto stopPromise = NewPromise<NProto::TError>();

        auto listener = std::make_shared<TTestEndpointListener>();
        listener->StartEndpointHandler = [&] (
            const NProto::TStartEndpointRequest& request)
        {
            Y_UNUSED(request);
            return startPromise.GetFuture();
        };
        listener->StopEndpointHandler = [&] (const TString& socketPath)
        {
            Y_UNUSED(socketPath);
            return stopPromise.GetFuture();
        };

        const TString dirPath = "./" + CreateGuidAsString();
        auto endpointStorage = CreateFileEndpointStorage(dirPath);

        auto logging = CreateLoggingService("console");

        auto executor = TExecutor::Create("TestService");
        executor->Start();

        auto endpointManager = CreateEndpointManager(
            CreateWallClockTimer(),
            CreateSchedulerStub(),
            CreateLoggingService("console"),
            CreateRequestStatsStub(),
            CreateVolumeStatsStub(),
            CreateServerStatsStub(),
            executor,
            CreateEndpointEventProxy(),
            std::make_shared<TTestSessionManager>(),
            endpointStorage,
            {{ NProto::IPC_GRPC, listener }},
            nullptr,    // nbdDeviceFactory
            {}          // options
        );
        endpointManager->RestoreEndpoints().Wait(5s);

        TTempDir dir;
        TString unixSocket = (dir.Path() / "testSocket").GetPath();

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
            auto future1 = endpointManager->StartEndpoint(ctx, startRequest);
            auto future2 = endpointManager->StartEndpoint(ctx, startRequest);
            UNIT_ASSERT(!future1.HasValue());
            UNIT_ASSERT(!future2.HasValue());

            auto future3 = endpointManager->StartEndpoint(ctx, otherStartRequest);
            auto response3 = future3.GetValue(TDuration::Seconds(5));
            UNIT_ASSERT_C(response3.GetError().GetCode() == E_REJECTED, response3.GetError());

            auto future = endpointManager->StopEndpoint(ctx, stopRequest);
            auto response = future.GetValue(TDuration::Seconds(5));
            UNIT_ASSERT(response.GetError().GetCode() == E_REJECTED);

            startPromise.SetValue(NProto::TError{});

            auto response1 = future1.GetValue(TDuration::Seconds(5));
            auto response2 = future2.GetValue(TDuration::Seconds(5));
            UNIT_ASSERT(response1.GetError().GetCode() == S_OK);
            UNIT_ASSERT(response2.GetError().GetCode() == S_OK);
        }

        {
            auto future1 = endpointManager->StopEndpoint(ctx, stopRequest);
            auto future2 = endpointManager->StopEndpoint(ctx, stopRequest);
            UNIT_ASSERT(!future1.HasValue());
            UNIT_ASSERT(!future2.HasValue());

            auto future3 = endpointManager->StartEndpoint(ctx, otherStartRequest);
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
        TTempDir endpointsDir(dirPath);

        TAtomic trigger = 0;
        TManualEvent event;

        auto listener = std::make_shared<TTestEndpointListener>();
        listener->StartEndpointHandler = [&] (
            const NProto::TStartEndpointRequest& request)
        {
            Y_UNUSED(request);

            NProto::TError error;
            if (!AtomicGet(trigger)) {
                error.SetCode(E_TIMEOUT);
            }
            event.Signal();
            return MakeFuture(error);
        };

        auto scheduler = CreateScheduler();
        scheduler->Start();
        Y_DEFER {
            scheduler->Stop();
        };

        auto executor = TExecutor::Create("TestService");
        executor->Start();

        auto endpointManager = CreateEndpointManager(
            CreateWallClockTimer(),
            scheduler,
            CreateLoggingService("console"),
            CreateRequestStatsStub(),
            CreateVolumeStatsStub(),
            CreateServerStatsStub(),
            executor,
            CreateEndpointEventProxy(),
            std::make_shared<TTestSessionManager>(),
            endpointStorage,
            {{ NProto::IPC_GRPC, listener }},
            nullptr,    // nbdDeviceFactory
            {}          // options
        );

        endpointManager->Start();
        Y_DEFER {
            endpointManager->Stop();
        };

        TTempDir dir;
        TString unixSocket = (dir.Path() / "testSocket").GetPath();

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

        auto restoreFuture = endpointManager->RestoreEndpoints();

        auto stopFuture = endpointManager->StopEndpoint(ctx, stopRequest);
        auto stopResponse = stopFuture.GetValue(TDuration::Seconds(5));
        UNIT_ASSERT(stopResponse.GetError().GetCode() == E_REJECTED);

        event.Wait();
        UNIT_ASSERT(!restoreFuture.HasValue());

        AtomicSet(trigger, 1);
        UNIT_ASSERT(restoreFuture.Wait(TDuration::Seconds(5)));
        UNIT_ASSERT(restoreFuture.HasValue());

        auto future = endpointManager->StopEndpoint(ctx, stopRequest);
        auto response = future.GetValue(TDuration::Seconds(5));
        UNIT_ASSERT(response.GetError().GetCode() == S_OK);

        executor->Stop();
    }
}

}   // namespace NCloud::NBlockStore::NServer
