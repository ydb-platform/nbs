#include "endpoint_manager.h"
#include "listener.h"

#include <cloud/filestore/libs/service/context.h>
#include <cloud/filestore/libs/service/endpoint.h>
#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/scheduler_test.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>
#include <cloud/storage/core/libs/endpoints/fs/fs_endpoints.h>

#include <library/cpp/testing/unittest/registar.h>

#include <google/protobuf/util/message_differencer.h>

#include <util/generic/guid.h>
#include <util/generic/scope.h>
#include <util/system/sysstat.h>
#include <util/folder/tempdir.h>

#include <atomic>
#include <thread>

namespace NCloud::NFileStore::NServer {

using namespace NThreading;

namespace {
////////////////////////////////////////////////////////////////////////////////

constexpr TDuration WaitTimeout = TDuration::Seconds(5);

static constexpr int MODE0660 = S_IRGRP | S_IWGRP | S_IRUSR | S_IWUSR;

////////////////////////////////////////////////////////////////////////////////

struct TTestEndpoint:
    public IEndpoint
{
    using TAlterHandler = std::function<
        void(const TTestEndpoint* endpoint, bool isReadonly, ui64 mountSeqNumber)
        >;

    NProto::TEndpointConfig Config;
    TAlterHandler AlterHandler;

    TPromise<NProto::TError> Start = NewPromise<NProto::TError>();
    TPromise<void> Stop = NewPromise();
    std::atomic<ui32> StopCalls = 0;
    std::atomic<ui32> SuspendCalls = 0;

    std::atomic<bool> Ready = true;

    TTestEndpoint(const NProto::TEndpointConfig& config, bool ready = true)
        : Config(config)
        , Ready(ready)
    {}

    ~TTestEndpoint()
    {
        if (!Start.HasValue()) {
            Start.SetValue({});
        }

        if (!Stop.HasValue()) {
            Stop.SetValue();
        }
    }

    TFuture<NProto::TError> StartAsync() override
    {
        if (!Ready.load()) {
            return Start;
        }

        return MakeFuture<NProto::TError>();
    }

    TFuture<void> StopAsync() override
    {
        ++StopCalls;

        if (!Ready.load()) {
            return Stop;
        }

        return MakeFuture();
    }

    TFuture<void> SuspendAsync() override
    {
        ++SuspendCalls;

        if (!Ready.load()) {
            return Stop;
        }

        return MakeFuture();
    }

    TFuture<NProto::TError> AlterAsync(
        bool isReadonly,
        ui64 mountSeqNumber) override
    {
        if (AlterHandler) {
            AlterHandler(this, isReadonly, mountSeqNumber);
        }
        return MakeFuture<NProto::TError>();
    }
};

using TTestEndpointPtr = std::shared_ptr<TTestEndpoint>;

////////////////////////////////////////////////////////////////////////////////

struct TTestEndpointListener final
    : public IEndpointListener
{
    using TCreateEndpointHandler = std::function<
        IEndpointPtr(const NProto::TEndpointConfig& config)
        >;

    TCreateEndpointHandler CreateEndpointHandler;
    TVector<TTestEndpointPtr> Endpoints;

    IEndpointPtr CreateEndpoint(
        const NProto::TEndpointConfig& config) override
    {
        if (CreateEndpointHandler) {
            return CreateEndpointHandler(config);
        }

        Endpoints.emplace_back(std::make_shared<TTestEndpoint>(config));
        return Endpoints.back();
    }
};

////////////////////////////////////////////////////////////////////////////////

NProto::TKickEndpointResponse KickEndpoint(IEndpointManager& service, ui32 keyringId)
{
    auto request = std::make_shared<NProto::TKickEndpointRequest>();
    request->SetKeyringId(keyringId);

    auto future = service.KickEndpoint(
        MakeIntrusive<TCallContext>(),
        std::move(request));

     return future.GetValue(WaitTimeout);
}

NProto::TStartEndpointResponse StartEndpoint(
    IEndpointManager& service,
    const NProto::TEndpointConfig& config)
{
    auto request = std::make_shared<NProto::TStartEndpointRequest>();
    *request->MutableEndpoint() = config;

    auto future = service.StartEndpoint(
        MakeIntrusive<TCallContext>(),
        std::move(request));

    return future.GetValue(WaitTimeout);
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TServiceEndpointTest)
{
    Y_UNIT_TEST(ShouldHandleKickEndpoint)
    {
        auto keyringId = 13;
        TString id = "id";
        TString unixSocket = "testSocket";

        NProto::TStartEndpointRequest start;
        auto config = start.MutableEndpoint();
        config->SetFileSystemId(id);
        config->SetSocketPath(unixSocket);
        config->SetClientId("client");

        const TString dirPath = "./" + CreateGuidAsString();
        auto endpointStorage = CreateFileEndpointStorage(dirPath);
        TTempDir endpointDir(dirPath);
        auto endpoint = std::make_shared<TTestEndpoint>(*config, false);
        auto listener = std::make_shared<TTestEndpointListener>();
        listener->CreateEndpointHandler =
            [&] (const NProto::TEndpointConfig&) {
                listener->Endpoints.push_back(endpoint);
                return endpoint;
            };

        auto service = CreateEndpointManager(
            CreateLoggingService("console"),
            endpointStorage,
            listener,
            MODE0660);
        service->Start();

        auto strOrError = SerializeEndpoint(start);
        UNIT_ASSERT_C(!HasError(strOrError), strOrError.GetError());

        auto error = endpointStorage->AddEndpoint(
            ToString(keyringId),
            strOrError.GetResult());
        UNIT_ASSERT_C(!HasError(error), error);

        auto request = std::make_shared<NProto::TKickEndpointRequest>();
        request->SetKeyringId(keyringId);

        auto future = service->KickEndpoint(
            MakeIntrusive<TCallContext>(),
            request);

        UNIT_ASSERT(!future.HasValue());
        endpoint->Start.SetValue(NProto::TError{});
        UNIT_ASSERT(future.Wait(WaitTimeout));

        auto response = future.GetValue();
        UNIT_ASSERT_C(!HasError(response), response.ShortDebugString());

        UNIT_ASSERT_VALUES_EQUAL(listener->Endpoints.size(), 1);
        const auto& created = *listener->Endpoints[0];

        google::protobuf::util::MessageDifferencer comparator;
        UNIT_ASSERT(comparator.Equals(created.Config, *config));

        // kick unexisting
        auto wrongKeyringId = keyringId + 42;
        response = KickEndpoint(*service, wrongKeyringId);

        UNIT_ASSERT(HasError(response));
        UNIT_ASSERT_VALUES_EQUAL(response.GetError().GetCode(), E_INVALID_STATE);
        UNIT_ASSERT_VALUES_EQUAL(listener->Endpoints.size(), 1);

        endpoint->Stop.SetValue();

        UNIT_ASSERT_NO_EXCEPTION(service->Drain());
        UNIT_ASSERT_NO_EXCEPTION(service->Stop());
    }

    Y_UNIT_TEST(ShouldAllowToChangeReadonlyAndMountSeqNumber)
    {
        auto keyringId = 13;
        TString id = "id";
        TString unixSocket = "testSocket";

        NProto::TStartEndpointRequest start;
        auto config = start.MutableEndpoint();
        config->SetFileSystemId(id);
        config->SetSocketPath(unixSocket);
        config->SetClientId("client");

        const TString dirPath = "./" + CreateGuidAsString();
        auto endpointStorage = CreateFileEndpointStorage(dirPath);
        TTempDir endpointDir(dirPath);
        auto endpoint = std::make_shared<TTestEndpoint>(*config, false);
        auto listener = std::make_shared<TTestEndpointListener>();
        listener->CreateEndpointHandler =
            [&] (const NProto::TEndpointConfig&) {
                listener->Endpoints.push_back(endpoint);
                return endpoint;
            };

        auto service = CreateEndpointManager(
            CreateLoggingService("console"),
            endpointStorage,
            listener,
            MODE0660);
        service->Start();

        auto strOrError = SerializeEndpoint(start);
        UNIT_ASSERT_C(!HasError(strOrError), strOrError.GetError());

        auto error = endpointStorage->AddEndpoint(
            ToString(keyringId),
            strOrError.GetResult());
        UNIT_ASSERT_C(!HasError(error), error);

        auto request = std::make_shared<NProto::TKickEndpointRequest>();
        request->SetKeyringId(keyringId);

        auto future = service->KickEndpoint(
            MakeIntrusive<TCallContext>(),
            request);

        UNIT_ASSERT(!future.HasValue());
        endpoint->Start.SetValue(NProto::TError{});
        UNIT_ASSERT(future.Wait(WaitTimeout));

        auto response = future.GetValue();
        UNIT_ASSERT_C(!HasError(response), response.ShortDebugString());

        UNIT_ASSERT_VALUES_EQUAL(listener->Endpoints.size(), 1);
        const auto& created = *listener->Endpoints[0];

        google::protobuf::util::MessageDifferencer comparator;
        UNIT_ASSERT(comparator.Equals(created.Config, *config));

        // change endpoint settings

        endpointStorage->RemoveEndpoint(id);

        {
            NProto::TStartEndpointRequest start;
            auto config = start.MutableEndpoint();
            config->SetFileSystemId(id);
            config->SetSocketPath(unixSocket);
            config->SetClientId("client");
            config->SetReadOnly(true);
            config->SetMountSeqNumber(1);

            const TTestEndpoint* endpointInfo = nullptr;

            ui32 alterCalls = 0;
            endpoint->AlterHandler = [&] (const TTestEndpoint* ep, bool, ui64) {
                endpointInfo = ep;
                ++alterCalls;
            };

            auto strOrError = SerializeEndpoint(start);
            UNIT_ASSERT_C(!HasError(strOrError), strOrError.GetError());

            auto keyringId = 57;
            auto error = endpointStorage->AddEndpoint(
                ToString(keyringId),
                strOrError.GetResult());
            UNIT_ASSERT_C(!HasError(error), error);

            auto request = std::make_shared<NProto::TKickEndpointRequest>();
            request->SetKeyringId(keyringId);

            auto future = service->KickEndpoint(
                MakeIntrusive<TCallContext>(),
                request);

            UNIT_ASSERT(future.Wait(WaitTimeout));

            auto response = future.GetValue();
            UNIT_ASSERT_C(!HasError(response), response.ShortDebugString());
            UNIT_ASSERT_VALUES_UNEQUAL(response.GetError().GetCode(), S_ALREADY);
            UNIT_ASSERT_VALUES_EQUAL(alterCalls, 1);
            UNIT_ASSERT_VALUES_UNEQUAL(endpointInfo, nullptr);
            UNIT_ASSERT_VALUES_EQUAL(listener->Endpoints.size(), 1);
        }

        endpoint->Stop.SetValue();

        UNIT_ASSERT_NO_EXCEPTION(service->Drain());
        UNIT_ASSERT_NO_EXCEPTION(service->Stop());
    }

    Y_UNIT_TEST(ShouldStartPersistentEndpoint)
    {
        TString id = "id";
        TString unixSocket = "testSocket";

        auto request = std::make_shared<NProto::TStartEndpointRequest>();
        auto config = request->MutableEndpoint();
        config->SetFileSystemId(id);
        config->SetSocketPath(unixSocket);
        config->SetClientId("client");
        config->SetPersistent(true);

        const TString dirPath = "./" + CreateGuidAsString();
        auto endpointStorage = CreateFileEndpointStorage(dirPath);
        TTempDir endpointDir(dirPath);
        auto endpoint = std::make_shared<TTestEndpoint>(*config, false);
        endpoint->Start.SetValue(NProto::TError{});

        auto listener = std::make_shared<TTestEndpointListener>();
        listener->CreateEndpointHandler =
            [&] (const NProto::TEndpointConfig&) {
                listener->Endpoints.push_back(endpoint);
                return endpoint;
            };

        auto service = CreateEndpointManager(
            CreateLoggingService("console"),
            endpointStorage,
            listener,
            MODE0660);
        service->Start();

        Y_DEFER {
            endpoint->Stop.SetValue();
            UNIT_ASSERT_NO_EXCEPTION(service->Drain());
            UNIT_ASSERT_NO_EXCEPTION(service->Stop());
        };

        auto idsOrError = endpointStorage->GetEndpointIds();
        UNIT_ASSERT_C(!HasError(idsOrError), idsOrError.GetError());
        UNIT_ASSERT_VALUES_EQUAL(idsOrError.GetResult().size(), 0);

        auto future = service->StartEndpoint(
            MakeIntrusive<TCallContext>(),
            request);
        auto response = future.GetValue(WaitTimeout);
        UNIT_ASSERT_C(!HasError(response), response.ShortDebugString());

        UNIT_ASSERT_VALUES_EQUAL(listener->Endpoints.size(), 1);

        idsOrError = endpointStorage->GetEndpointIds();
        UNIT_ASSERT_C(!HasError(idsOrError), idsOrError.GetError());
        UNIT_ASSERT_VALUES_EQUAL(idsOrError.GetResult().size(), 1);
    }

    Y_UNIT_TEST(ShouldRestoreEndpoint)
    {
        TString id = "id";
        TString unixSocket = "testSocket";

        NProto::TStartEndpointRequest request;
        auto* config = request.MutableEndpoint();
        config->SetFileSystemId(id);
        config->SetSocketPath(unixSocket);
        config->SetClientId("client");
        config->SetPersistent(true);

        auto [endpointData, error] = SerializeEndpoint(request);
        UNIT_ASSERT_C(!HasError(error), error);

        const TString dirPath = "./" + CreateGuidAsString();
        TTempDir endpointDir(dirPath);
        auto endpointStorage = CreateFileEndpointStorage(dirPath);
        auto endpoint = std::make_shared<TTestEndpoint>(*config, false);
        endpoint->Start.SetValue(NProto::TError{});
        endpointStorage->AddEndpoint(unixSocket, endpointData);

        auto listener = std::make_shared<TTestEndpointListener>();

        listener->CreateEndpointHandler =
            [&] (const NProto::TEndpointConfig&) {
                listener->Endpoints.push_back(endpoint);
                return endpoint;
            };

        auto service = CreateEndpointManager(
            CreateLoggingService("console"),
            endpointStorage,
            listener,
            MODE0660);
        service->Start();

        service->RestoreEndpoints().GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(1, listener->Endpoints.size());
    }

    Y_UNIT_TEST(ShouldRemoveEndpointForNotFoundFilesystem)
    {
        TString id = "id";
        TString unixSocket = "testSocket";

        NProto::TStartEndpointRequest request;
        auto* config = request.MutableEndpoint();
        config->SetFileSystemId(id);
        config->SetSocketPath(unixSocket);
        config->SetClientId("client");
        config->SetPersistent(true);

        auto [endpointData, error] = SerializeEndpoint(request);
        UNIT_ASSERT_C(!HasError(error), error);

        const TString dirPath = "./" + CreateGuidAsString();
        TTempDir endpointDir(dirPath);
        auto endpointStorage = CreateFileEndpointStorage(dirPath);
        auto endpoint = std::make_shared<TTestEndpoint>(*config, false);
        NProto::TError startError;
        startError.SetCode(MAKE_SCHEMESHARD_ERROR(ENOENT));
        endpoint->Start.SetValue(startError);
        endpointStorage->AddEndpoint(unixSocket, endpointData);

        auto listener = std::make_shared<TTestEndpointListener>();

        listener->CreateEndpointHandler =
            [&] (const NProto::TEndpointConfig&) {
                return endpoint;
            };

        auto service = CreateEndpointManager(
            CreateLoggingService("console"),
            endpointStorage,
            listener,
            MODE0660);
        service->Start();

        service->RestoreEndpoints().GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(0, listener->Endpoints.size());
        auto ret = endpointStorage->GetEndpointIds();
        UNIT_ASSERT_C(!HasError(ret.GetError()), error);
        UNIT_ASSERT_VALUES_EQUAL(0, ret.GetResult().size());
    }

    Y_UNIT_TEST(ShouldRestoreEndpointWhenSocketDirectoryIsMissing)
    {
        TString id = "id";
        TTempDir socketDir("./" + CreateGuidAsString());
        auto unixSocketPath = socketDir.Path() / TFsPath("socket1/disk1/fs1.sock");
        TString unixSocket = unixSocketPath.GetPath();

        UNIT_ASSERT(!unixSocketPath.Parent().Exists());

        NProto::TStartEndpointRequest request;
        auto* config = request.MutableEndpoint();
        config->SetFileSystemId(id);
        config->SetSocketPath(unixSocket);
        config->SetClientId("client");
        config->SetPersistent(true);

        auto [endpointData, error] = SerializeEndpoint(request);
        UNIT_ASSERT_C(!HasError(error), error);

        const TString dirPath = "./" + CreateGuidAsString();
        TTempDir endpointDir(dirPath);
        auto endpointStorage = CreateFileEndpointStorage(dirPath);
        auto endpoint = std::make_shared<TTestEndpoint>(*config, false);
        endpoint->Start.SetValue(NProto::TError{});
        endpointStorage->AddEndpoint(unixSocket, endpointData);

        auto listener = std::make_shared<TTestEndpointListener>();

        listener->CreateEndpointHandler = [&](const NProto::TEndpointConfig&)
        {
            listener->Endpoints.push_back(endpoint);
            return endpoint;
        };

        auto service = CreateEndpointManager(
            CreateLoggingService("console"),
            endpointStorage,
            listener,
            MODE0660);
        service->Start();

        service->RestoreEndpoints().GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(1, listener->Endpoints.size());
        UNIT_ASSERT(unixSocketPath.Parent().Exists());
    }

    Y_UNIT_TEST(ShouldWaitStopAndSkipSuspendWhileDraining)
    {
        TString id = "id";
        TString unixSocket = "testSocket";

        NProto::TEndpointConfig config;
        config.SetFileSystemId(id);
        config.SetSocketPath(unixSocket);
        config.SetClientId("client");

        const TString dirPath = "./" + CreateGuidAsString();
        auto endpointStorage = CreateFileEndpointStorage(dirPath);
        TTempDir endpointDir(dirPath);

        auto endpoint = std::make_shared<TTestEndpoint>(config, false);
        endpoint->Start.SetValue(NProto::TError{});

        auto listener = std::make_shared<TTestEndpointListener>();
        listener->CreateEndpointHandler = [&](const NProto::TEndpointConfig&)
        {
            listener->Endpoints.push_back(endpoint);
            return endpoint;
        };

        auto service = CreateEndpointManager(
            CreateLoggingService("console"),
            endpointStorage,
            listener,
            MODE0660);
        service->Start();

        Y_DEFER
        {
            if (!endpoint->Stop.HasValue()) {
                endpoint->Stop.SetValue();
            }
            UNIT_ASSERT_NO_EXCEPTION(service->Stop());
        };

        auto startResponse = StartEndpoint(*service, config);
        UNIT_ASSERT_C(
            !HasError(startResponse),
            startResponse.ShortDebugString());
        UNIT_ASSERT_VALUES_EQUAL(1, listener->Endpoints.size());

        auto stopRequest = std::make_shared<NProto::TStopEndpointRequest>();
        stopRequest->SetSocketPath(unixSocket);
        auto stopFuture = service->StopEndpoint(
            MakeIntrusive<TCallContext>(),
            std::move(stopRequest));
        UNIT_ASSERT(!stopFuture.HasValue());

        auto drainDone = NewPromise<void>();
        std::thread drainThread(
            [&]
            {
                service->Drain();
                drainDone.SetValue();
            });

        UNIT_ASSERT(!drainDone.GetFuture().Wait(TDuration::MilliSeconds(100)));
        UNIT_ASSERT_VALUES_EQUAL(0, endpoint->SuspendCalls.load());

        endpoint->Stop.SetValue();

        UNIT_ASSERT(stopFuture.Wait(WaitTimeout));
        auto stopResponse = stopFuture.GetValue();
        UNIT_ASSERT_C(!HasError(stopResponse), stopResponse.ShortDebugString());
        UNIT_ASSERT(drainDone.GetFuture().Wait(WaitTimeout));

        drainThread.join();

        UNIT_ASSERT_VALUES_EQUAL(0, endpoint->SuspendCalls.load());
    }

    Y_UNIT_TEST(ShouldSuspendEndpointIfStartCompletesDuringDrain)
    {
        TString id = "id";
        TString unixSocket = "testSocket";

        NProto::TEndpointConfig config;
        config.SetFileSystemId(id);
        config.SetSocketPath(unixSocket);
        config.SetClientId("client");

        const TString dirPath = "./" + CreateGuidAsString();
        auto endpointStorage = CreateFileEndpointStorage(dirPath);
        TTempDir endpointDir(dirPath);

        auto endpoint = std::make_shared<TTestEndpoint>(config, false);

        auto listener = std::make_shared<TTestEndpointListener>();
        listener->CreateEndpointHandler = [&](const NProto::TEndpointConfig&)
        {
            listener->Endpoints.push_back(endpoint);
            return endpoint;
        };

        auto service = CreateEndpointManager(
            CreateLoggingService("console"),
            endpointStorage,
            listener,
            MODE0660);
        service->Start();

        Y_DEFER
        {
            if (!endpoint->Start.HasValue()) {
                endpoint->Start.SetValue({});
            }
            if (!endpoint->Stop.HasValue()) {
                endpoint->Stop.SetValue();
            }
            UNIT_ASSERT_NO_EXCEPTION(service->Stop());
        };

        auto startRequest = std::make_shared<NProto::TStartEndpointRequest>();
        *startRequest->MutableEndpoint() = config;
        auto startFuture = service->StartEndpoint(
            MakeIntrusive<TCallContext>(),
            std::move(startRequest));
        UNIT_ASSERT(!startFuture.HasValue());

        auto drainDone = NewPromise<void>();
        std::thread drainThread(
            [&]
            {
                service->Drain();
                drainDone.SetValue();
            });

        UNIT_ASSERT(!drainDone.GetFuture().Wait(TDuration::MilliSeconds(100)));
        UNIT_ASSERT_VALUES_EQUAL(0, endpoint->SuspendCalls.load());

        endpoint->Ready.store(true);
        endpoint->Start.SetValue({});

        UNIT_ASSERT(startFuture.Wait(WaitTimeout));
        auto startResponse = startFuture.GetValue();
        UNIT_ASSERT_C(
            !HasError(startResponse),
            startResponse.ShortDebugString());

        UNIT_ASSERT(drainDone.GetFuture().Wait(WaitTimeout));
        drainThread.join();

        UNIT_ASSERT_VALUES_EQUAL(1, endpoint->SuspendCalls.load());
    }

    Y_UNIT_TEST(ShouldNotSuspendEndpointIfStartFailsDuringDrain)
    {
        TString id = "id";
        TString unixSocket = "testSocket";

        NProto::TEndpointConfig config;
        config.SetFileSystemId(id);
        config.SetSocketPath(unixSocket);
        config.SetClientId("client");

        const TString dirPath = "./" + CreateGuidAsString();
        auto endpointStorage = CreateFileEndpointStorage(dirPath);
        TTempDir endpointDir(dirPath);

        auto endpoint = std::make_shared<TTestEndpoint>(config, false);

        auto listener = std::make_shared<TTestEndpointListener>();
        listener->CreateEndpointHandler = [&](const NProto::TEndpointConfig&)
        {
            listener->Endpoints.push_back(endpoint);
            return endpoint;
        };

        auto service = CreateEndpointManager(
            CreateLoggingService("console"),
            endpointStorage,
            listener,
            MODE0660);
        service->Start();

        Y_DEFER
        {
            if (!endpoint->Start.HasValue()) {
                NProto::TError error;
                error.SetCode(E_FAIL);
                error.SetMessage("start failed");
                endpoint->Start.SetValue(error);
            }
            if (!endpoint->Stop.HasValue()) {
                endpoint->Stop.SetValue();
            }
            UNIT_ASSERT_NO_EXCEPTION(service->Stop());
        };

        auto startRequest = std::make_shared<NProto::TStartEndpointRequest>();
        *startRequest->MutableEndpoint() = config;
        auto startFuture = service->StartEndpoint(
            MakeIntrusive<TCallContext>(),
            std::move(startRequest));
        UNIT_ASSERT(!startFuture.HasValue());

        auto drainDone = NewPromise<void>();
        std::thread drainThread(
            [&]
            {
                service->Drain();
                drainDone.SetValue();
            });

        UNIT_ASSERT(!drainDone.GetFuture().Wait(TDuration::MilliSeconds(100)));
        UNIT_ASSERT_VALUES_EQUAL(0, endpoint->SuspendCalls.load());

        NProto::TError error;
        error.SetCode(E_FAIL);
        error.SetMessage("start failed");
        endpoint->Start.SetValue(error);

        UNIT_ASSERT(startFuture.Wait(WaitTimeout));
        auto startResponse = startFuture.GetValue();
        UNIT_ASSERT(HasError(startResponse));

        UNIT_ASSERT(drainDone.GetFuture().Wait(WaitTimeout));
        drainThread.join();

        UNIT_ASSERT_VALUES_EQUAL(0, endpoint->SuspendCalls.load());
    }
}

}   // namespace NCloud::NFileStore::NServer
