#include "endpoint_manager.h"

#include "endpoint_events.h"
#include "endpoint_listener.h"
#include "session_manager.h"

#include <cloud/blockstore/config/server.pb.h>

#include <cloud/blockstore/libs/cells/iface/cell_manager.h>
#include <cloud/blockstore/libs/client/config.h>
#include <cloud/blockstore/libs/client/session.h>
#include <cloud/blockstore/libs/common/iovector.h>
#include <cloud/blockstore/libs/diagnostics/critical_events.h>
#include <cloud/blockstore/libs/diagnostics/request_stats.h>
#include <cloud/blockstore/libs/diagnostics/server_stats_test.h>
#include <cloud/blockstore/libs/diagnostics/volume_stats.h>
#include <cloud/blockstore/libs/encryption/encryption_client.h>
#include <cloud/blockstore/libs/encryption/encryption_key.h>
#include <cloud/blockstore/libs/endpoints_grpc/socket_endpoint_listener.h>
#include <cloud/blockstore/libs/nbd/device.h>
#include <cloud/blockstore/libs/server/client_storage_factory.h>
#include <cloud/blockstore/libs/service/device_handler.h>
#include <cloud/blockstore/libs/service/service_test.h>
#include <cloud/blockstore/libs/service/storage_provider.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/scheduler.h>
#include <cloud/storage/core/libs/common/scheduler_test.h>
#include <cloud/storage/core/libs/common/sglist_test.h>
#include <cloud/storage/core/libs/common/timer.h>
#include <cloud/storage/core/libs/coroutine/executor.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>
#include <cloud/storage/core/libs/diagnostics/monitoring.h>
#include <cloud/storage/core/libs/endpoints/fs/fs_endpoints.h>

#include <library/cpp/testing/gmock_in_unittest/gmock.h>
#include <library/cpp/testing/unittest/registar.h>

#include <google/protobuf/util/message_differencer.h>

#include <util/generic/guid.h>
#include <util/folder/path.h>
#include <util/folder/tempdir.h>
#include <util/generic/scope.h>

namespace NCloud::NBlockStore::NServer {

using namespace NCells;
using namespace NThreading;

using namespace std::chrono_literals;

namespace {

////////////////////////////////////////////////////////////////////////////////

static constexpr int MODE0660 = S_IRGRP | S_IWGRP | S_IRUSR | S_IWUSR;
static constexpr TDuration TestRequestTimeout = TDuration::Seconds(42);
static const TString TestClientId = "testClientId";

////////////////////////////////////////////////////////////////////////////////

struct TTestSessionManager final
    : public ISessionManager
{
    ui32 CreateSessionCounter = 0;
    NProto::TStartEndpointRequest LastCreateSesionRequest;

    ui32 AlterSessionCounter = 0;
    TString LastAlterSocketPath;
    NProto::EVolumeAccessMode LastAlterAccessMode;
    NProto::EVolumeMountMode LastAlterMountMode;
    ui64 LastAlterMountSeqNumber;

    TFuture<TSessionOrError> CreateSession(
        TCallContextPtr ctx,
        const NProto::TStartEndpointRequest& request) override
    {
        Y_UNUSED(ctx);

        ++CreateSessionCounter;
        LastCreateSesionRequest = request;
        return MakeFuture<TSessionOrError>(TSessionInfo());
    }

    TFuture<NProto::TError> RemoveSession(
        TCallContextPtr ctx,
        const TString& socketPath,
        const NProto::THeaders& headers) override
    {
        Y_UNUSED(ctx);
        Y_UNUSED(socketPath);
        Y_UNUSED(headers);
        return MakeFuture(NProto::TError());
    }

    TFuture<NProto::TError> AlterSession(
        TCallContextPtr ctx,
        const TString& socketPath,
        NProto::EVolumeAccessMode accessMode,
        NProto::EVolumeMountMode mountMode,
        ui64 mountSeqNumber,
        const NProto::THeaders& headers) override
    {
        Y_UNUSED(ctx);
        Y_UNUSED(headers);
        ++AlterSessionCounter;
        LastAlterSocketPath = socketPath;
        LastAlterAccessMode = accessMode;
        LastAlterMountMode = mountMode;
        LastAlterMountSeqNumber = mountSeqNumber;
        return MakeFuture(NProto::TError());
    }

    TFuture<TSessionOrError> GetSession(
        TCallContextPtr callContext,
        const TString& socketPath,
        const NProto::THeaders& headers) override
    {
        Y_UNUSED(callContext);
        Y_UNUSED(socketPath);
        Y_UNUSED(headers);
        return MakeFuture<TSessionOrError>(TSessionInfo());
    }

    TResultOrError<NProto::TClientPerformanceProfile> GetProfile(
        const TString& socketPath) override
    {
        Y_UNUSED(socketPath);
        return NProto::TClientPerformanceProfile();
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TMockSessionManager final: public ISessionManager
{
    MOCK_METHOD(
        TFuture<TSessionOrError>,
        CreateSession,
        (TCallContextPtr, const NProto::TStartEndpointRequest&),
        (override));
    MOCK_METHOD(
        TFuture<NProto::TError>,
        RemoveSession,
        (TCallContextPtr, const TString&, const NProto::THeaders&),
        (override));
    MOCK_METHOD(
        TFuture<NProto::TError>,
        AlterSession,
        (TCallContextPtr,
         const TString&,
         NProto::EVolumeAccessMode,
         NProto::EVolumeMountMode,
         ui64,
         const NProto::THeaders&),
        (override));
    MOCK_METHOD(
        TFuture<TSessionOrError>,
        GetSession,
        (TCallContextPtr, const TString&, const NProto::THeaders&),
        (override));
    MOCK_METHOD(
        TResultOrError<NProto::TClientPerformanceProfile>,
        GetProfile,
        (const TString&),
        (override));
};

////////////////////////////////////////////////////////////////////////////////

struct TTestDeviceFactory
    : public NBD::IDeviceFactory
{
    TVector<TString> Devices;

    NBD::IDevicePtr Create(
        const TNetworkAddress& connectAddress,
        TString deviceName,
        ui64 blockCount,
        ui32 blockSize) override
    {
        Y_UNUSED(connectAddress);
        Y_UNUSED(blockCount);
        Y_UNUSED(blockSize);
        Devices.push_back(deviceName);
        return NBD::CreateDeviceStub();
    }
};

////////////////////////////////////////////////////////////////////////////////

class TControlledDevice final: public NBD::IDevice
{
public:
    explicit TControlledDevice(
        std::function<NThreading::TFuture<NProto::TError>()> deviceStubFunction)
        : DeviceStubFunction(std::move(deviceStubFunction))
    {}

    NThreading::TFuture<NProto::TError> Start() override
    {
        return DeviceStubFunction();
    }

    NThreading::TFuture<NProto::TError> Stop(bool deleteDevice) override
    {
        Y_UNUSED(deleteDevice);

        return DeviceStubFunction();
    }

    NThreading::TFuture<NProto::TError> Resize(ui64 deviceSizeInBytes) override
    {
        Y_UNUSED(deviceSizeInBytes);

        return DeviceStubFunction();
    }

private:
    std::function<NThreading::TFuture<NProto::TError>()> DeviceStubFunction;
};

struct TTestControlledDeviceFactory: public NBD::IDeviceFactory
{
    std::function<NThreading::TFuture<NProto::TError>()> DeviceStubFunction;

    explicit TTestControlledDeviceFactory(
        std::function<NThreading::TFuture<NProto::TError>()> deviceStubFunction)
        : DeviceStubFunction(std::move(deviceStubFunction))
    {}

    NBD::IDevicePtr Create(
        const TNetworkAddress& connectAddress,
        TString deviceName,
        ui64 blockCount,
        ui32 blockSize) override
    {
        Y_UNUSED(connectAddress);
        Y_UNUSED(deviceName);
        Y_UNUSED(blockCount);
        Y_UNUSED(blockSize);
        return std::make_shared<TControlledDevice>(DeviceStubFunction);
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TTestEndpoint
{
    NProto::TStartEndpointRequest Request;
    NClient::ISessionPtr Session;
};

////////////////////////////////////////////////////////////////////////////////

class TTestEndpointListener final
    : public IEndpointListener
{
private:
    const TFuture<NProto::TError> Result;

    TMap<TString, TTestEndpoint> Endpoints;

public:
    ui32 AlterEndpointCounter = 0;
    ui32 SwitchEndpointCounter = 0;

    using TStartEndpointHandler = std::function<TFuture<NProto::TError>(
        const NProto::TStartEndpointRequest& request,
        NClient::ISessionPtr session)>;

public:
    TTestEndpointListener(
            TFuture<NProto::TError> result = MakeFuture<NProto::TError>())
        : Result(std::move(result))
    {}

    TFuture<NProto::TError> StartEndpoint(
        const NProto::TStartEndpointRequest& request,
        const NProto::TVolume& volume,
        NClient::ISessionPtr session) override
    {
        Y_UNUSED(volume);
        return StartEndpointHandler(request, session);
    }

    TStartEndpointHandler StartEndpointHandler = [&] (
        const NProto::TStartEndpointRequest& request,
        NClient::ISessionPtr session)
    {
        UNIT_ASSERT(!Endpoints.contains(request.GetUnixSocketPath()));

        TFsPath(request.GetUnixSocketPath()).Touch();

        Endpoints.emplace(
            request.GetUnixSocketPath(),
            TTestEndpoint {
                .Request = request,
                .Session = std::move(session)
            });

        return Result;
    };

    TFuture<NProto::TError> AlterEndpoint(
        const NProto::TStartEndpointRequest& request,
        const NProto::TVolume& volume,
        NClient::ISessionPtr session) override
    {
        Y_UNUSED(request, volume, session);

        ++AlterEndpointCounter;

        return MakeFuture<NProto::TError>();
    }

    TFuture<NProto::TError> StopEndpoint(const TString& socketPath) override
    {
        Endpoints.erase(socketPath);
        TFsPath(socketPath).DeleteIfExists();
        return Result;
    }

    NProto::TError RefreshEndpoint(
        const TString& socketPath,
        const NProto::TVolume& volume) override
    {
        Y_UNUSED(socketPath);
        Y_UNUSED(volume);
        return {};
    }

    TFuture<NProto::TError> SwitchEndpoint(
        const NProto::TStartEndpointRequest& request,
        const NProto::TVolume& volume,
        NClient::ISessionPtr session) override
    {
        Y_UNUSED(request);
        Y_UNUSED(volume);
        Y_UNUSED(session);

        ++SwitchEndpointCounter;

        return MakeFuture<NProto::TError>();
    }

    TMap<TString, TTestEndpoint> GetEndpoints() const
    {
        return Endpoints;
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TBootstrap
{
    const TString DirPath = "./" + CreateGuidAsString();
    ILoggingServicePtr Logging = CreateLoggingService("console");
    ITimerPtr Timer = CreateWallClockTimer();
    std::shared_ptr<TTestScheduler> Scheduler = std::make_shared<TTestScheduler>();
    IBlockStorePtr Service = std::make_shared<TTestService>();
    TExecutorPtr Executor = TExecutor::Create("TestService");
    IRequestStatsPtr RequestStats = CreateRequestStatsStub();
    IVolumeStatsPtr VolumeStats = CreateVolumeStatsStub();
    IServerStatsPtr ServerStats = CreateServerStatsStub();
    ISessionManagerPtr SessionManager;
    IEndpointStoragePtr EndpointStorage = CreateFileEndpointStorage(DirPath);
    TTempDir EndpointsDir = TTempDir(DirPath);
    THashMap<NProto::EClientIpcType, IEndpointListenerPtr> EndpointListeners;
    NBD::IDeviceFactoryPtr NbdDeviceFactory;
    IEndpointEventProxyPtr EndpointEventHandler = CreateEndpointEventProxy();
    TEndpointManagerOptions Options;
    IEndpointManagerPtr EndpointManager;

    TBootstrap() = default;

    ~TBootstrap()
    {
        Stop();
    }

    void Start()
    {
        if (Logging) {
            Logging->Start();
        }

        if (Service) {
            Service->Start();
        }

        if (Executor) {
            Executor->Start();
        }

        if (EndpointManager) {
            EndpointManager->Start();
        }
    }

    void Stop()
    {
        if (EndpointManager) {
            EndpointManager->Stop();
        }

        if (Executor) {
            Executor->Stop();
        }

        if (Service) {
            Service->Stop();
        }

        if (Logging) {
            Logging->Stop();
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

void SetDefaultHeaders(NProto::TStartEndpointRequest& request)
{
    request.MutableHeaders()->SetRequestTimeout(
        TestRequestTimeout.MilliSeconds());
}

void CheckRequestHeaders(const NProto::THeaders& headers)
{
    UNIT_ASSERT_VALUES_EQUAL(
        TestRequestTimeout.MilliSeconds(),
        headers.GetRequestTimeout());

    UNIT_ASSERT_VALUES_EQUAL(TestClientId, headers.GetClientId());
}

////////////////////////////////////////////////////////////////////////////////

std::shared_ptr<TTestService> CreateTestService(
    TMap<TString, NProto::TMountVolumeRequest>& mountedVolumes)
{
    auto service = std::make_shared<TTestService>();
    service->DescribeVolumeHandler =
        [&] (std::shared_ptr<NProto::TDescribeVolumeRequest> request) {
            Y_DEFER {
                request->Clear();
            };

            UNIT_ASSERT_VALUES_EQUAL(
                TestRequestTimeout.MilliSeconds(),
                request->GetHeaders().GetRequestTimeout());

            NProto::TDescribeVolumeResponse response;
            response.MutableVolume()->SetDiskId(request->GetDiskId());
            response.MutableVolume()->SetBlockSize(DefaultBlockSize);
            return MakeFuture(response);
        };
    service->MountVolumeHandler =
        [&] (std::shared_ptr<NProto::TMountVolumeRequest> request) {
            Y_DEFER {
                request->Clear();
            };

            CheckRequestHeaders(request->GetHeaders());

            mountedVolumes[request->GetDiskId()] = *request;

            NProto::TMountVolumeResponse response;
            response.MutableVolume()->SetDiskId(request->GetDiskId());
            response.MutableVolume()->SetBlockSize(DefaultBlockSize);
            return MakeFuture(response);
        };
    service->UnmountVolumeHandler =
        [&] (std::shared_ptr<NProto::TUnmountVolumeRequest> request) {
            Y_DEFER {
                request->Clear();
            };

            CheckRequestHeaders(request->GetHeaders());

            mountedVolumes.erase(request->GetDiskId());

            return MakeFuture(NProto::TUnmountVolumeResponse());
        };
    service->WriteBlocksLocalHandler =
        [&] (std::shared_ptr<NProto::TWriteBlocksLocalRequest> request) {
            Y_DEFER {
                request->Clear();
            };

            CheckRequestHeaders(request->GetHeaders());

            return MakeFuture(NProto::TWriteBlocksLocalResponse());
        };
    service->ReadBlocksLocalHandler =
        [&] (std::shared_ptr<NProto::TReadBlocksLocalRequest> request) {
            Y_DEFER {
                request->Clear();
            };

            CheckRequestHeaders(request->GetHeaders());

            return MakeFuture(NProto::TReadBlocksLocalResponse());
        };
    service->ZeroBlocksHandler =
        [&] (std::shared_ptr<NProto::TZeroBlocksRequest> request) {
            Y_DEFER {
                request->Clear();
            };

            CheckRequestHeaders(request->GetHeaders());

            return MakeFuture(NProto::TZeroBlocksResponse());
        };

    return service;
}

////////////////////////////////////////////////////////////////////////////////

IEndpointManagerPtr CreateEndpointManager(TBootstrap& bootstrap)
{
    if (!bootstrap.SessionManager) {
        TSessionManagerOptions sessionManagerOptions;
        sessionManagerOptions.DefaultClientConfig.SetRequestTimeout(
            TestRequestTimeout.MilliSeconds());

        auto encryptionClientFactory = CreateEncryptionClientFactory(
            bootstrap.Logging,
            CreateDefaultEncryptionKeyProvider(),
            NProto::EZP_WRITE_ENCRYPTED_ZEROS);

        bootstrap.SessionManager = CreateSessionManager(
            bootstrap.Timer,
            bootstrap.Scheduler,
            bootstrap.Logging,
            CreateMonitoringServiceStub(),
            bootstrap.RequestStats,
            bootstrap.VolumeStats,
            bootstrap.ServerStats,
            bootstrap.Service,
            CreateCellManagerStub(),
            CreateDefaultStorageProvider(bootstrap.Service),
            std::move(encryptionClientFactory),
            bootstrap.Executor,
            std::move(sessionManagerOptions));
    }

    bootstrap.EndpointManager = NServer::CreateEndpointManager(
        bootstrap.Timer,
        bootstrap.Scheduler,
        bootstrap.Logging,
        bootstrap.RequestStats,
        bootstrap.VolumeStats,
        bootstrap.ServerStats,
        bootstrap.Executor,
        bootstrap.EndpointEventHandler,
        bootstrap.SessionManager,
        bootstrap.EndpointStorage,
        bootstrap.EndpointListeners,
        bootstrap.NbdDeviceFactory,
        bootstrap.Options);

    return bootstrap.EndpointManager;
}

////////////////////////////////////////////////////////////////////////////////

TFuture<NProto::TStartEndpointResponse> StartEndpoint(
    IEndpointManager& endpointManager,
    const NProto::TStartEndpointRequest& request)
{
    return endpointManager.StartEndpoint(
        MakeIntrusive<TCallContext>(),
        std::make_shared<NProto::TStartEndpointRequest>(request));
}

TFuture<NProto::TStopEndpointResponse> StopEndpoint(
    IEndpointManager& endpointManager,
    const TString& unixSocketPath)
{
    auto request = std::make_shared<NProto::TStopEndpointRequest>();
    request->SetUnixSocketPath(unixSocketPath);

    return endpointManager.StopEndpoint(
        MakeIntrusive<TCallContext>(),
        std::move(request));
}

TFuture<NProto::TListEndpointsResponse> ListEndpoints(
    IEndpointManager& endpointManager)
{
    return endpointManager.ListEndpoints(
        MakeIntrusive<TCallContext>(),
        std::make_shared<NProto::TListEndpointsRequest>());
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TEndpointManagerTest)
{
    Y_UNIT_TEST(ShouldHandleStartStopEndpoint)
    {
        TTempDir dir;
        TString unixSocket = (dir.Path() / "testSocket").GetPath();
        TString diskId = "testDiskId";
        auto ipcType = NProto::IPC_GRPC;

        TBootstrap bootstrap;
        TMap<TString, NProto::TMountVolumeRequest> mountedVolumes;
        bootstrap.Service = CreateTestService(mountedVolumes);

        auto listener = std::make_shared<TTestEndpointListener>();
        bootstrap.EndpointListeners = {{ ipcType, listener }};

        auto manager = CreateEndpointManager(bootstrap);
        bootstrap.Start();
        Y_DEFER {
            bootstrap.Stop();
        };
        manager->RestoreEndpoints().Wait(5s);

        {
            NProto::TStartEndpointRequest request;
            SetDefaultHeaders(request);
            request.SetUnixSocketPath(unixSocket);
            request.SetDiskId(diskId);
            request.SetClientId(TestClientId);
            request.SetIpcType(ipcType);

            auto future = StartEndpoint(*manager, request);
            auto response = future.GetValue(TDuration::Seconds(5));
            UNIT_ASSERT(!HasError(response));

            UNIT_ASSERT(mountedVolumes.contains(diskId));
            UNIT_ASSERT(listener->GetEndpoints().contains(unixSocket));
        }

        {
            auto future = StopEndpoint(*manager, unixSocket);
            auto response = future.GetValue(TDuration::Seconds(5));
            UNIT_ASSERT(!HasError(response));

            UNIT_ASSERT(mountedVolumes.empty());
            UNIT_ASSERT(listener->GetEndpoints().empty());
        }
    }

    Y_UNIT_TEST(ShouldChangeMountModesUsingStartEndpoint)
    {
        TTempDir dir;
        TString unixSocket = (dir.Path() / "testSocket").GetPath();
        auto ipcType = NProto::IPC_GRPC;
        TString diskId = "testDiskId";

        TBootstrap bootstrap;
        TMap<TString, NProto::TMountVolumeRequest> mountedVolumes;
        bootstrap.Service = CreateTestService(mountedVolumes);

        auto listener = std::make_shared<TTestEndpointListener>();
        bootstrap.EndpointListeners = {{ ipcType, listener }};

        auto manager = CreateEndpointManager(bootstrap);
        bootstrap.Start();
        Y_DEFER {
            bootstrap.Stop();
        };
        manager->RestoreEndpoints().Wait(5s);

        UNIT_ASSERT_VALUES_EQUAL(0, listener->AlterEndpointCounter);

        auto accessMode = NProto::VOLUME_ACCESS_READ_ONLY;
        auto mountMode = NProto::VOLUME_MOUNT_REMOTE;
        ui64 mountSeqNumber = 2;

        {
            NProto::TStartEndpointRequest request;
            SetDefaultHeaders(request);
            request.SetUnixSocketPath(unixSocket);
            request.SetDiskId(diskId);
            request.SetClientId(TestClientId);
            request.SetIpcType(ipcType);
            request.SetVolumeAccessMode(accessMode);
            request.SetVolumeMountMode(mountMode);
            request.SetMountSeqNumber(mountSeqNumber);

            auto future = StartEndpoint(*manager, request);
            auto response = future.GetValue(TDuration::Seconds(5));
            UNIT_ASSERT(!HasError(response));

            UNIT_ASSERT_VALUES_EQUAL(0, listener->AlterEndpointCounter);

            const auto& mountRequest = mountedVolumes.find(diskId)->second;
            UNIT_ASSERT(mountRequest.GetVolumeAccessMode() == accessMode);
            UNIT_ASSERT(mountRequest.GetVolumeMountMode() == mountMode);
            UNIT_ASSERT(mountRequest.GetMountSeqNumber() == mountSeqNumber);
        }

        {
            accessMode = NProto::VOLUME_ACCESS_READ_WRITE;
            mountMode = NProto::VOLUME_MOUNT_LOCAL;
            ++mountSeqNumber;

            NProto::TStartEndpointRequest request;
            SetDefaultHeaders(request);
            request.SetUnixSocketPath(unixSocket);
            request.SetDiskId(diskId);
            request.SetClientId(TestClientId);
            request.SetIpcType(ipcType);
            request.SetVolumeAccessMode(accessMode);
            request.SetVolumeMountMode(mountMode);
            request.SetMountSeqNumber(mountSeqNumber);

            auto future = StartEndpoint(*manager, request);
            auto response = future.GetValue(TDuration::Seconds(5));
            UNIT_ASSERT(!HasError(response));

            UNIT_ASSERT_VALUES_EQUAL(1, listener->AlterEndpointCounter);

            const auto& mountRequest = mountedVolumes.find(diskId)->second;
            UNIT_ASSERT(mountRequest.GetVolumeAccessMode() == accessMode);
            UNIT_ASSERT(mountRequest.GetVolumeMountMode() == mountMode);
            UNIT_ASSERT(mountRequest.GetMountSeqNumber() == mountSeqNumber);
        }

        {
            auto future = StopEndpoint(*manager, unixSocket);
            auto response = future.GetValue(TDuration::Seconds(5));
            UNIT_ASSERT(!HasError(response));

            UNIT_ASSERT(mountedVolumes.empty());
        }

        UNIT_ASSERT_VALUES_EQUAL(1, listener->AlterEndpointCounter);
    }

    Y_UNIT_TEST(ShouldHandleListEndpoints)
    {
        TBootstrap bootstrap;
        TMap<TString, NProto::TMountVolumeRequest> mountedVolumes;
        bootstrap.Service = CreateTestService(mountedVolumes);

        bootstrap.EndpointListeners = {
            { NProto::IPC_GRPC, std::make_shared<TTestEndpointListener>() },
            { NProto::IPC_NBD, std::make_shared<TTestEndpointListener>() },
        };

        auto manager = CreateEndpointManager(bootstrap);
        bootstrap.Start();
        Y_DEFER {
            bootstrap.Stop();
        };
        manager->RestoreEndpoints().Wait(5s);

        TTempDir dir;

        NProto::TStartEndpointRequest request1;
        SetDefaultHeaders(request1);
        request1.SetUnixSocketPath((dir.Path() / "testSocket1").GetPath());
        request1.SetDiskId("testDiskId1");
        request1.SetClientId(TestClientId);
        request1.SetIpcType(NProto::IPC_GRPC);

        NProto::TStartEndpointRequest request2;
        SetDefaultHeaders(request2);
        request2.SetUnixSocketPath((dir.Path() / "testSocket2").GetPath());
        request2.SetDiskId("testDiskId2");
        request2.SetClientId(TestClientId);
        request2.SetIpcType(NProto::IPC_NBD);

        {
            auto future = StartEndpoint(*manager, request1);
            auto response = future.GetValue(TDuration::Seconds(5));
            UNIT_ASSERT(!HasError(response));
        }

        {
            auto future = StartEndpoint(*manager, request2);
            auto response = future.GetValue(TDuration::Seconds(5));
            UNIT_ASSERT(!HasError(response));
        }

        {
            auto future = ListEndpoints(*manager);
            auto response = future.GetValue(TDuration::Seconds(5));
            UNIT_ASSERT(!HasError(response));

            UNIT_ASSERT(response.EndpointsSize() == 2);

            auto endpoint1 = response.GetEndpoints().Get(0);
            auto endpoint2 = response.GetEndpoints().Get(1);
            if (endpoint1.GetUnixSocketPath() != request1.GetUnixSocketPath()) {
                endpoint1 = response.GetEndpoints().Get(1);
                endpoint2 = response.GetEndpoints().Get(0);
            }

            UNIT_ASSERT(endpoint1.GetUnixSocketPath() == request1.GetUnixSocketPath());
            UNIT_ASSERT(endpoint1.GetIpcType() == request1.GetIpcType());
            UNIT_ASSERT(endpoint1.GetDiskId() == request1.GetDiskId());

            UNIT_ASSERT(endpoint2.GetUnixSocketPath() == request2.GetUnixSocketPath());
            UNIT_ASSERT(endpoint2.GetIpcType() == request2.GetIpcType());
            UNIT_ASSERT(endpoint2.GetDiskId() == request2.GetDiskId());
        }
    }

    Y_UNIT_TEST(ShouldNotStartStopEndpointTwice)
    {
        TBootstrap bootstrap;
        TMap<TString, NProto::TMountVolumeRequest> mountedVolumes;
        bootstrap.Service = CreateTestService(mountedVolumes);

        auto listener = std::make_shared<TTestEndpointListener>();
        bootstrap.EndpointListeners = {{ NProto::IPC_GRPC, listener }};

        auto manager = CreateEndpointManager(bootstrap);
        bootstrap.Start();
        Y_DEFER {
            bootstrap.Stop();
        };
        manager->RestoreEndpoints().Wait(5s);

        TTempDir dir;
        auto socketPath = (dir.Path() / "testSocket").GetPath();
        auto diskId = "testDiskId";

        NProto::TStartEndpointRequest startRequest;
        SetDefaultHeaders(startRequest);
        startRequest.SetUnixSocketPath(socketPath);
        startRequest.SetDiskId(diskId);
        startRequest.SetClientId(TestClientId);
        startRequest.SetIpcType(NProto::IPC_GRPC);

        {
            auto future = StartEndpoint(*manager, startRequest);
            auto response = future.GetValue(TDuration::Seconds(5));
            UNIT_ASSERT(response.GetError().GetCode() == S_OK);
        }

        {
            auto future = StartEndpoint(*manager, startRequest);
            auto response = future.GetValue(TDuration::Seconds(5));
            UNIT_ASSERT_C(response.GetError().GetCode() == S_ALREADY, response.GetError());
        }

        UNIT_ASSERT(mountedVolumes.contains(diskId));
        UNIT_ASSERT(listener->GetEndpoints().contains(socketPath));

        {
            auto future = StopEndpoint(*manager, socketPath);
            auto response = future.GetValue(TDuration::Seconds(5));
            UNIT_ASSERT(response.GetError().GetCode() == S_OK);
        }

        {
            auto future = StopEndpoint(*manager, socketPath);
            auto response = future.GetValue(TDuration::Seconds(5));
            UNIT_ASSERT(response.GetError().GetCode() == S_FALSE);
        }

        UNIT_ASSERT(mountedVolumes.empty());
        UNIT_ASSERT(listener->GetEndpoints().empty());
    }

    Y_UNIT_TEST(ShouldNotStartBusyEndpoint)
    {
        TBootstrap bootstrap;
        TMap<TString, NProto::TMountVolumeRequest> mountedVolumes;
        bootstrap.Service = CreateTestService(mountedVolumes);

        auto grpcListener = std::make_shared<TTestEndpointListener>();
        auto nbdListener = std::make_shared<TTestEndpointListener>();
        bootstrap.EndpointListeners = {
            { NProto::IPC_GRPC, grpcListener },
            { NProto::IPC_NBD, nbdListener },
        };

        auto manager = CreateEndpointManager(bootstrap);
        bootstrap.Start();
        Y_DEFER {
            bootstrap.Stop();
        };
        manager->RestoreEndpoints().Wait(5s);

        TTempDir dir;
        auto socketPath = (dir.Path() / "testSocket").GetPath();

        {
            NProto::TStartEndpointRequest request;
            SetDefaultHeaders(request);
            request.SetUnixSocketPath(socketPath);
            request.SetDiskId("testDiskId1");
            request.SetClientId(TestClientId);
            request.SetIpcType(NProto::IPC_GRPC);

            auto future = StartEndpoint(*manager, request);
            auto response = future.GetValue(TDuration::Seconds(5));
            UNIT_ASSERT(!HasError(response));
            UNIT_ASSERT(grpcListener->GetEndpoints().size() == 1);
            UNIT_ASSERT(nbdListener->GetEndpoints().size() == 0);
        }

        {
            NProto::TStartEndpointRequest request;
            SetDefaultHeaders(request);
            request.SetUnixSocketPath(socketPath);
            request.SetDiskId("testDiskId2");
            request.SetClientId(TestClientId);
            request.SetIpcType(NProto::IPC_NBD);

            auto future = StartEndpoint(*manager, request);
            auto response = future.GetValue(TDuration::Seconds(5));
            UNIT_ASSERT(response.GetError().GetCode() == E_INVALID_STATE);
            UNIT_ASSERT(grpcListener->GetEndpoints().size() == 1);
            UNIT_ASSERT(nbdListener->GetEndpoints().size() == 0);
        }
    }

    Y_UNIT_TEST(ShouldNotMountDiskWhenStartEndpointFailed)
    {
        TBootstrap bootstrap;
        TMap<TString, NProto::TMountVolumeRequest> mountedVolumes;
        bootstrap.Service = CreateTestService(mountedVolumes);

        auto error = TErrorResponse(E_FAIL, "Endpoint listener is broken");
        auto listener = std::make_shared<TTestEndpointListener>(
            MakeFuture<NProto::TError>(error));
        bootstrap.EndpointListeners = {{ NProto::IPC_GRPC, listener }};

        auto manager = CreateEndpointManager(bootstrap);
        bootstrap.Start();
        Y_DEFER {
            bootstrap.Stop();
        };

        TTempDir dir;

        NProto::TStartEndpointRequest request;
        SetDefaultHeaders(request);
        request.SetUnixSocketPath((dir.Path() / "testSocket").GetPath());
        request.SetDiskId("testDiskId");
        request.SetClientId(TestClientId);
        request.SetIpcType(NProto::IPC_GRPC);

        auto future = StartEndpoint(*manager, request);
        UNIT_ASSERT(HasError(future.GetValue(TDuration::Seconds(5))));
        UNIT_ASSERT(mountedVolumes.empty());
    }

    Y_UNIT_TEST(ShouldHandleLocalRequests)
    {
        TBootstrap bootstrap;
        TMap<TString, NProto::TMountVolumeRequest> mountedVolumes;
        bootstrap.Service = CreateTestService(mountedVolumes);

        auto listener = std::make_shared<TTestEndpointListener>();
        bootstrap.EndpointListeners = {{ NProto::IPC_GRPC, listener }};

        auto manager = CreateEndpointManager(bootstrap);
        bootstrap.Start();
        Y_DEFER {
            bootstrap.Stop();
        };
        manager->RestoreEndpoints().Wait(5s);

        TTempDir dir;
        TString unixSocket = (dir.Path() / "testSocket").GetPath();

        {
            NProto::TStartEndpointRequest request;
            SetDefaultHeaders(request);
            request.SetUnixSocketPath(unixSocket);
            request.SetDiskId("testDiskId");
            request.SetClientId(TestClientId);
            request.SetIpcType(NProto::IPC_GRPC);

            auto future = StartEndpoint(*manager, request);
            auto response = future.GetValue(TDuration::Seconds(5));
            UNIT_ASSERT(!HasError(response));
        }

        const auto& endpoints = listener->GetEndpoints();
        auto session = endpoints.find(unixSocket)->second.Session;

        const ui64 startIndex = 8;
        const ui64 blocksCount = 32;

        TVector<TString> blocks;
        auto sglist = ResizeBlocks(
            blocks,
            blocksCount,
            TString(DefaultBlockSize, 'f'));

        {
            auto request = std::make_shared<NProto::TWriteBlocksLocalRequest>();
            request->SetStartIndex(startIndex);
            request->BlocksCount = blocksCount;
            request->BlockSize = DefaultBlockSize;
            request->Sglist = TGuardedSgList(sglist);

            auto future = session->WriteBlocksLocal(
                MakeIntrusive<TCallContext>(),
                std::move(request));

            const auto& response = future.GetValue(TDuration::Seconds(5));
            UNIT_ASSERT(!HasError(response));
        }

        {
            auto request = std::make_shared<NProto::TReadBlocksLocalRequest>();
            request->SetStartIndex(startIndex);
            request->SetBlocksCount(blocksCount);
            request->BlockSize = DefaultBlockSize;
            request->Sglist = TGuardedSgList(sglist);

            auto future = session->ReadBlocksLocal(
                MakeIntrusive<TCallContext>(),
                std::move(request));

            const auto& response = future.GetValue(TDuration::Seconds(5));
            UNIT_ASSERT(!HasError(response));
        }

        {
            auto request = std::make_shared<NProto::TZeroBlocksRequest>();
            request->SetStartIndex(startIndex);
            request->SetBlocksCount(blocksCount);

            auto future = session->ZeroBlocks(
                MakeIntrusive<TCallContext>(),
                std::move(request));

            const auto& response = future.GetValue(TDuration::Seconds(5));
            UNIT_ASSERT(!HasError(response));
        }
    }

    Y_UNIT_TEST(ShouldMountUnmountVolumeForMetrics)
    {
        TString testDiskId = "testDiskId";

        auto serverStats = std::make_shared<TTestServerStats>();

        ui32 mountCounter = 0;
        ui32 unmountCounter = 0;

        serverStats->MountVolumeHandler = [&] (
                const NProto::TVolume& volume,
                const TString& clientId,
                const TString& instanceId)
            {
                Y_UNUSED(clientId);
                Y_UNUSED(instanceId);

                UNIT_ASSERT(volume.GetDiskId() == testDiskId);
                ++mountCounter;
                return true;
            };

        serverStats->UnmountVolumeHandler = [&] (
                const TString& diskId,
                const TString& clientId)
            {
                Y_UNUSED(clientId);
                UNIT_ASSERT(diskId == testDiskId);
                ++unmountCounter;
            };

        TBootstrap bootstrap;
        TMap<TString, NProto::TMountVolumeRequest> mountedVolumes;
        bootstrap.Service = CreateTestService(mountedVolumes);

        auto listener = std::make_shared<TTestEndpointListener>();
        bootstrap.EndpointListeners = {{ NProto::IPC_VHOST, listener }};
        bootstrap.ServerStats = serverStats;

        auto manager = CreateEndpointManager(bootstrap);
        bootstrap.Start();
        Y_DEFER {
            bootstrap.Stop();
        };
        manager->RestoreEndpoints().Wait(5s);

        TTempDir dir;
        TString unixSocket = (dir.Path() / "testSocket").GetPath();

        {
            NProto::TStartEndpointRequest request;
            SetDefaultHeaders(request);
            request.SetUnixSocketPath(unixSocket);
            request.SetDiskId(testDiskId);
            request.SetClientId(TestClientId);
            request.SetIpcType(NProto::IPC_VHOST);

            auto future = StartEndpoint(*manager, request);
            auto response = future.GetValue(TDuration::Seconds(5));
            UNIT_ASSERT(!HasError(response));
            UNIT_ASSERT(mountCounter == 1);
        }

        {
            auto future = StopEndpoint(*manager, unixSocket);
            auto response = future.GetValue(TDuration::Seconds(5));
            UNIT_ASSERT(!HasError(response));
            UNIT_ASSERT(unmountCounter == 1);
        }
    }

    Y_UNIT_TEST(ShouldNotStartEndpointWithSocketPathLongerThanLimit)
    {
        TBootstrap bootstrap;
        TMap<TString, NProto::TMountVolumeRequest> mountedVolumes;
        bootstrap.Service = CreateTestService(mountedVolumes);

        auto grpcListener = CreateSocketEndpointListener(
            bootstrap.Logging,
            16,
            MODE0660);
        grpcListener->SetClientStorageFactory(CreateClientStorageFactoryStub());
        bootstrap.EndpointListeners = {{ NProto::IPC_GRPC, grpcListener }};

        auto manager = CreateEndpointManager(bootstrap);
        bootstrap.Start();
        Y_DEFER {
            bootstrap.Stop();
        };
        manager->RestoreEndpoints().Wait(5s);

        TString maxSocketPath(UnixSocketPathLengthLimit, 'x');

        NProto::TStartEndpointRequest startRequest;
        SetDefaultHeaders(startRequest);
        startRequest.SetDiskId("testDiskId");
        startRequest.SetClientId(TestClientId);
        startRequest.SetIpcType(NProto::IPC_GRPC);

        {
            startRequest.SetUnixSocketPath(maxSocketPath);
            auto future1 = StartEndpoint(*manager, startRequest);
            UNIT_ASSERT(!HasError(future1.GetValue(TDuration::Seconds(5))));

            UNIT_ASSERT(TFsPath(maxSocketPath).Exists());

            auto future2 = StopEndpoint(*manager, maxSocketPath);
            UNIT_ASSERT(!HasError(future2.GetValue(TDuration::Seconds(5))));

            UNIT_ASSERT(!TFsPath(maxSocketPath).Exists());
        }

        {
            startRequest.SetUnixSocketPath(maxSocketPath + 'x');

            auto future = StartEndpoint(*manager, startRequest);
            auto response = future.GetValue(TDuration::Seconds(5));
            UNIT_ASSERT(HasError(response)
                && response.GetError().GetCode() == E_ARGUMENT);

            UNIT_ASSERT(!TFsPath(startRequest.GetUnixSocketPath()).Exists());
            UNIT_ASSERT(!TFsPath(maxSocketPath).Exists());
        }
    }

    Y_UNIT_TEST(ShouldStartStopNbdEndpointWithGrpcEndpoint)
    {
        TTempDir dir;
        TString unixSocket = (dir.Path() / "testSocket").GetPath();
        TString diskId = "testDiskId";
        TString nbdSocketSuffix = "_nbd";

        TBootstrap bootstrap;
        TMap<TString, NProto::TMountVolumeRequest> mountedVolumes;
        bootstrap.Service = CreateTestService(mountedVolumes);

        auto grpcListener = std::make_shared<TTestEndpointListener>();
        auto nbdListener = std::make_shared<TTestEndpointListener>();
        bootstrap.EndpointListeners = {
            { NProto::IPC_GRPC, grpcListener },
            { NProto::IPC_NBD, nbdListener },
        };
        bootstrap.Options.NbdSocketSuffix = nbdSocketSuffix;

        auto manager = CreateEndpointManager(bootstrap);
        bootstrap.Start();
        Y_DEFER {
            bootstrap.Stop();
        };
        manager->RestoreEndpoints().Wait(5s);

        {
            NProto::TStartEndpointRequest request;
            SetDefaultHeaders(request);
            request.SetUnixSocketPath(unixSocket);
            request.SetDiskId(diskId);
            request.SetClientId(TestClientId);
            request.SetIpcType(NProto::IPC_GRPC);

            auto future = StartEndpoint(*manager, request);
            auto response = future.GetValue(TDuration::Seconds(5));
            UNIT_ASSERT_C(!HasError(response), response.GetError());

            UNIT_ASSERT(mountedVolumes.contains(diskId));

            UNIT_ASSERT(grpcListener->GetEndpoints().contains(unixSocket));
            const auto& grpcEndpoints = grpcListener->GetEndpoints();
            auto grpcRequest = grpcEndpoints.find(unixSocket)->second.Request;
            UNIT_ASSERT(!grpcRequest.GetUnalignedRequestsDisabled());
            UNIT_ASSERT(!grpcRequest.GetSendNbdMinBlockSize());

            auto nbdUnixSocket = unixSocket + nbdSocketSuffix;

            UNIT_ASSERT(nbdListener->GetEndpoints().contains(nbdUnixSocket));
            const auto& nbdEndpoints = nbdListener->GetEndpoints();
            auto nbdRequest = nbdEndpoints.find(nbdUnixSocket)->second.Request;
            UNIT_ASSERT(nbdRequest.GetUnalignedRequestsDisabled());
            UNIT_ASSERT(nbdRequest.GetSendNbdMinBlockSize());
        }

        {
            auto future = StopEndpoint(*manager, unixSocket);
            auto response = future.GetValue(TDuration::Seconds(5));
            UNIT_ASSERT_C(!HasError(response), response.GetError());

            UNIT_ASSERT(mountedVolumes.empty());
            UNIT_ASSERT(grpcListener->GetEndpoints().empty());
            UNIT_ASSERT(nbdListener->GetEndpoints().empty());
        }
    }

    Y_UNIT_TEST(ShouldIgnoreInstanceIdWhenCompareStartEndpointRequests)
    {
        TBootstrap bootstrap;
        TMap<TString, NProto::TMountVolumeRequest> mountedVolumes;
        bootstrap.Service = CreateTestService(mountedVolumes);

        auto sessionManager = std::make_shared<TTestSessionManager>();
        bootstrap.SessionManager = sessionManager;

        auto listener = std::make_shared<TTestEndpointListener>();
        bootstrap.EndpointListeners = {{ NProto::IPC_GRPC, listener }};

        auto manager = CreateEndpointManager(bootstrap);
        bootstrap.Start();
        Y_DEFER {
            bootstrap.Stop();
        };
        manager->RestoreEndpoints().Wait(5s);

        TTempDir dir;
        size_t requestId = 42;

        NProto::TStartEndpointRequest request;
        SetDefaultHeaders(request);
        request.MutableHeaders()->SetRequestId(++requestId);
        request.SetUnixSocketPath((dir.Path() / "testSocket").GetPath());
        request.SetDiskId("testDiskId");
        request.SetClientId(TestClientId);
        request.SetInstanceId("testInstanceId");
        request.SetIpcType(NProto::IPC_GRPC);
        request.SetVolumeAccessMode(NProto::VOLUME_ACCESS_READ_ONLY);
        request.SetVolumeMountMode(NProto::VOLUME_MOUNT_REMOTE);
        request.SetMountSeqNumber(1);

        {
            request.MutableHeaders()->SetRequestId(++requestId);

            auto future = StartEndpoint(*manager, request);
            auto response = future.GetValue(TDuration::Seconds(5));
            UNIT_ASSERT_C(
                S_OK == response.GetError().GetCode(),
                response.GetError());

            UNIT_ASSERT_VALUES_EQUAL(1, sessionManager->CreateSessionCounter);
            UNIT_ASSERT_VALUES_EQUAL(0, sessionManager->AlterSessionCounter);

            google::protobuf::util::MessageDifferencer comparator;
            UNIT_ASSERT(comparator.Equals(
                request,
                sessionManager->LastCreateSesionRequest));
        }

        {
            request.MutableHeaders()->SetRequestId(++requestId);

            auto future = StartEndpoint(*manager, request);
            auto response = future.GetValue(TDuration::Seconds(5));
            UNIT_ASSERT_C(
                S_ALREADY == response.GetError().GetCode(),
                response.GetError());

            UNIT_ASSERT_VALUES_EQUAL(1, sessionManager->CreateSessionCounter);
            UNIT_ASSERT_VALUES_EQUAL(0, sessionManager->AlterSessionCounter);
        }

        {
            request.MutableHeaders()->SetRequestId(++requestId);
            request.SetVolumeAccessMode(NProto::VOLUME_ACCESS_READ_WRITE);
            request.SetVolumeMountMode(NProto::VOLUME_MOUNT_LOCAL);
            request.SetMountSeqNumber(42);

            auto future = StartEndpoint(*manager, request);
            auto response = future.GetValue(TDuration::Seconds(5));
            UNIT_ASSERT_C(
                S_OK == response.GetError().GetCode(),
                response.GetError());

            UNIT_ASSERT_VALUES_EQUAL(1, sessionManager->CreateSessionCounter);
            UNIT_ASSERT_VALUES_EQUAL(1, sessionManager->AlterSessionCounter);

            UNIT_ASSERT_VALUES_EQUAL(
                request.GetUnixSocketPath(), sessionManager->LastAlterSocketPath);
            UNIT_ASSERT(
                NProto::VOLUME_ACCESS_READ_WRITE == sessionManager->LastAlterAccessMode);
            UNIT_ASSERT(
                NProto::VOLUME_MOUNT_LOCAL == sessionManager->LastAlterMountMode);
            UNIT_ASSERT_VALUES_EQUAL(42, sessionManager->LastAlterMountSeqNumber);
        }

        {
            request.SetClientId("otherTestClientId");

            auto future = StartEndpoint(*manager, request);
            auto response = future.GetValue(TDuration::Seconds(5));
            UNIT_ASSERT_C(HasError(response.GetError()), response.GetError());

            UNIT_ASSERT_VALUES_EQUAL(1, sessionManager->CreateSessionCounter);
            UNIT_ASSERT_VALUES_EQUAL(1, sessionManager->AlterSessionCounter);
        }
    }

    Y_UNIT_TEST(ShouldCompareStartEndpointRequestsWithoutHeaders)
    {
        TTempDir dir;
        TString unixSocket = (dir.Path() / "testSocket").GetPath();

        NProto::TStartEndpointRequest request1;
        request1.SetUnixSocketPath(unixSocket);
        request1.SetDiskId("testDiskId");
        request1.SetInstanceId("testInstanceId");
        request1.SetClientId("testClientId");
        auto request2 = request1;

        auto& headers1 = *request1.MutableHeaders();
        headers1.SetClientId("42");
        headers1.SetRequestId(42);

        auto& headers2 = *request1.MutableHeaders();
        headers2.SetClientId("13");
        headers2.SetRequestId(13);

        google::protobuf::util::MessageDifferencer comparator;
        UNIT_ASSERT(!comparator.Equals(request1, request2));

        UNIT_ASSERT(AreSameStartEndpointRequests(request1, request2));
    }

    // NBS-3018, CLOUD-98154
    Y_UNIT_TEST(ShouldIgnoreSomeArgsWhenStartEndpointTwice)
    {
        TBootstrap bootstrap;
        TMap<TString, NProto::TMountVolumeRequest> mountedVolumes;
        bootstrap.Service = CreateTestService(mountedVolumes);

        auto listener = std::make_shared<TTestEndpointListener>();
        bootstrap.EndpointListeners = {{ NProto::IPC_GRPC, listener }};

        auto manager = CreateEndpointManager(bootstrap);
        bootstrap.Start();
        Y_DEFER {
            bootstrap.Stop();
        };
        manager->RestoreEndpoints().Wait(5s);

        TTempDir dir;
        auto socketPath = (dir.Path() / "testSocket").GetPath();
        auto diskId = "testDiskId";

        NProto::TStartEndpointRequest startRequest;
        SetDefaultHeaders(startRequest);
        startRequest.SetUnixSocketPath(socketPath);
        startRequest.SetDiskId(diskId);
        startRequest.SetClientId(TestClientId);
        startRequest.SetIpcType(NProto::IPC_GRPC);

        {
            auto future = StartEndpoint(*manager, startRequest);
            auto response = future.GetValue(TDuration::Seconds(5));
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                response.GetError().GetCode(),
                response.GetError());
        }

        startRequest.MutableClientProfile()->SetCpuUnitCount(42);
        startRequest.SetDeviceName("testDeviceName");

        {
            auto future = StartEndpoint(*manager, startRequest);
            auto response = future.GetValue(TDuration::Seconds(5));
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_ALREADY,
                response.GetError().GetCode(),
                response.GetError());
        }
    }

    Y_UNIT_TEST(ShouldSwitchEndpointWhenEndpointStarted)
    {
        TBootstrap bootstrap;
        TMap<TString, NProto::TMountVolumeRequest> mountedVolumes;
        bootstrap.Service = CreateTestService(mountedVolumes);

        auto listener = std::make_shared<TTestEndpointListener>();
        bootstrap.EndpointListeners = {{ NProto::IPC_VHOST, listener }};

        auto manager = CreateEndpointManager(bootstrap);
        bootstrap.Start();
        Y_DEFER {
            bootstrap.Stop();
        };
        manager->RestoreEndpoints().Wait(5s);

        TTempDir dir;
        auto socketPath = (dir.Path() / "testSocket").GetPath();
        auto diskId = "testDiskId";

        {
            // without started endpoint SwitchEndpointIfNeeded is ignored
            auto future = bootstrap.EndpointEventHandler->SwitchEndpointIfNeeded(
                diskId, "test");
            auto error = future.GetValue(TDuration::Seconds(5));
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                error.GetCode(),
                error);
            UNIT_ASSERT_VALUES_EQUAL(0, listener->SwitchEndpointCounter);
        }

        NProto::TStartEndpointRequest startRequest;
        SetDefaultHeaders(startRequest);
        startRequest.SetUnixSocketPath(socketPath);
        startRequest.SetDiskId(diskId);
        startRequest.SetClientId(TestClientId);
        startRequest.SetIpcType(NProto::IPC_VHOST);

        {
            auto future = StartEndpoint(*manager, startRequest);
            auto response = future.GetValue(TDuration::Seconds(5));
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                response.GetError().GetCode(),
                response.GetError());
        }

        {
            // with started endpoint SwitchEndpointIfNeeded leads to
            // SwitchEndpoint call
            auto future = bootstrap.EndpointEventHandler->SwitchEndpointIfNeeded(
                diskId,
                "test");
            auto error = future.GetValue(TDuration::Seconds(5));
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                error.GetCode(),
                error);
            UNIT_ASSERT_VALUES_EQUAL(1, listener->SwitchEndpointCounter);
        }
    }

    Y_UNIT_TEST(ShouldStartEndpointWithNbdDevice)
    {
        TString nbdDevPrefix = CreateGuidAsString() + "_nbd";
        int deviceCount = 6;
        for (int i = 0; i < deviceCount; ++i) {
            TFsPath(nbdDevPrefix + ToString(i)).Touch();
        }
        Y_DEFER {
            for (int i = 0; i < deviceCount; ++i) {
                TFsPath(nbdDevPrefix + ToString(i)).DeleteIfExists();
            }
        };

        TBootstrap bootstrap;
        TMap<TString, NProto::TMountVolumeRequest> mountedVolumes;
        bootstrap.Service = CreateTestService(mountedVolumes);

        auto listener = std::make_shared<TTestEndpointListener>();
        bootstrap.EndpointListeners = {{ NProto::IPC_NBD, listener }};

        auto deviceFactory = std::make_shared<TTestDeviceFactory>();
        bootstrap.NbdDeviceFactory = deviceFactory;

        bootstrap.Options.NbdDevicePrefix = nbdDevPrefix;

        auto manager = CreateEndpointManager(bootstrap);
        bootstrap.Start();
        Y_DEFER {
            bootstrap.Stop();
        };
        manager->RestoreEndpoints().Wait(5s);

        auto& storage = *bootstrap.EndpointStorage;
        google::protobuf::util::MessageDifferencer comparator;

        TTempDir dir;
        TString unixSocket = (dir.Path() / "testSocket").GetPath();
        TString diskId = "testDiskId";
        TString nbdDevFile = nbdDevPrefix + "0";

        NProto::TStartEndpointRequest baseRequest;
        SetDefaultHeaders(baseRequest);
        baseRequest.SetUnixSocketPath(unixSocket);
        baseRequest.SetDiskId(diskId);
        baseRequest.SetClientId(TestClientId);
        baseRequest.SetIpcType(NProto::IPC_NBD);
        baseRequest.SetNbdDeviceFile(nbdDevFile);
        baseRequest.SetPersistent(true);

        {
            auto request = baseRequest;
            auto future = StartEndpoint(*manager, request);
            auto response = future.GetValue(TDuration::Seconds(5));
            UNIT_ASSERT_C(!HasError(response), response.GetError());

            UNIT_ASSERT(mountedVolumes.contains(diskId));
            UNIT_ASSERT(listener->GetEndpoints().contains(unixSocket));

            UNIT_ASSERT_VALUES_EQUAL(1, deviceFactory->Devices.size());
            UNIT_ASSERT_VALUES_EQUAL(nbdDevFile, deviceFactory->Devices[0]);

            auto [str, error] = storage.GetEndpoint(request.GetUnixSocketPath());
            UNIT_ASSERT(!HasError(error));
            auto req = DeserializeEndpoint<NProto::TStartEndpointRequest>(str);
            UNIT_ASSERT(req);
            UNIT_ASSERT(comparator.Equals(request, *req));
        }

        {
            auto request = baseRequest;
            auto future = StartEndpoint(*manager, request);
            auto response = future.GetValue(TDuration::Seconds(5));
            UNIT_ASSERT(response.GetError().GetCode() == S_ALREADY);
            UNIT_ASSERT_VALUES_EQUAL(nbdDevFile, response.GetNbdDeviceFile());
        }

        {
            auto request = baseRequest;
            request.SetUseFreeNbdDeviceFile(true);
            auto future = StartEndpoint(*manager, request);
            auto response = future.GetValue(TDuration::Seconds(5));
            UNIT_ASSERT(response.GetError().GetCode() == S_ALREADY);
            UNIT_ASSERT_VALUES_EQUAL(nbdDevFile, response.GetNbdDeviceFile());
        }

        {
            auto request = baseRequest;
            request.SetUnixSocketPath(unixSocket + "other");
            auto future = StartEndpoint(*manager, request);
            auto response = future.GetValue(TDuration::Seconds(5));
            UNIT_ASSERT(response.GetError().GetCode() == E_INVALID_STATE);
        }

        {
            auto request = baseRequest;
            request.SetNbdDeviceFile(nbdDevPrefix + "1");
            auto future = StartEndpoint(*manager, request);
            auto response = future.GetValue(TDuration::Seconds(5));
            UNIT_ASSERT(response.GetError().GetCode() == E_INVALID_STATE);
        }

        {
            auto request = baseRequest;
            request.SetNbdDeviceFile("");
            auto future = StartEndpoint(*manager, request);
            auto response = future.GetValue(TDuration::Seconds(5));
            UNIT_ASSERT(response.GetError().GetCode() == E_INVALID_STATE);
        }

        {
            auto future = StopEndpoint(*manager, unixSocket);
            auto response = future.GetValue(TDuration::Seconds(5));
            UNIT_ASSERT(!HasError(response));

            UNIT_ASSERT(mountedVolumes.empty());
            UNIT_ASSERT(listener->GetEndpoints().empty());
        }

        //

        {
            auto request = baseRequest;
            request.SetNbdDeviceFile("");
            auto future = StartEndpoint(*manager, request);
            auto response = future.GetValue(TDuration::Seconds(5));
            UNIT_ASSERT_C(!HasError(response), response.GetError());
        }

        {
            auto request = baseRequest;
            request.SetNbdDeviceFile("");
            auto future = StartEndpoint(*manager, request);
            auto response = future.GetValue(TDuration::Seconds(5));
            UNIT_ASSERT(response.GetError().GetCode() == S_ALREADY);
            UNIT_ASSERT_VALUES_EQUAL("", response.GetNbdDeviceFile());
        }

        {
            auto request = baseRequest;
            request.SetUseFreeNbdDeviceFile(true);
            auto future = StartEndpoint(*manager, request);
            auto response = future.GetValue(TDuration::Seconds(5));
            UNIT_ASSERT(response.GetError().GetCode() == E_INVALID_STATE);
        }

        {
            auto request = baseRequest;
            request.SetNbdDeviceFile(nbdDevPrefix + "1");
            auto future = StartEndpoint(*manager, request);
            auto response = future.GetValue(TDuration::Seconds(5));
            UNIT_ASSERT(response.GetError().GetCode() == E_INVALID_STATE);
        }

        {
            auto future = StopEndpoint(*manager, unixSocket);
            auto response = future.GetValue(TDuration::Seconds(5));
            UNIT_ASSERT(!HasError(response));

            UNIT_ASSERT(mountedVolumes.empty());
            UNIT_ASSERT(listener->GetEndpoints().empty());
        }

        //

        {
            auto request = baseRequest;
            request.SetNbdDeviceFile("blabla3");
            auto future = StartEndpoint(*manager, request);
            auto response = future.GetValue(TDuration::Seconds(5));
            UNIT_ASSERT(response.GetError().GetCode() == E_ARGUMENT);
        }

        {
            auto request = baseRequest;
            request.SetPersistent(false);
            auto future = StartEndpoint(*manager, request);
            auto response = future.GetValue(TDuration::Seconds(5));
            UNIT_ASSERT(response.GetError().GetCode() == E_ARGUMENT);
        }

        //

        int num = 0;
        deviceFactory->Devices.clear();

        for (int i: {0, 1, 3}) {
            auto request = baseRequest;
            request.SetUnixSocketPath(unixSocket + ToString(num++));
            request.SetNbdDeviceFile(nbdDevPrefix + ToString(i));

            auto future = StartEndpoint(*manager, request);
            auto response = future.GetValue(TDuration::Seconds(5));
            UNIT_ASSERT_C(!HasError(response), response.GetError());
            UNIT_ASSERT_VALUES_EQUAL(
                nbdDevPrefix + ToString(i),
                response.GetNbdDeviceFile());

            UNIT_ASSERT_VALUES_EQUAL(num, deviceFactory->Devices.size());
            UNIT_ASSERT_VALUES_EQUAL(
                nbdDevPrefix + ToString(i),
                deviceFactory->Devices[num - 1]);

            auto [str, error] = storage.GetEndpoint(request.GetUnixSocketPath());
            UNIT_ASSERT(!HasError(error));
            auto req = DeserializeEndpoint<NProto::TStartEndpointRequest>(str);
            UNIT_ASSERT(req);
            UNIT_ASSERT(comparator.Equals(request, *req));
        }

        for (int i: {2, 4, 5}) {
            auto request = baseRequest;
            request.SetUnixSocketPath(unixSocket + ToString(num++));
            request.SetUseFreeNbdDeviceFile(true);

            auto future = StartEndpoint(*manager, request);
            auto response = future.GetValue(TDuration::Seconds(5));
            UNIT_ASSERT_C(!HasError(response), response.GetError());
            UNIT_ASSERT_VALUES_EQUAL(
                nbdDevPrefix + ToString(i),
                response.GetNbdDeviceFile());

            UNIT_ASSERT_VALUES_EQUAL(num, deviceFactory->Devices.size());
            UNIT_ASSERT_VALUES_EQUAL(
                nbdDevPrefix + ToString(i),
                deviceFactory->Devices[num - 1]);

            auto [str, error] = storage.GetEndpoint(request.GetUnixSocketPath());
            UNIT_ASSERT(!HasError(error));
            auto req = DeserializeEndpoint<NProto::TStartEndpointRequest>(str);
            UNIT_ASSERT(req);
            UNIT_ASSERT(!comparator.Equals(request, *req));
            UNIT_ASSERT_VALUES_EQUAL(
                nbdDevPrefix + ToString(i),
                req->GetNbdDeviceFile());
        }

         {
            auto request = baseRequest;
            request.SetUnixSocketPath(unixSocket + ToString(num++));
            request.SetUseFreeNbdDeviceFile(true);
            auto future = StartEndpoint(*manager, request);
            auto response = future.GetValue(TDuration::Seconds(5));
            UNIT_ASSERT(response.GetError().GetCode() == E_INVALID_STATE);
        }

        for (int i = 0; i < deviceCount; ++i) {
            auto future = StopEndpoint(*manager, unixSocket + ToString(i));
            auto response = future.GetValue(TDuration::Seconds(5));
            UNIT_ASSERT(!HasError(response));
        }

        UNIT_ASSERT(mountedVolumes.empty());
        UNIT_ASSERT(listener->GetEndpoints().empty());
    }

    Y_UNIT_TEST(ShouldRestoreEndpointWithNbdDevice)
    {
        TString nbdDevPrefix = CreateGuidAsString() + "_nbd";
        int deviceCount = 10;
        for (int i = 0; i < deviceCount; ++i) {
            TFsPath(nbdDevPrefix + ToString(i)).Touch();
        }
        Y_DEFER {
            for (int i = 0; i < deviceCount; ++i) {
                TFsPath(nbdDevPrefix + ToString(i)).DeleteIfExists();
            }
        };

        TBootstrap bootstrap;
        TMap<TString, NProto::TMountVolumeRequest> mountedVolumes;
        bootstrap.Service = CreateTestService(mountedVolumes);

        auto listener = std::make_shared<TTestEndpointListener>();
        bootstrap.EndpointListeners = {{ NProto::IPC_NBD, listener }};

        auto deviceFactory = std::make_shared<TTestDeviceFactory>();
        bootstrap.NbdDeviceFactory = deviceFactory;

        bootstrap.Options.NbdDevicePrefix = nbdDevPrefix;

        auto manager = CreateEndpointManager(bootstrap);
        bootstrap.Start();
        Y_DEFER {
            bootstrap.Stop();
        };

        TTempDir dir;
        TString unixSocket = (dir.Path() / "testSocket").GetPath();
        TString diskId = "testDiskId";

        NProto::TStartEndpointRequest request;
        SetDefaultHeaders(request);
        request.SetDiskId(diskId);
        request.SetClientId(TestClientId);
        request.SetIpcType(NProto::IPC_NBD);

        size_t correctCount = 5;
        size_t wrongCount = 3;

        for (size_t i = 0; i < wrongCount + correctCount; ++i) {
            request.SetUnixSocketPath(unixSocket + ToString(i));

            if (i < wrongCount) {
                request.SetUseFreeNbdDeviceFile(true);
            } else {
                request.SetNbdDeviceFile(nbdDevPrefix + ToString(i));
            }

            auto [str, error] = SerializeEndpoint(request);
            UNIT_ASSERT_C(!HasError(error), error);

            auto ret = bootstrap.EndpointStorage->AddEndpoint(
                request.GetUnixSocketPath(),
                str);
            UNIT_ASSERT_EQUAL_C(S_OK, ret.GetCode(), ret.GetMessage());
        }

        NMonitoring::TDynamicCountersPtr counters = new NMonitoring::TDynamicCounters();
        InitCriticalEventsCounter(counters);
        auto configCounter = counters->GetCounter("AppCriticalEvents/EndpointRestoringError", true);
        UNIT_ASSERT_VALUES_EQUAL(0, static_cast<int>(*configCounter));

        manager->RestoreEndpoints().Wait();

        UNIT_ASSERT(wrongCount != correctCount);
        UNIT_ASSERT_VALUES_EQUAL(wrongCount, static_cast<int>(*configCounter));
    }

    Y_UNIT_TEST(ShouldRemoveEndpointForNotFoundVolume)
    {
        using namespace ::testing;

        TString nbdDevPrefix = CreateGuidAsString() + "_nbd";
        TFsPath(nbdDevPrefix + "0").Touch();
        Y_DEFER {
            TFsPath(nbdDevPrefix +"0").DeleteIfExists();
        };

        TBootstrap bootstrap;
        auto sessionManager =
            std::make_shared<TMockSessionManager>();
        bootstrap.SessionManager = sessionManager;
        TMap<TString, NProto::TMountVolumeRequest> mountedVolumes;
        bootstrap.Service = CreateTestService(mountedVolumes);

        auto listener = std::make_shared<TTestEndpointListener>();
        bootstrap.EndpointListeners = {{ NProto::IPC_NBD, listener }};

        auto deviceFactory = std::make_shared<TTestDeviceFactory>();
        bootstrap.NbdDeviceFactory = deviceFactory;

        bootstrap.Options.NbdDevicePrefix = nbdDevPrefix;

        auto manager = CreateEndpointManager(bootstrap);
        bootstrap.Start();
        Y_DEFER {
            bootstrap.Stop();
        };

        TTempDir dir;
        TString unixSocket = (dir.Path() / "testSocket").GetPath();
        TString diskId = "testDiskId";

        NProto::TStartEndpointRequest request;
        SetDefaultHeaders(request);
        request.SetDiskId(diskId);
        request.SetClientId(TestClientId);
        request.SetIpcType(NProto::IPC_NBD);
        request.SetUnixSocketPath(unixSocket + "0");
        request.SetNbdDeviceFile(nbdDevPrefix + "0");
        auto [str, error] = SerializeEndpoint(request);
        UNIT_ASSERT_C(!HasError(error), error);
        auto ret = bootstrap.EndpointStorage->AddEndpoint(
            request.GetUnixSocketPath(),
            str);
        UNIT_ASSERT_EQUAL_C(S_OK, ret.GetCode(), ret.GetMessage());

        NMonitoring::TDynamicCountersPtr counters = new NMonitoring::TDynamicCounters();
        InitCriticalEventsCounter(counters);
        auto configCounter = counters->GetCounter("AppCriticalEvents/EndpointRestoringError", true);
        UNIT_ASSERT_VALUES_EQUAL(0, static_cast<int>(*configCounter));

        EXPECT_CALL(*sessionManager, CreateSession(_, _))
            .WillOnce(Return(MakeFuture<TMockSessionManager::TSessionOrError>(
                MakeError(MAKE_SCHEMESHARD_ERROR(ENOENT)))));
        manager->RestoreEndpoints().Wait();

        UNIT_ASSERT_VALUES_EQUAL(1, static_cast<int>(*configCounter));

        auto endpoints = bootstrap.EndpointStorage->GetEndpointIds();
        UNIT_ASSERT_C(!HasError(endpoints.GetError()), endpoints.GetError());
        UNIT_ASSERT(endpoints.GetResult().empty());
    }

    Y_UNIT_TEST(ShouldNotUseRestoringNbdDevices)
    {
        TString nbdDevPrefix = CreateGuidAsString() + "_nbd";
        int deviceCount = 6;
        for (int i = 0; i < deviceCount; ++i) {
            TFsPath(nbdDevPrefix + ToString(i)).Touch();
        }
        Y_DEFER {
            for (int i = 0; i < deviceCount; ++i) {
                TFsPath(nbdDevPrefix + ToString(i)).DeleteIfExists();
            }
        };

        TTempDir dir;
        TString unixSocket = (dir.Path() / "testSocket").GetPath();
        TString diskId = "testDiskId";
        TString nbdDevFile = nbdDevPrefix + "0";

        TBootstrap bootstrap;
        TMap<TString, NProto::TMountVolumeRequest> mountedVolumes;
        bootstrap.Service = CreateTestService(mountedVolumes);

        auto rejected = MakeFuture(MakeError(E_REJECTED));
        auto listener = std::make_shared<TTestEndpointListener>(rejected);
        listener->StartEndpointHandler = [&] (
            const NProto::TStartEndpointRequest& request,
            NClient::ISessionPtr session)
        {
            UNIT_ASSERT(session);

            if (unixSocket == request.GetUnixSocketPath()) {
                return MakeFuture(MakeError(E_REJECTED));
            }
            return MakeFuture<NProto::TError>();
        };

        bootstrap.EndpointListeners = {{ NProto::IPC_NBD, listener }};

        auto deviceFactory = std::make_shared<TTestDeviceFactory>();
        bootstrap.NbdDeviceFactory = deviceFactory;

        bootstrap.Options.NbdDevicePrefix = nbdDevPrefix;

        auto manager = CreateEndpointManager(bootstrap);
        bootstrap.Start();
        Y_DEFER {
            bootstrap.Stop();
        };

        NProto::TStartEndpointRequest request;
        SetDefaultHeaders(request);
        request.SetUnixSocketPath(unixSocket);
        request.SetDiskId(diskId);
        request.SetClientId(TestClientId);
        request.SetIpcType(NProto::IPC_NBD);
        request.SetNbdDeviceFile(nbdDevFile);
        request.SetPersistent(true);

        {
            auto [str, error] = SerializeEndpoint(request);
            UNIT_ASSERT_C(!HasError(error), error);

            auto ret = bootstrap.EndpointStorage->AddEndpoint(
                request.GetUnixSocketPath(),
                str);
            UNIT_ASSERT_EQUAL_C(S_OK, ret.GetCode(), ret.GetMessage());
        }

        NMonitoring::TDynamicCountersPtr counters = new NMonitoring::TDynamicCounters();
        InitCriticalEventsCounter(counters);
        auto configCounter = counters->GetCounter("AppCriticalEvents/EndpointRestoringError", true);
        UNIT_ASSERT_VALUES_EQUAL(0, static_cast<int>(*configCounter));

        manager->RestoreEndpoints();
        bootstrap.Scheduler->RunAllScheduledTasks();

        UNIT_ASSERT_VALUES_EQUAL(0, static_cast<int>(*configCounter));

        {
            request.SetUnixSocketPath(unixSocket + "other");
            auto future = StartEndpoint(*manager, request);
            auto response = future.GetValue(TDuration::Seconds(5));
            UNIT_ASSERT(response.GetError().GetCode() == E_INVALID_STATE);
        }

        {
            request.SetUnixSocketPath(unixSocket + "other");
            request.SetUseFreeNbdDeviceFile(true);
            auto future = StartEndpoint(*manager, request);
            auto response = future.GetValue(TDuration::Seconds(5));
            UNIT_ASSERT(!HasError(response));
            UNIT_ASSERT(response.GetNbdDeviceFile() != nbdDevFile);
        }
    }

    Y_UNIT_TEST(ShouldRecreateSocketWhenRestartEndpoint)
    {
        TTempDir dir;
        auto socketPath = dir.Path() / "testSocket";
        TString diskId = "testDiskId";
        auto ipcType = NProto::IPC_GRPC;

        TBootstrap bootstrap;
        TMap<TString, NProto::TMountVolumeRequest> mountedVolumes;
        bootstrap.Service = CreateTestService(mountedVolumes);

        auto grpcListener = CreateSocketEndpointListener(
            bootstrap.Logging,
            16,
            MODE0660);
        grpcListener->SetClientStorageFactory(CreateClientStorageFactoryStub());
        bootstrap.EndpointListeners = {{ NProto::IPC_GRPC, grpcListener }};

        auto manager = CreateEndpointManager(bootstrap);
        bootstrap.Start();
        manager->RestoreEndpoints().Wait(5s);

        NProto::TStartEndpointRequest request;
        SetDefaultHeaders(request);
        request.SetUnixSocketPath(socketPath.GetPath());
        request.SetDiskId(diskId);
        request.SetClientId(TestClientId);
        request.SetIpcType(ipcType);

        socketPath.DeleteIfExists();
        UNIT_ASSERT(!socketPath.Exists());

        {
            auto future = StartEndpoint(*manager, request);
            auto response = future.GetValue(TDuration::Seconds(5));
            UNIT_ASSERT_C(!HasError(response), response.GetError());
        }

        UNIT_ASSERT(socketPath.Exists());
        socketPath.DeleteIfExists();
        UNIT_ASSERT(!socketPath.Exists());

        {
            auto future = StartEndpoint(*manager, request);
            auto response = future.GetValue(TDuration::Seconds(5));
            UNIT_ASSERT(!HasError(response));
        }

        UNIT_ASSERT(socketPath.Exists());

        {
            auto future = StopEndpoint(*manager, socketPath.GetPath());
            auto response = future.GetValue(TDuration::Seconds(5));
            UNIT_ASSERT(!HasError(response));
        }
    }
}

}   // namespace NCloud::NBlockStore::NServer
