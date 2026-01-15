#include "session_manager.h"

#include <cloud/blockstore/libs/cells/iface/cell_manager.h>
#include <cloud/blockstore/libs/client/session.h>
#include <cloud/blockstore/libs/diagnostics/config.h>
#include <cloud/blockstore/libs/diagnostics/dumpable.h>
#include <cloud/blockstore/libs/diagnostics/profile_log.h>
#include <cloud/blockstore/libs/diagnostics/request_stats.h>
#include <cloud/blockstore/libs/diagnostics/server_stats_test.h>
#include <cloud/blockstore/libs/diagnostics/volume_stats.h>
#include <cloud/blockstore/libs/encryption/encryption_client.h>
#include <cloud/blockstore/libs/encryption/encryption_key.h>
#include <cloud/blockstore/libs/service/service_test.h>
#include <cloud/blockstore/libs/service/storage_provider.h>
#include <cloud/blockstore/libs/storage/model/volume_label.h>

#include <cloud/storage/core/libs/common/scheduler_test.h>
#include <cloud/storage/core/libs/common/thread_pool.h>
#include <cloud/storage/core/libs/common/timer.h>
#include <cloud/storage/core/libs/coroutine/executor.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>
#include <cloud/storage/core/libs/diagnostics/monitoring.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/scope.h>

namespace NCloud::NBlockStore::NServer {

using namespace NCells;
using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TTestDumpable
    : public IDumpable
{
    void Dump(IOutputStream& out) const override
    {
        Y_UNUSED(out);
    };

    void DumpHtml(IOutputStream& out) const override
    {
        Y_UNUSED(out);
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TBootstrap
{
    const ILoggingServicePtr Logging = CreateLoggingService("console");
    const TExecutorPtr Executor = TExecutor::Create("TestService");

    TBootstrap()
    {}

    ~TBootstrap()
    {
        Stop();
    }

    void Start()
    {
        if (Logging) {
            Logging->Start();
        }

        if (Executor) {
            Executor->Start();
        }
    }

    void Stop()
    {
        if (Executor) {
            Executor->Stop();
        }

        if (Logging) {
            Logging->Stop();
        }
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TSessionManagerTest)
{
    void ServerStatsShouldMountVolumeWhenEndpointIsStarted(
        NProto::EClientIpcType ipcType)
    {
        TString socketPath = "testSocket";
        TString diskId = "testDiskId";
        ui32 serverStatsMountCounter = 0;

        auto scheduler = std::make_shared<TTestScheduler>();

        auto serverStats = std::make_shared<TTestServerStats>();
        serverStats->MountVolumeHandler = [&] (
                const NProto::TVolume& volume,
                const TString& clientId,
                const TString& instanceId)
            {
                Y_UNUSED(clientId);
                Y_UNUSED(instanceId);

                UNIT_ASSERT_VALUES_EQUAL(diskId, volume.GetDiskId());
                ++serverStatsMountCounter;
                return true;
            };
        serverStats->UnmountVolumeHandler = [&] (
                const TString& unmountDiskId,
                const TString& clientId)
            {
                Y_UNUSED(clientId);
                UNIT_ASSERT_VALUES_EQUAL(diskId, unmountDiskId);
            };

        auto service = std::make_shared<TTestService>();
        service->DescribeVolumeHandler =
            [&] (std::shared_ptr<NProto::TDescribeVolumeRequest> request) {
                auto response = NProto::TDescribeVolumeResponse();
                response.MutableVolume()->SetDiskId(request->GetDiskId());
                return MakeFuture(std::move(response));
            };
        service->MountVolumeHandler =
            [&] (std::shared_ptr<NProto::TMountVolumeRequest> request) {
                NProto::TMountVolumeResponse response;
                response.MutableVolume()->SetDiskId(request->GetDiskId());
                response.SetInactiveClientsTimeout(100);
                return MakeFuture(response);
            };
        service->UnmountVolumeHandler =
            [&] (std::shared_ptr<NProto::TUnmountVolumeRequest> request) {
                Y_UNUSED(request);
                return MakeFuture(NProto::TUnmountVolumeResponse());
            };

        auto executor = TExecutor::Create("TestService");
        auto logging = CreateLoggingService("console");

        auto encryptionClientFactory = CreateEncryptionClientFactory(
            logging,
            CreateDefaultEncryptionKeyProvider(),
            NProto::EZP_WRITE_ENCRYPTED_ZEROS);

        auto sessionManager = CreateSessionManager(
            CreateWallClockTimer(),
            scheduler,
            logging,
            CreateMonitoringServiceStub(),
            CreateRequestStatsStub(),
            CreateVolumeStatsStub(),
            serverStats,
            service,
            CreateCellManagerStub(),
            CreateDefaultStorageProvider(service),
            encryptionClientFactory,
            executor,
            TSessionManagerOptions());

        executor->Start();
        Y_DEFER {
            executor->Stop();
        };

        NProto::TStartEndpointRequest request;
        request.SetUnixSocketPath(socketPath);
        request.SetDiskId(diskId);
        request.SetClientId("testClientId");
        request.SetIpcType(ipcType);

        {
            auto future = sessionManager->CreateSession(
                MakeIntrusive<TCallContext>(),
                request);

            auto sessionOrError = future.GetValue(TDuration::Seconds(3));
            UNIT_ASSERT_C(!HasError(sessionOrError), sessionOrError.GetError());
        }

        ui32 expectedCount = 1;
        UNIT_ASSERT_VALUES_EQUAL(expectedCount, serverStatsMountCounter);

        for (size_t i = 0; i < 10; ++i) {
            scheduler->RunAllScheduledTasks();
            ++expectedCount;
            UNIT_ASSERT_VALUES_EQUAL(expectedCount, serverStatsMountCounter);
        }

        {
            auto future = sessionManager->RemoveSession(
                MakeIntrusive<TCallContext>(),
                socketPath,
                request.GetHeaders());
            auto error = future.GetValue(TDuration::Seconds(3));
            UNIT_ASSERT_C(!HasError(error), error);
        }

        for (size_t i = 0; i < 10; ++i) {
            scheduler->RunAllScheduledTasks();
            UNIT_ASSERT_VALUES_EQUAL(expectedCount, serverStatsMountCounter);
        }
    }

    Y_UNIT_TEST(ServerStatsShouldMountVolumeWhenEndpointIsStarted_grpc)
    {
        ServerStatsShouldMountVolumeWhenEndpointIsStarted(NProto::IPC_GRPC);
    }

    Y_UNIT_TEST(ServerStatsShouldMountVolumeWhenEndpointIsStarted_nbd)
    {
        ServerStatsShouldMountVolumeWhenEndpointIsStarted(NProto::IPC_NBD);
    }

    Y_UNIT_TEST(ServerStatsShouldMountVolumeWhenEndpointIsStarted_vhost)
    {
        ServerStatsShouldMountVolumeWhenEndpointIsStarted(NProto::IPC_VHOST);
    }

    void ShouldTransformFatalErrorsForTemporaryServer(bool temporaryServer)
    {
        TString socketPath = "testSocket";
        TString diskId = "testDiskId";

        auto service = std::make_shared<TTestService>();
        service->DescribeVolumeHandler =
            [&] (std::shared_ptr<NProto::TDescribeVolumeRequest> request) {
                auto response = NProto::TDescribeVolumeResponse();
                response.MutableVolume()->SetDiskId(request->GetDiskId());
                return MakeFuture(std::move(response));
            };
        service->MountVolumeHandler =
            [&] (std::shared_ptr<NProto::TMountVolumeRequest> request) {
                NProto::TMountVolumeResponse response;
                response.MutableVolume()->SetDiskId(request->GetDiskId());
                response.SetInactiveClientsTimeout(100);
                return MakeFuture(response);
            };
        service->UnmountVolumeHandler =
            [&] (std::shared_ptr<NProto::TUnmountVolumeRequest> request) {
                Y_UNUSED(request);
                return MakeFuture(NProto::TUnmountVolumeResponse());
            };
        service->ReadBlocksLocalHandler =
            [&] (std::shared_ptr<NProto::TReadBlocksLocalRequest> request) {
                Y_UNUSED(request);

                return MakeFuture<NProto::TReadBlocksLocalResponse>(
                    TErrorResponse(E_ARGUMENT, "Test fatal error"));
            };

        auto executor = TExecutor::Create("TestService");
        auto logging = CreateLoggingService("console");

        TSessionManagerOptions options;
        options.TemporaryServer = temporaryServer;
        options.DisableDurableClient = true;

        auto encryptionClientFactory = CreateEncryptionClientFactory(
            logging,
            CreateDefaultEncryptionKeyProvider(),
            NProto::EZP_WRITE_ENCRYPTED_ZEROS);

        auto sessionManager = CreateSessionManager(
            CreateWallClockTimer(),
            CreateSchedulerStub(),
            logging,
            CreateMonitoringServiceStub(),
            CreateRequestStatsStub(),
            CreateVolumeStatsStub(),
            CreateServerStatsStub(),
            service,
            CreateCellManagerStub(),
            CreateDefaultStorageProvider(service),
            encryptionClientFactory,
            executor,
            options);

        executor->Start();
        Y_DEFER {
            executor->Stop();
        };

        NProto::TStartEndpointRequest request;
        request.SetUnixSocketPath(socketPath);
        request.SetDiskId(diskId);
        request.SetClientId("testClientId");

        auto future = sessionManager->CreateSession(
            MakeIntrusive<TCallContext>(),
            request);

        auto sessionOrError = future.GetValue(TDuration::Seconds(3));
        UNIT_ASSERT_C(!HasError(sessionOrError), sessionOrError.GetError());
        auto session = sessionOrError.GetResult().Session;

        {
            auto future = session->ReadBlocksLocal(
                MakeIntrusive<TCallContext>(),
                std::make_shared<NProto::TReadBlocksLocalRequest>());

            auto response = future.GetValue(TDuration::Seconds(3));
            UNIT_ASSERT_VALUES_EQUAL_C(
                temporaryServer ? E_REJECTED : E_ARGUMENT,
                response.GetError().GetCode(),
                response.GetError().GetMessage());
        }
    }

    Y_UNIT_TEST(ShouldTransformFatalErrorsForTemporaryServer)
    {
        ShouldTransformFatalErrorsForTemporaryServer(true);
    }

    Y_UNIT_TEST(ShouldNotTransformFatalErrorsForNotTemporaryServer)
    {
        ShouldTransformFatalErrorsForTemporaryServer(false);
    }

    void ShouldDisableClientThrottler(bool disableClientThrottler)
    {
        TString socketPath = "testSocket";
        TString diskId = "testDiskId";
        TString clientId = "testClientId";
        TString folderId = "testFolderId";
        TString cloudId = "testCloudId";

        auto service = std::make_shared<TTestService>();
        service->DescribeVolumeHandler =
            [&](std::shared_ptr<NProto::TDescribeVolumeRequest> request)
        {
            auto response = NProto::TDescribeVolumeResponse();
            response.MutableVolume()->SetDiskId(request->GetDiskId());
            return MakeFuture(std::move(response));
        };
        service->MountVolumeHandler =
            [&](std::shared_ptr<NProto::TMountVolumeRequest> request)
        {
            NProto::TMountVolumeResponse response;
            response.MutableVolume()->SetDiskId(request->GetDiskId());
            response.MutableVolume()->SetFolderId(folderId);
            response.MutableVolume()->SetCloudId(cloudId);
            response.SetInactiveClientsTimeout(100);
            return MakeFuture(response);
        };
        service->UnmountVolumeHandler =
            [&](std::shared_ptr<NProto::TUnmountVolumeRequest> request)
        {
            Y_UNUSED(request);
            return MakeFuture(NProto::TUnmountVolumeResponse());
        };
        service->ReadBlocksLocalHandler =
            [&] (std::shared_ptr<NProto::TReadBlocksLocalRequest> request) {
                Y_UNUSED(request);
                return MakeFuture<NProto::TReadBlocksLocalResponse>();
            };

        auto monitoring = CreateMonitoringServiceStub();
        auto timer = CreateCpuCycleTimer();
        auto volumeStats = CreateVolumeStats(
            monitoring,
            {},
            EVolumeStatsType::EServerStats,
            timer);

        auto serverStats = CreateServerStats(
            std::make_shared<TTestDumpable>(),
            std::make_shared<TDiagnosticsConfig>(),
            monitoring,
            CreateProfileLogStub(),
            CreateRequestStatsStub(),
            volumeStats);

        auto scheduler = CreateScheduler(timer);
        scheduler->Start();
        Y_DEFER
        {
            scheduler->Stop();
        };

        auto executor = TExecutor::Create("TestService");
        auto logging = CreateLoggingService("console");
        auto encryptionClientFactory = CreateEncryptionClientFactory(
            logging,
            CreateDefaultEncryptionKeyProvider(),
            NProto::EZP_WRITE_ENCRYPTED_ZEROS);

        TSessionManagerOptions options;
        options.DisableClientThrottler = disableClientThrottler;
        options.DisableDurableClient = true;
        auto sessionManager = CreateSessionManager(
            timer,
            scheduler,
            logging,
            monitoring,
            CreateRequestStatsStub(),
            volumeStats,
            serverStats,
            service,
            CreateCellManagerStub(),
            CreateDefaultStorageProvider(service),
            encryptionClientFactory,
            executor,
            options);

        executor->Start();
        Y_DEFER
        {
            executor->Stop();
        };

        NProto::TStartEndpointRequest request;
        request.SetUnixSocketPath(socketPath);
        request.SetDiskId(diskId);
        request.SetClientId(clientId);

        // Set client profile to something that would normally trigger
        // throttling
        auto* clientProfile = request.MutableClientProfile();
        clientProfile->SetCpuUnitCount(1);

        auto* perfProfile =
            request.MutableClientPerformanceProfile()->MutableHDDProfile();
        perfProfile->SetMaxReadIops(1);
        perfProfile->SetMaxWriteIops(1);
        perfProfile->SetMaxReadBandwidth(100_MB);
        perfProfile->SetMaxWriteBandwidth(100_MB);

        auto future = sessionManager->CreateSession(
            MakeIntrusive<TCallContext>(),
            request);

        const auto& sessionOrError = future.GetValue(TDuration::Max());
        UNIT_ASSERT_C(!HasError(sessionOrError), sessionOrError.GetError());
        auto session = sessionOrError.GetResult().Session;

        TVector<TFuture<NProto::TReadBlocksLocalResponse>> futures;
        for (int i = 0; i < 3; i++) {
            auto readRequest =
                std::make_shared<NProto::TReadBlocksLocalRequest>();
            readRequest->SetDiskId(diskId);
            readRequest->SetBlocksCount(1);
            readRequest->SetStartIndex(i * 100);
            *readRequest->MutableHeaders()->MutableClientId() = clientId;
            futures.push_back(session->ReadBlocksLocal(
                MakeIntrusive<TCallContext>(),
                std::move(readRequest)));
        }
        WaitAll(futures).Wait();

        auto counters = monitoring->GetCounters();
        auto diskCounters = counters->GetSubgroup("counters", "blockstore")
                                ->GetSubgroup("component", "server_volume")
                                ->GetSubgroup("host", "cluster")
                                ->GetSubgroup("volume", diskId)
                                ->GetSubgroup("instance", clientId)
                                ->GetSubgroup("cloud", cloudId)
                                ->GetSubgroup("folder", folderId);

        auto postponedCount = diskCounters->GetSubgroup("request", "ReadBlocks")
                                 ->FindCounter("PostponedCount")
                                 ->Val();
        if (disableClientThrottler) {
            UNIT_ASSERT_VALUES_EQUAL(0, postponedCount);
        } else {
            UNIT_ASSERT_VALUES_EQUAL(2, postponedCount);
        }
    }

    Y_UNIT_TEST(ShouldNotThrottleWhenDisableClientThrottlerIsTrue)
    {
        ShouldDisableClientThrottler(true);
    }

    Y_UNIT_TEST(ShouldThrottleWhenDisableClientThrottlerIsFalse)
    {
        ShouldDisableClientThrottler(false);
    }

    Y_UNIT_TEST(ShouldSwitchSessionByMountResponseWithPrincipalDiskId)
    {
        TString socketPath = "testSocket";
        TString diskId = "testDiskId";

        auto timer = CreateCpuCycleTimer();
        auto scheduler = CreateScheduler(timer);
        scheduler->Start();

        auto service = std::make_shared<TTestService>();
        service->DescribeVolumeHandler =
            [&] (std::shared_ptr<NProto::TDescribeVolumeRequest> request) {
                auto response = NProto::TDescribeVolumeResponse();
                response.MutableVolume()->SetDiskId(request->GetDiskId());
                return MakeFuture(std::move(response));
            };
        // Setting up the handler for the mount request, which will add the
        // PrincipalDiskId when mounting testDiskId, which will lead to switch
        // session.
        service->MountVolumeHandler =
            [&] (std::shared_ptr<NProto::TMountVolumeRequest> request) {
                NProto::TMountVolumeResponse response;
                response.MutableVolume()->SetDiskId(request->GetDiskId());
                if (request->GetDiskId() == diskId) {
                    // Response with filled PrincipalDiskId will switch session.
                    response.MutableVolume()->SetPrincipalDiskId(
                        NStorage::GetNextDiskId(diskId));
                }
                response.SetInactiveClientsTimeout(100);
                return MakeFuture(response);
            };
        service->UnmountVolumeHandler =
            [&] (std::shared_ptr<NProto::TUnmountVolumeRequest> request) {
                Y_UNUSED(request);
                return MakeFuture(NProto::TUnmountVolumeResponse());
            };

        // Setting up the handler to respond E_REJECTED if the request handled
        // by testDiskId and S_OK if the request handled by disk testDiskId-copy
        service->ReadBlocksLocalHandler =
            [&](std::shared_ptr<NProto::TReadBlocksLocalRequest> request)
            -> TFuture<NProto::TReadBlocksLocalResponse>
        {
            if (request->GetDiskId() == diskId) {
                return MakeFuture<NProto::TReadBlocksLocalResponse>(
                    TErrorResponse(E_REJECTED));
            }
            if (request->GetDiskId() == NStorage::GetNextDiskId(diskId)) {
                return MakeFuture<NProto::TReadBlocksLocalResponse>(
                    TErrorResponse(S_OK));
            }
            return MakeFuture<NProto::TReadBlocksLocalResponse>(
                TErrorResponse(E_ARGUMENT, "Unexpected disk id"));
        };

        auto executor = TExecutor::Create("TestService");
        auto logging = CreateLoggingService("console");
        auto encryptionClientFactory = CreateEncryptionClientFactory(
            logging,
            CreateDefaultEncryptionKeyProvider(),
            NProto::EZP_WRITE_ENCRYPTED_ZEROS);

        auto sessionManager = CreateSessionManager(
            CreateWallClockTimer(),
            scheduler,
            logging,
            CreateMonitoringServiceStub(),
            CreateRequestStatsStub(),
            CreateVolumeStatsStub(),
            CreateServerStatsStub(),
            service,
            CreateCellManagerStub(),
            CreateDefaultStorageProvider(service),
            encryptionClientFactory,
            executor,
            TSessionManagerOptions());


        executor->Start();
        Y_DEFER {
            executor->Stop();
        };

        NProto::TStartEndpointRequest request;
        request.SetUnixSocketPath(socketPath);
        request.SetDiskId(diskId);
        request.SetClientId("testClientId");
        request.SetIpcType(NProto::IPC_VHOST);

        // Create session to testDiskId
        NClient::ISessionPtr session;
        {
            auto future = sessionManager->CreateSession(
                MakeIntrusive<TCallContext>(),
                request);

            auto sessionOrError = future.GetValue(TDuration::Seconds(3));
            UNIT_ASSERT_C(!HasError(sessionOrError), sessionOrError.GetError());
            auto sessionInfo = sessionOrError.ExtractResult();
            session = sessionInfo.Session;
        }

        // Switch from testDiskId to testDiskId-copy.

        {
            // Make sure that the read response processed by testDiskId-copy
            // that responds with S_OK.
            auto future = session->ReadBlocksLocal(
                MakeIntrusive<TCallContext>(),
                std::make_shared<NProto::TReadBlocksLocalRequest>());
            auto response = future.GetValue(TDuration::Seconds(1));
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                response.GetError().GetCode(),
                FormatError(response.GetError()));
        }

        // Switch from testDiskId-copy to testDiskId.

        // Setting up the handler for the mount request, which will add the
        // PrincipalDiskId when mounting testDiskId-copy, which will lead to
        // switch session back to testDiskId.
        service->MountVolumeHandler =
            [&](std::shared_ptr<NProto::TMountVolumeRequest> request)
        {
            NProto::TMountVolumeResponse response;
            response.MutableVolume()->SetDiskId(request->GetDiskId());
            if (request->GetDiskId() == NStorage::GetNextDiskId(diskId)) {
                // Response with filled PrincipalDiskId will switch session.
                response.MutableVolume()->SetPrincipalDiskId(diskId);
            }
            response.SetInactiveClientsTimeout(100);
            return MakeFuture(response);
        };

        // Setting up the handler to respond E_REJECTED if the request handled
        // by testDiskId and S_OK if the request handled by disk testDiskId-copy
        service->ReadBlocksLocalHandler =
            [&](std::shared_ptr<NProto::TReadBlocksLocalRequest> request)
            -> TFuture<NProto::TReadBlocksLocalResponse>
        {
            if (request->GetDiskId() == diskId) {
                return MakeFuture<NProto::TReadBlocksLocalResponse>(
                    TErrorResponse(S_OK));
            }
            if (request->GetDiskId() == NStorage::GetNextDiskId(diskId)) {
                return MakeFuture<NProto::TReadBlocksLocalResponse>(
                    TErrorResponse(E_REJECTED));
            }
            return MakeFuture<NProto::TReadBlocksLocalResponse>(
                TErrorResponse(E_ARGUMENT, "Unexpected disk id"));
        };

        {
            // Make sure that the read response processed by testDiskId-copy
            // that responds with S_OK.
            auto future = session->ReadBlocksLocal(
                MakeIntrusive<TCallContext>(),
                std::make_shared<NProto::TReadBlocksLocalRequest>());
            auto response = future.GetValue(TDuration::Seconds(1));
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                response.GetError().GetCode(),
                FormatError(response.GetError()));
        }
    }
}

}   // namespace NCloud::NBlockStore::NServer
