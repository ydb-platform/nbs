#include "server.h"

#include "vhost_test.h"

#include <cloud/blockstore/libs/diagnostics/critical_events.h>
#include <cloud/blockstore/libs/diagnostics/critical_events_init.h>
#include <cloud/blockstore/libs/diagnostics/server_stats_test.h>
#include <cloud/blockstore/libs/diagnostics/volume_stats_test.h>
#include <cloud/blockstore/libs/service/device_handler.h>
#include <cloud/blockstore/libs/service/storage_test.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/sglist_test.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/folder/path.h>
#include <util/generic/guid.h>
#include <util/generic/scope.h>
#include <util/system/tempfile.h>
#include <util/thread/factory.h>
#include <util/thread/lfqueue.h>

#include <atomic>

namespace NCloud::NBlockStore::NVhost {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TTestRequest
{
    EBlockStoreRequest Type = EBlockStoreRequest::ReadBlocks;
    ui64 StartIndex = 0;
    ui64 BlocksCount = 0;
    TSgList SgList;
};

////////////////////////////////////////////////////////////////////////////////

class TTestEnvironment
{
private:
    const size_t ThreadsCount = 2;

    const TFsPath SocketPath = TFsPath(CreateGuidAsString() + ".sock");
    const ui32 VhostQueuesCount = 1;
    const ui32 BlockSize;
    const ui64 BlocksCount = 256;
    const bool DropDiscardRequests;

    IServerPtr VhostServer;
    std::shared_ptr<TTestStorage> TestStorage;
    std::shared_ptr<ITestVhostDevice> VhostDevice;
    std::shared_ptr<TTestVhostQueueFactory> VhostQueueFactory;
    TLockFreeQueue<TTestRequest> RequestQueue;

    std::atomic_flag ServiceFrozen = false;
    TLockFreeQueue<TPromise<void>> FrozenPromises;

public:
    TTestEnvironment(ui32 blockSize, bool dropDiscardRequests = false)
        : BlockSize(blockSize)
        , DropDiscardRequests(dropDiscardRequests)
    {
        InitVhostDeviceEnvironment();
    }

    ~TTestEnvironment()
    {
        UninitVhostDeviceEnvironment();
    }

    void StopVhostServer()
    {
        VhostServer->Stop();
        VhostServer.reset();
    }

    std::shared_ptr<ITestVhostDevice> GetVhostDevice()
    {
        return VhostDevice;
    }

    TTestVhostQueueFactory& GetVhostQueueFactory()
    {
        return *VhostQueueFactory;
    }

    bool DequeueRequest(TTestRequest& request)
    {
        return RequestQueue.Dequeue(&request);
    }

    void FreezeService(bool freeze)
    {
        if (freeze) {
            ServiceFrozen.test_and_set();
        } else {
            ServiceFrozen.clear();
        }

        if (!freeze) {
            TPromise<void> promise;
            while (FrozenPromises.Dequeue(&promise)) {
                promise.SetValue();
            }
        }
    }

private:
    void InitVhostDeviceEnvironment()
    {
        TestStorage = std::make_shared<TTestStorage>();
        TestStorage->WriteBlocksLocalHandler =
            [&] (TCallContextPtr ctx, std::shared_ptr<NProto::TWriteBlocksLocalRequest> request) {
                Y_UNUSED(ctx);

                auto guard = request->Sglist.Acquire();
                UNIT_ASSERT(guard);
                auto sglist = guard.Get();
                UNIT_ASSERT(request->BlocksCount * BlockSize == SgListGetSize(sglist));

                RequestQueue.Enqueue({
                    EBlockStoreRequest::WriteBlocks,
                    request->GetStartIndex(),
                    request->BlocksCount,
                    std::move(sglist)});

                if (ServiceFrozen.test()) {
                    auto promise = NewPromise<void>();
                    auto future = promise.GetFuture();
                    FrozenPromises.Enqueue(std::move(promise));
                    return future.Apply([=] (const auto& future) {
                        Y_UNUSED(future);
                        return NProto::TWriteBlocksLocalResponse();
                    });
                }

                return MakeFuture(NProto::TWriteBlocksLocalResponse());
            };
        TestStorage->ReadBlocksLocalHandler =
            [&] (TCallContextPtr ctx, std::shared_ptr<NProto::TReadBlocksLocalRequest> request) {
                Y_UNUSED(ctx);

                auto guard = request->Sglist.Acquire();
                UNIT_ASSERT(guard);
                auto sglist = guard.Get();
                UNIT_ASSERT(request->GetBlocksCount() * BlockSize == SgListGetSize(sglist));

                RequestQueue.Enqueue({
                    EBlockStoreRequest::ReadBlocks,
                    request->GetStartIndex(),
                    request->GetBlocksCount(),
                    std::move(sglist)});

                if (ServiceFrozen.test()) {
                    auto promise = NewPromise<void>();
                    auto future = promise.GetFuture();
                    FrozenPromises.Enqueue(std::move(promise));
                    return future.Apply([=] (const auto& future) {
                        Y_UNUSED(future);
                        return NProto::TReadBlocksLocalResponse();
                    });
                }

                return MakeFuture(NProto::TReadBlocksLocalResponse());
            };

        TestStorage->ZeroBlocksHandler =
            [&](TCallContextPtr ctx,
                std::shared_ptr<NProto::TZeroBlocksRequest> request)
        {
            Y_UNUSED(ctx);

            RequestQueue.Enqueue(
                {EBlockStoreRequest::ZeroBlocks,
                 request->GetStartIndex(),
                 request->GetBlocksCount(),
                 {}});

            if (ServiceFrozen.test()) {
                auto promise = NewPromise<void>();
                auto future = promise.GetFuture();
                FrozenPromises.Enqueue(std::move(promise));
                return future.Apply(
                    [=](const auto& future)
                    {
                        Y_UNUSED(future);
                        return NProto::TZeroBlocksResponse();
                    });
            }

            return MakeFuture(NProto::TZeroBlocksResponse());
        };

        VhostQueueFactory = std::make_shared<TTestVhostQueueFactory>();

        TServerConfig serverConfig;
        serverConfig.ThreadsCount = ThreadsCount;

        VhostServer = CreateServer(
            CreateLoggingService("console"),
            CreateServerStatsStub(),
            VhostQueueFactory,
            CreateDefaultDeviceHandlerFactory(),
            serverConfig,
            TVhostCallbacks());

        VhostServer->Start();
        Sleep(TDuration::MilliSeconds(300));
        UNIT_ASSERT(VhostQueueFactory->Queues.size() == ThreadsCount);
        auto firstQueue = VhostQueueFactory->Queues.at(0);
        UNIT_ASSERT(firstQueue->IsRun());

        {
            TStorageOptions options;
            options.DiskId = "TestDiskId";
            options.BlockSize = BlockSize;
            options.BlocksCount = BlocksCount;
            options.VhostQueuesCount = VhostQueuesCount;
            options.UnalignedRequestsDisabled = false;
            options.OptimalIoSize = 4_MB;
            options.DropDiscardRequests = DropDiscardRequests;

            auto future = VhostServer->StartEndpoint(
                SocketPath.GetPath(),
                TestStorage,
                options);
            const auto& error = future.GetValue(TDuration::Seconds(5));
            UNIT_ASSERT_C(!HasError(error), error);
        }
        UNIT_ASSERT(firstQueue->GetDevices().size() == 1);
        VhostDevice = firstQueue->GetDevices().at(0);
        UNIT_ASSERT_VALUES_EQUAL(4_MB, VhostDevice->GetOptimalIoSize());
    }

    void UninitVhostDeviceEnvironment()
    {
        if (VhostServer) {
            auto future = VhostServer->StopEndpoint(SocketPath.GetPath());
            const auto& error = future.GetValue(TDuration::Seconds(5));
            UNIT_ASSERT_C(!HasError(error), error);
        }

        Sleep(TDuration::MilliSeconds(300));
        UNIT_ASSERT(VhostDevice->IsStopped());

        if (VhostServer) {
            VhostServer->Stop();
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

// Starts a single endpoint on a server with |threadPoolSize| executors and
// returns the number of executors that serve it, i.e. the number of request
// queues the endpoint's vhost device got registered in.
ui32 StartEndpointAndCountExecutors(
    ui32 threadPoolSize,
    ui32 vhostQueuesCount,
    ui32 threadCount)
{
    const TString unixSocketPath = "testSocket";
    TTempFile tempFile(unixSocketPath);

    auto queueFactory = std::make_shared<TTestVhostQueueFactory>();

    TServerConfig serverConfig;
    serverConfig.ThreadsCount = threadPoolSize;

    auto server = CreateServer(
        CreateLoggingService("console"),
        std::make_shared<TTestServerStats>(),
        queueFactory,
        CreateDefaultDeviceHandlerFactory(),
        serverConfig,
        TVhostCallbacks());

    server->Start();
    Y_DEFER {
        server->Stop();
    };

    UNIT_ASSERT_VALUES_EQUAL(threadPoolSize, queueFactory->Queues.size());

    // Every executor thread has to reach its queue before the server is
    // stopped - the test queue does not allow stopping a queue that was never
    // run.
    const auto deadline = TInstant::Now() + TDuration::Seconds(5);
    for (const auto& queue: queueFactory->Queues) {
        while (!queue->IsRun() && TInstant::Now() < deadline) {
            Sleep(TDuration::MilliSeconds(10));
        }
        UNIT_ASSERT(queue->IsRun());
    }

    TStorageOptions options;
    options.DiskId = "testDiskId";
    options.BlockSize = 4096;
    options.BlocksCount = 256;
    options.VhostQueuesCount = vhostQueuesCount;
    options.ThreadCount = threadCount;

    auto future = server->StartEndpoint(
        unixSocketPath,
        std::make_shared<TTestStorage>(),
        options);
    const auto& error = future.GetValue(TDuration::Seconds(5));
    UNIT_ASSERT_C(!HasError(error), error);

    ui32 executorsCount = 0;
    for (const auto& queue: queueFactory->Queues) {
        executorsCount += queue->GetDevices().size();
    }
    return executorsCount;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TServerTest)
{
    Y_UNIT_TEST(ShouldStartStopVhostEndpoint)
    {
        auto logging = CreateLoggingService("console");
        InitVhostLog(logging);

        auto vhostQueueFactory = CreateVhostQueueFactory();

        auto vhostServer = CreateServer(
            logging,
            CreateServerStatsStub(),
            vhostQueueFactory,
            CreateDefaultDeviceHandlerFactory(),
            TServerConfig(),
            TVhostCallbacks());

        vhostServer->Start();

        const TFsPath socket(CreateGuidAsString() + ".sock");

        {
            TStorageOptions options;
            options.DiskId = "TestDiskId";
            options.BlockSize = DefaultBlockSize;
            options.BlocksCount = 42;
            options.VhostQueuesCount = 1;
            options.UnalignedRequestsDisabled = false;

            auto future = vhostServer->StartEndpoint(
                socket.GetPath(),
                std::make_shared<TTestStorage>(),
                options);
            const auto& error = future.GetValue(TDuration::Seconds(5));
            UNIT_ASSERT_C(!HasError(error), error);
        }

        UNIT_ASSERT(socket.Exists());

        {
            auto future = vhostServer->StopEndpoint(socket.GetPath());
            const auto& error = future.GetValue(TDuration::Seconds(5));
            UNIT_ASSERT_C(!HasError(error), error);
        }

        vhostServer->Stop();
    }

    Y_UNIT_TEST(ShouldStopVhostServerWithStartedEndpoints)
    {
        auto logging = CreateLoggingService("console");
        InitVhostLog(logging);

        auto vhostQueueFactory = CreateVhostQueueFactory();

        auto vhostServer = CreateServer(
            logging,
            CreateServerStatsStub(),
            vhostQueueFactory,
            CreateDefaultDeviceHandlerFactory(),
            TServerConfig(),
            TVhostCallbacks());

        vhostServer->Start();

        TStorageOptions options;
        options.DiskId = "TestDiskId";
        options.BlockSize = DefaultBlockSize;
        options.BlocksCount = 42;
        options.VhostQueuesCount = 1;
        options.UnalignedRequestsDisabled = false;

        const size_t endpointCount = 8;
        TString sockets[endpointCount];

        for (size_t i = 0; i < endpointCount; ++i) {
            char ch = '0' + i;
            sockets[i] = CreateGuidAsString() + ch + ".sock";

            auto future = vhostServer->StartEndpoint(
                sockets[i],
                std::make_shared<TTestStorage>(),
                options);
            const auto& error = future.GetValue(TDuration::Seconds(5));
            UNIT_ASSERT_C(!HasError(error), error);
            UNIT_ASSERT(TFsPath(sockets[i]).Exists());
        }

        vhostServer->Stop();
    }

    Y_UNIT_TEST(ShouldHandleVhostReadWriteRequests)
    {
        const ui32 blockSize = 4096;
        const ui64 firstSector = 8;
        const ui64 totalSectors = 32;
        const ui64 sectorSize = 512;

        UNIT_ASSERT(totalSectors * sectorSize % blockSize == 0);

        auto environment = TTestEnvironment(blockSize);
        auto device = environment.GetVhostDevice();

        TVector<TString> blocks;
        auto sgList = ResizeBlocks(
            blocks,
            totalSectors * sectorSize / blockSize,
            TString(blockSize, 'f'));

        {
            auto future = device->SendTestRequest(
                EBlockStoreRequest::WriteBlocks,
                firstSector * sectorSize,
                totalSectors * sectorSize,
                sgList);
            const auto& response = future.GetValue(TDuration::Seconds(5));
            UNIT_ASSERT(response == TVhostRequest::SUCCESS);

            TTestRequest request;
            bool res = environment.DequeueRequest(request);
            UNIT_ASSERT(res);
            UNIT_ASSERT(request.Type == EBlockStoreRequest::WriteBlocks);
            UNIT_ASSERT(request.StartIndex * blockSize == firstSector * sectorSize);
            UNIT_ASSERT(request.BlocksCount * blockSize == totalSectors * sectorSize);
            UNIT_ASSERT_VALUES_EQUAL(request.SgList, sgList);
            UNIT_ASSERT(!environment.DequeueRequest(request));
        }

        {
            auto future = device->SendTestRequest(
                EBlockStoreRequest::ReadBlocks,
                firstSector * sectorSize,
                totalSectors * sectorSize,
                sgList);
            const auto& response = future.GetValue(TDuration::Seconds(5));
            UNIT_ASSERT(response == TVhostRequest::SUCCESS);

            TTestRequest request;
            bool res = environment.DequeueRequest(request);
            UNIT_ASSERT(res);
            UNIT_ASSERT(request.Type == EBlockStoreRequest::ReadBlocks);
            UNIT_ASSERT(request.StartIndex * blockSize == firstSector * sectorSize);
            UNIT_ASSERT(request.BlocksCount * blockSize == totalSectors * sectorSize);
            UNIT_ASSERT_VALUES_EQUAL(request.SgList, sgList);
            UNIT_ASSERT(!environment.DequeueRequest(request));
        }
    }

    Y_UNIT_TEST(ShouldThrowCriticalEventIfFailedRequestQueueRunning)
    {
        NMonitoring::TDynamicCountersPtr counters = new NMonitoring::TDynamicCounters();
        InitCriticalEventsCounter(counters);
        auto configCounter =
            counters->GetCounter("AppCriticalEvents/VhostQueueRunningError", true);

        auto environment = TTestEnvironment(DefaultBlockSize);

        UNIT_ASSERT_VALUES_EQUAL(0, static_cast<int>(*configCounter));

        auto& factory = environment.GetVhostQueueFactory();
        factory.Queues.at(0)->Break();

        factory.FailedEvent.Reset();
        factory.FailedEvent.WaitT(TDuration::Seconds(1));
        factory.FailedEvent.Reset();
        factory.FailedEvent.WaitT(TDuration::Seconds(1));

        UNIT_ASSERT_VALUES_EQUAL(1, static_cast<int>(*configCounter));
    }

    Y_UNIT_TEST(ShouldGetFatalErrorIfEndpointHasInvalidSocketPath)
    {
        auto logging = CreateLoggingService("console");
        InitVhostLog(logging);

        auto vhostServer = CreateServer(
            logging,
            CreateServerStatsStub(),
            CreateVhostQueueFactory(),
            CreateDefaultDeviceHandlerFactory(),
            TServerConfig(),
            TVhostCallbacks());

        vhostServer->Start();

        TString socketPath("./invalid/path/to/socket");

        TStorageOptions options;
        options.DiskId = "TestDiskId";
        options.BlockSize = DefaultBlockSize;
        options.BlocksCount = 42;
        options.VhostQueuesCount = 1;
        options.UnalignedRequestsDisabled = false;

        auto future = vhostServer->StartEndpoint(
            socketPath,
            std::make_shared<TTestStorage>(),
            options);

        const auto& error = future.GetValue(TDuration::Seconds(5));
        UNIT_ASSERT_VALUES_EQUAL_C(
            EErrorKind::ErrorFatal,
            GetErrorKind(error),
            error);

        vhostServer->Stop();
    }

    Y_UNIT_TEST(ShouldStartEndpointIfSocketAlreadyExists)
    {
        auto logging = CreateLoggingService("console");
        InitVhostLog(logging);

        auto vhostQueueFactory = CreateVhostQueueFactory();

        auto vhostServer = CreateServer(
            logging,
            CreateServerStatsStub(),
            vhostQueueFactory,
            CreateDefaultDeviceHandlerFactory(),
            TServerConfig(),
            TVhostCallbacks());

        vhostServer->Start();

        const TFsPath socket(CreateGuidAsString() + ".sock");
        socket.Touch();
        Y_DEFER {
            socket.DeleteIfExists();
        };

        {
            TStorageOptions options;
            options.DiskId = "TestDiskId";
            options.BlockSize = DefaultBlockSize;
            options.BlocksCount = 42;
            options.VhostQueuesCount = 1;
            options.UnalignedRequestsDisabled = false;

            auto future = vhostServer->StartEndpoint(
                socket.GetPath(),
                std::make_shared<TTestStorage>(),
                options);
            const auto& error = future.GetValue(TDuration::Seconds(5));
            UNIT_ASSERT_C(!HasError(error), error);
        }

        vhostServer->Stop();
    }

    Y_UNIT_TEST(ShouldRemoveUnixSocketAfterStopEndpoint)
    {
        auto logging = CreateLoggingService("console");
        InitVhostLog(logging);

        auto vhostQueueFactory = CreateVhostQueueFactory();

        auto vhostServer = CreateServer(
            logging,
            CreateServerStatsStub(),
            vhostQueueFactory,
            CreateDefaultDeviceHandlerFactory(),
            TServerConfig(),
            TVhostCallbacks());

        vhostServer->Start();

        const TFsPath socket(CreateGuidAsString() + ".sock");

        {
            TStorageOptions options;
            options.DiskId = "TestDiskId";
            options.BlockSize = DefaultBlockSize;
            options.BlocksCount = 42;
            options.VhostQueuesCount = 1;
            options.UnalignedRequestsDisabled = false;

            auto future = vhostServer->StartEndpoint(
                socket.GetPath(),
                std::make_shared<TTestStorage>(),
                options);
            const auto& error = future.GetValue(TDuration::Seconds(5));
            UNIT_ASSERT_C(!HasError(error), error);
        }

        auto future = vhostServer->StopEndpoint(socket.GetPath());
        const auto& error = future.GetValue(TDuration::Seconds(5));
        UNIT_ASSERT_C(!HasError(error), error);
        UNIT_ASSERT(!socket.Exists());

        vhostServer->Stop();
    }

    Y_UNIT_TEST(ShouldNotRemoveUnixSocketAfterStopServer)
    {
        auto logging = CreateLoggingService("console");
        InitVhostLog(logging);

        auto vhostQueueFactory = CreateVhostQueueFactory();

        auto vhostServer = CreateServer(
            logging,
            CreateServerStatsStub(),
            vhostQueueFactory,
            CreateDefaultDeviceHandlerFactory(),
            TServerConfig(),
            TVhostCallbacks());

        vhostServer->Start();

        const TFsPath socket(CreateGuidAsString() + ".sock");

        {
            TStorageOptions options;
            options.DiskId = "TestDiskId";
            options.BlockSize = DefaultBlockSize;
            options.BlocksCount = 42;
            options.VhostQueuesCount = 1;
            options.UnalignedRequestsDisabled = false;

            auto future = vhostServer->StartEndpoint(
                socket.GetPath(),
                std::make_shared<TTestStorage>(),
                options);
            const auto& error = future.GetValue(TDuration::Seconds(5));
            UNIT_ASSERT_C(!HasError(error), error);
        }

        vhostServer->Stop();
        UNIT_ASSERT(socket.Exists());
    }

    Y_UNIT_TEST(ShouldCancelRequestsInFlightWhenStopEndpointOrStopServer)
    {
        TString unixSocketPath =
            MakeTempName(nullptr, CreateGuidAsString().c_str(), "sock");
        const ui32 blockSize = 4096;
        const ui64 startIndex = 3;
        const ui64 blocksCount = 41;

        auto promise = NewPromise<void>();

        auto testStorage = std::make_shared<TTestStorage>();
        testStorage->WriteBlocksLocalHandler =
            [&] (TCallContextPtr ctx, std::shared_ptr<NProto::TWriteBlocksLocalRequest> request) {
                Y_UNUSED(ctx);
                Y_UNUSED(request);
                return promise.GetFuture().Apply([] (const auto& f) {
                    Y_UNUSED(f);
                    return NProto::TWriteBlocksLocalResponse();
                });
            };
        testStorage->ReadBlocksLocalHandler =
            [&] (TCallContextPtr ctx, std::shared_ptr<NProto::TReadBlocksLocalRequest> request) {
                Y_UNUSED(ctx);
                Y_UNUSED(request);
                return promise.GetFuture().Apply([] (const auto& f) {
                    Y_UNUSED(f);
                    return NProto::TReadBlocksLocalResponse();
                });
            };

        auto queueFactory = std::make_shared<TTestVhostQueueFactory>();

        TServerConfig serverConfig;
        serverConfig.ThreadsCount = 2;

        size_t fatalErrorCount = 0;
        auto serverStats = std::make_shared<TTestServerStats>();
        serverStats->RequestCompletedHandler = [&] (
            TLog& log,
            TMetricRequest& metricRequest,
            TCallContext& callContext,
            const NProto::TError& error)
        {
            Y_UNUSED(log);
            Y_UNUSED(metricRequest);
            Y_UNUSED(callContext);
            if (GetDiagnosticsErrorKind(error)
                    == EDiagnosticsErrorKind::ErrorFatal)
            {
                ++fatalErrorCount;
            }
        };

        auto server = CreateServer(
            CreateLoggingService("console"),
            serverStats,
            queueFactory,
            CreateDefaultDeviceHandlerFactory(),
            serverConfig,
            TVhostCallbacks());

        server->Start();
        Sleep(TDuration::MilliSeconds(300));
        UNIT_ASSERT(queueFactory->Queues.size() == serverConfig.ThreadsCount);
        auto firstQueue = queueFactory->Queues.at(0);
        UNIT_ASSERT(firstQueue->IsRun());

        TStorageOptions options;
        options.DiskId = "testDiskId";
        options.BlockSize = blockSize;
        options.BlocksCount = 256;
        options.VhostQueuesCount = 1;
        options.UnalignedRequestsDisabled = false;

        {
            auto future = server->StartEndpoint(
                unixSocketPath,
                testStorage,
                options);
            const auto& error = future.GetValue(TDuration::Seconds(5));
            UNIT_ASSERT_C(!HasError(error), error);
        }
        UNIT_ASSERT(firstQueue->GetDevices().size() == 1);
        auto device = firstQueue->GetDevices().at(0);

        TVector<TString> blocks;
        auto sgList = ResizeBlocks(
            blocks,
            blocksCount,
            TString(blockSize, 'f'));

        auto writeFuture = device->SendTestRequest(
            EBlockStoreRequest::WriteBlocks,
            startIndex * blockSize,
            blocksCount * blockSize,
            sgList);

        auto readFuture = device->SendTestRequest(
            EBlockStoreRequest::ReadBlocks,
            startIndex * blockSize,
            blocksCount * blockSize,
            sgList);

        Sleep(TDuration::MilliSeconds(300));
        UNIT_ASSERT(!writeFuture.HasValue());
        UNIT_ASSERT(!readFuture.HasValue());
        UNIT_ASSERT_VALUES_EQUAL(0, fatalErrorCount);

        {
            device->DisableAutostop(true);
            auto future = server->StopEndpoint(unixSocketPath);

            for (size_t i = 0; i < 5; ++i) {
                auto type = (i % 2 == 0)
                    ? EBlockStoreRequest::WriteBlocks
                    : EBlockStoreRequest::ReadBlocks;
                auto reqFuture = device->SendTestRequest(
                    type,
                    startIndex * blockSize,
                    blocksCount * blockSize,
                    sgList);
                auto response = reqFuture.GetValue(TDuration::Seconds(5));
                UNIT_ASSERT(response == TVhostRequest::CANCELLED);
            }

            UNIT_ASSERT(!future.HasValue());
            device->DisableAutostop(false);
            auto response = future.GetValue(TDuration::Seconds(5));
            UNIT_ASSERT(!HasError(response));
        }

        auto writeResponse = writeFuture.GetValue(TDuration::Seconds(5));
        UNIT_ASSERT(writeResponse == TVhostRequest::CANCELLED);
        auto readResponse = readFuture.GetValue(TDuration::Seconds(5));
        UNIT_ASSERT(readResponse == TVhostRequest::CANCELLED);
        UNIT_ASSERT_VALUES_EQUAL(0, fatalErrorCount);

        {
            auto future = server->StartEndpoint(
                unixSocketPath,
                testStorage,
                options);
            const auto& error = future.GetValue(TDuration::Seconds(5));
            UNIT_ASSERT_C(!HasError(error), error);
        }
        device.reset();
        UNIT_ASSERT(firstQueue->GetDevices().size() == 1);
        device = firstQueue->GetDevices().at(0);

        writeFuture = device->SendTestRequest(
            EBlockStoreRequest::WriteBlocks,
            startIndex * blockSize,
            blocksCount * blockSize,
            sgList);

        readFuture = device->SendTestRequest(
            EBlockStoreRequest::ReadBlocks,
            startIndex * blockSize,
            blocksCount * blockSize,
            sgList);

        Sleep(TDuration::MilliSeconds(300));
        UNIT_ASSERT(!writeFuture.HasValue());
        UNIT_ASSERT(!readFuture.HasValue());
        UNIT_ASSERT_VALUES_EQUAL(0, fatalErrorCount);

        device->DisableAutostop(true);

        TManualEvent startEvent;
        TManualEvent stopEvent;
        SystemThreadFactory()->Run([&]() {
            startEvent.Signal();
            server->Stop();
            stopEvent.Signal();
        });
        startEvent.Wait();

        for (size_t i = 0; i < 5; ++i) {
            auto type = (i % 2 == 0)
                ? EBlockStoreRequest::WriteBlocks
                : EBlockStoreRequest::ReadBlocks;
            auto reqFuture = device->SendTestRequest(
                type,
                startIndex * blockSize,
                blocksCount * blockSize,
                sgList);
            auto response = reqFuture.GetValue(TDuration::Seconds(5));
            UNIT_ASSERT(response == TVhostRequest::CANCELLED);
        }
        device->DisableAutostop(false);

        writeResponse = writeFuture.GetValue(TDuration::Seconds(5));
        UNIT_ASSERT(writeResponse == TVhostRequest::CANCELLED);
        readResponse = readFuture.GetValue(TDuration::Seconds(5));
        UNIT_ASSERT(readResponse == TVhostRequest::CANCELLED);
        UNIT_ASSERT_VALUES_EQUAL(0, fatalErrorCount);

        stopEvent.Wait();
    }

    Y_UNIT_TEST(ShouldWaitForStoppingEndpointBeforeShuttingDownExecutors)
    {
        const TString unixSocketPath = CreateGuidAsString() + ".sock";

        auto queueFactory = std::make_shared<TTestVhostQueueFactory>();

        auto server = CreateServer(
            CreateLoggingService("console"),
            CreateServerStatsStub(),
            queueFactory,
            CreateDefaultDeviceHandlerFactory(),
            TServerConfig(),
            TVhostCallbacks());

        server->Start();

        const auto deadline = TInstant::Now() + TDuration::Seconds(5);
        while (!queueFactory->Queues.at(0)->IsRun() &&
               TInstant::Now() < deadline)
        {
            Sleep(TDuration::MilliSeconds(10));
        }
        UNIT_ASSERT(queueFactory->Queues.at(0)->IsRun());

        TStorageOptions options;
        options.DiskId = "testDiskId";
        options.BlockSize = 4096;
        options.BlocksCount = 256;
        options.VhostQueuesCount = 1;

        {
            auto future = server->StartEndpoint(
                unixSocketPath,
                std::make_shared<TTestStorage>(),
                options);
            const auto& error = future.GetValue(TDuration::Seconds(5));
            UNIT_ASSERT_C(!HasError(error), error);
        }

        auto device = queueFactory->Queues.at(0)->GetDevices().at(0);
        device->DisableAutostop(true);

        auto stopEndpointFuture = server->StopEndpoint(unixSocketPath);
        UNIT_ASSERT(!stopEndpointFuture.HasValue());

        TManualEvent stopStarted;
        TManualEvent stopCompleted;
        auto stopThread = SystemThreadFactory()->Run([&] {
            stopStarted.Signal();
            server->Stop();
            stopCompleted.Signal();
        });

        stopStarted.Wait();
        UNIT_ASSERT(!stopCompleted.WaitT(TDuration::MilliSeconds(100)));

        device->DisableAutostop(false);

        const auto& error =
            stopEndpointFuture.GetValue(TDuration::Seconds(5));
        UNIT_ASSERT_C(!HasError(error), error);
        UNIT_ASSERT(stopCompleted.WaitT(TDuration::Seconds(5)));
        stopThread->Join();
    }

    Y_UNIT_TEST(ShouldReleaseExecutorAssignmentsBeforeRequestCallbackReturns)
    {
        const TString unixSocketPath = CreateGuidAsString() + ".sock";

        TManualEvent requestStarted;
        TManualEvent completionPaused;
        TManualEvent resumeCompletion;
        auto storagePromise = NewPromise<void>();

        auto storage = std::make_shared<TTestStorage>();
        storage->ReadBlocksLocalHandler =
            [&](TCallContextPtr ctx,
                std::shared_ptr<NProto::TReadBlocksLocalRequest> request)
        {
            Y_UNUSED(ctx);
            Y_UNUSED(request);
            requestStarted.Signal();
            return storagePromise.GetFuture().Apply(
                [](const auto&) { return NProto::TReadBlocksLocalResponse(); });
        };

        auto queueFactory = std::make_shared<TTestVhostQueueFactory>();
        queueFactory->RequestCompletionHandler = [&]
        {
            completionPaused.Signal();
            resumeCompletion.Wait();
        };

        auto server = CreateServer(
            CreateLoggingService("console"),
            CreateServerStatsStub(),
            queueFactory,
            CreateDefaultDeviceHandlerFactory(),
            TServerConfig(),
            TVhostCallbacks());

        server->Start();

        const auto deadline = TInstant::Now() + TDuration::Seconds(5);
        while (!queueFactory->Queues.at(0)->IsRun() &&
               TInstant::Now() < deadline)
        {
            Sleep(TDuration::MilliSeconds(10));
        }
        UNIT_ASSERT(queueFactory->Queues.at(0)->IsRun());

        TStorageOptions options;
        options.DiskId = "testDiskId";
        options.BlockSize = 4096;
        options.BlocksCount = 256;
        options.VhostQueuesCount = 1;

        const auto startError =
            server->StartEndpoint(unixSocketPath, storage, options)
                .GetValue(TDuration::Seconds(5));
        UNIT_ASSERT_C(!HasError(startError), startError);

        auto device = queueFactory->Queues.at(0)->GetDevices().at(0);
        TVector<TString> blocks;
        auto sgList = ResizeBlocks(blocks, 1, TString(options.BlockSize, 'f'));
        auto requestFuture = device->SendTestRequest(
            EBlockStoreRequest::ReadBlocks,
            0,
            options.BlockSize,
            sgList);
        Y_DEFER
        {
            resumeCompletion.Signal();
            storagePromise.TrySetValue();
        };

        UNIT_ASSERT(requestStarted.WaitT(TDuration::Seconds(5)));

        auto completionThread =
            SystemThreadFactory()->Run([&] { storagePromise.SetValue(); });
        Y_DEFER
        {
            resumeCompletion.Signal();
            completionThread->Join();
        };

        UNIT_ASSERT(completionPaused.WaitT(TDuration::Seconds(5)));
        UNIT_ASSERT(requestFuture.HasValue());

        const auto stopError = server->StopEndpoint(unixSocketPath)
                                   .GetValue(TDuration::Seconds(5));
        UNIT_ASSERT_C(!HasError(stopError), stopError);

        // The request continuation still owns the endpoint and is paused
        // before UnregisterRequest(). Executor shutdown must not depend on
        // that endpoint being destroyed.
        server->Stop();
        server.reset();
    }

    Y_UNIT_TEST(ShouldPassCorrectMetrics)
    {
        TString testDiskId = "testDiskId";
        const ui32 blockSize = 4096;
        const ui64 sectorSize = 512;
        ui64 firstSector = 0;
        ui64 totalSectors = 0;

        bool expectedUnaligned = false;
        ui64 expectedStartIndex = 0;
        ui64 expectedBlockCount = 0;

        UNIT_ASSERT(totalSectors * sectorSize % blockSize == 0);

        auto serverStats = std::make_shared<TTestServerStats>();

        ui32 requestCounter = 0;
        ui32 expectedRequestCounter = 0;

        serverStats->PrepareMetricRequestHandler = [&] (
            TMetricRequest& metricRequest,
            TString clientId,
            TString diskId,
            ui64 startIndex,
            ui32 requestBytes,
            bool unaligned)
        {
            Y_UNUSED(clientId);

            UNIT_ASSERT(diskId == testDiskId);
            metricRequest.DiskId = std::move(diskId);

            UNIT_ASSERT_VALUES_EQUAL(expectedUnaligned, unaligned);

            switch (metricRequest.RequestType)
            {
                case EBlockStoreRequest::ReadBlocks:
                case EBlockStoreRequest::WriteBlocks:
                case EBlockStoreRequest::ZeroBlocks:
                    UNIT_ASSERT_VALUES_EQUAL(expectedStartIndex, startIndex);
                    UNIT_ASSERT_VALUES_EQUAL(expectedBlockCount * blockSize, requestBytes);
                    break;
                case EBlockStoreRequest::MountVolume:
                case EBlockStoreRequest::UnmountVolume:
                    break;
                default:
                    UNIT_FAIL("Unexpected request");
                    break;
            }

            ++requestCounter;
        };

        auto testStorage = std::make_shared<TTestStorage>();
        testStorage->WriteBlocksLocalHandler =
            [&] (TCallContextPtr ctx, std::shared_ptr<NProto::TWriteBlocksLocalRequest> request) {
                Y_UNUSED(ctx);
                Y_UNUSED(request);
                return MakeFuture(NProto::TWriteBlocksLocalResponse());
            };
        testStorage->ReadBlocksLocalHandler =
            [&] (TCallContextPtr ctx, std::shared_ptr<NProto::TReadBlocksLocalRequest> request) {
                Y_UNUSED(ctx);
                Y_UNUSED(request);
                return MakeFuture(NProto::TReadBlocksLocalResponse());
            };

        auto queueFactory = std::make_shared<TTestVhostQueueFactory>();

        TServerConfig serverConfig;
        serverConfig.ThreadsCount = 2;

        auto server = CreateServer(
            CreateLoggingService("console"),
            serverStats,
            queueFactory,
            CreateDefaultDeviceHandlerFactory(),
            serverConfig,
            TVhostCallbacks());

        server->Start();
        Sleep(TDuration::MilliSeconds(300));
        UNIT_ASSERT(queueFactory->Queues.size() == serverConfig.ThreadsCount);
        auto firstQueue = queueFactory->Queues.at(0);
        UNIT_ASSERT(firstQueue->IsRun());

        {
            TStorageOptions options;
            options.DiskId = testDiskId;
            options.BlockSize = blockSize;
            options.BlocksCount = 256;
            options.VhostQueuesCount = 1;
            options.UnalignedRequestsDisabled = false;

            auto future = server->StartEndpoint(
                CreateGuidAsString() + ".sock",
                testStorage,
                options);
            const auto& error = future.GetValue(TDuration::Seconds(5));
            UNIT_ASSERT_C(!HasError(error), error);
        }
        UNIT_ASSERT(firstQueue->GetDevices().size() == 1);
        auto device = firstQueue->GetDevices().at(0);

        auto testIoRequets = [&] () {
            TVector<TString> blocks;
            auto sgList = ResizeBlocks(
                blocks,
                totalSectors,
                TString(sectorSize, 'f'));

            {
                auto future = device->SendTestRequest(
                    EBlockStoreRequest::WriteBlocks,
                    firstSector * sectorSize,
                    totalSectors * sectorSize,
                    sgList);
                const auto& response = future.GetValue(TDuration::Seconds(5));
                UNIT_ASSERT(response == TVhostRequest::SUCCESS);
                UNIT_ASSERT_VALUES_EQUAL(++expectedRequestCounter, requestCounter);
            }

            {
                auto future = device->SendTestRequest(
                    EBlockStoreRequest::ReadBlocks,
                    firstSector * sectorSize,
                    totalSectors * sectorSize,
                    sgList);
                const auto& response = future.GetValue(TDuration::Seconds(5));
                UNIT_ASSERT(response == TVhostRequest::SUCCESS);
                UNIT_ASSERT_VALUES_EQUAL(++expectedRequestCounter, requestCounter);
            }
        };

        firstSector = 8;
        totalSectors = 32;
        expectedUnaligned = false;
        expectedStartIndex = 1;
        expectedBlockCount = 4;
        testIoRequets();

        firstSector = 5;
        totalSectors = 16;
        expectedUnaligned = true;
        expectedStartIndex = 0;
        expectedBlockCount = 3;
        testIoRequets();

        firstSector = 16;
        totalSectors = 29;
        expectedUnaligned = true;
        expectedStartIndex = 2;
        expectedBlockCount = 4;
        testIoRequets();

        firstSector = 13;
        totalSectors = 11;
        expectedUnaligned = true;
        expectedStartIndex = 1;
        expectedBlockCount = 2;
        testIoRequets();
    }

    Y_UNIT_TEST(ShouldNotBeRaceOnStopEndpoint)
    {
        TString unixSocketPath = CreateGuidAsString() + ".sock";
        const ui32 blockSize = 4096;
        const ui64 startIndex = 3;
        const ui64 blocksCount = 2;

        TManualEvent handleRequestEvent;
        TManualEvent stopEndpointEvent;

        auto promise = NewPromise<NProto::TWriteBlocksLocalResponse>();

        auto testStorage = std::make_shared<TTestStorage>();
        testStorage->WriteBlocksLocalHandler =
            [&] (TCallContextPtr ctx, std::shared_ptr<NProto::TWriteBlocksLocalRequest> request) {
                Y_UNUSED(ctx);
                Y_UNUSED(request);
                handleRequestEvent.Signal();
                stopEndpointEvent.Wait();
                return promise.GetFuture();
            };

        auto queueFactory = std::make_shared<TTestVhostQueueFactory>();

        TServerConfig serverConfig;
        serverConfig.ThreadsCount = 2;

        auto server = CreateServer(
            CreateLoggingService("console"),
            CreateServerStatsStub(),
            queueFactory,
            CreateDefaultDeviceHandlerFactory(),
            serverConfig,
            TVhostCallbacks());

        server->Start();
        Sleep(TDuration::MilliSeconds(300));
        UNIT_ASSERT(queueFactory->Queues.size() == serverConfig.ThreadsCount);
        auto firstQueue = queueFactory->Queues.at(0);
        UNIT_ASSERT(firstQueue->IsRun());

        TStorageOptions options;
        options.DiskId = "testDiskId";
        options.BlockSize = blockSize;
        options.BlocksCount = 256;
        options.VhostQueuesCount = 1;
        options.UnalignedRequestsDisabled = false;

        {
            auto future = server->StartEndpoint(
                unixSocketPath,
                testStorage,
                options);
            const auto& error = future.GetValue(TDuration::Seconds(5));
            UNIT_ASSERT_C(!HasError(error), error);
        }
        UNIT_ASSERT(firstQueue->GetDevices().size() == 1);
        auto device = firstQueue->GetDevices().at(0);

        auto collector = CreateIncompleteRequestsCollectorStub();
        UNIT_ASSERT_VALUES_EQUAL(0, server->CollectRequests(collector));

        TVector<TString> blocks;
        auto sgList = ResizeBlocks(
            blocks,
            blocksCount,
            TString(blockSize, 'f'));

        auto future1 = device->SendTestRequest(
            EBlockStoreRequest::WriteBlocks,
            startIndex * blockSize,
            blocksCount * blockSize,
            sgList);

        handleRequestEvent.Wait();
        UNIT_ASSERT_VALUES_EQUAL(1, server->CollectRequests(collector));

        auto future2 = device->SendTestRequest(
            EBlockStoreRequest::WriteBlocks,
            startIndex * blockSize,
            blocksCount * blockSize,
            sgList);

        {
            auto future = server->StopEndpoint(unixSocketPath);
            UNIT_ASSERT_VALUES_EQUAL(0, server->CollectRequests(collector));

            stopEndpointEvent.Signal();

            auto response = future.GetValue(TDuration::Seconds(5));
            UNIT_ASSERT(!HasError(response));
        }

        future1.GetValue(TDuration::Seconds(5));
        future2.GetValue(TDuration::Seconds(5));

        server->Stop();
    }

    Y_UNIT_TEST(ShouldHandleVhostZeroBlocksRequests)
    {
        const ui32 blockSize = 4096;
        const ui64 firstSector = 8;
        const ui64 totalSectors = 32;
        const ui64 sectorSize = 512;

        UNIT_ASSERT(totalSectors * sectorSize % blockSize == 0);

        auto environment = TTestEnvironment(blockSize);
        auto device = environment.GetVhostDevice();

        {
            auto future = device->SendTestRequest(
                EBlockStoreRequest::ZeroBlocks,
                firstSector * sectorSize,
                totalSectors * sectorSize,
                {},
                true /* isDiscardRequest */);
            const auto& response = future.GetValue(TDuration::Seconds(5));
            UNIT_ASSERT(response == TVhostRequest::SUCCESS);

            TTestRequest request;
            bool res = environment.DequeueRequest(request);
            UNIT_ASSERT(res);
            UNIT_ASSERT(request.Type == EBlockStoreRequest::ZeroBlocks);
            UNIT_ASSERT(
                request.StartIndex * blockSize == firstSector * sectorSize);
            UNIT_ASSERT(
                request.BlocksCount * blockSize == totalSectors * sectorSize);
            UNIT_ASSERT(!environment.DequeueRequest(request));
        }

        {
            auto future = device->SendTestRequest(
                EBlockStoreRequest::ZeroBlocks,
                firstSector * sectorSize,
                totalSectors * sectorSize,
                {},
                false /* isDiscardRequest */);
            const auto& response = future.GetValue(TDuration::Seconds(5));
            UNIT_ASSERT(response == TVhostRequest::SUCCESS);

            TTestRequest request;
            bool res = environment.DequeueRequest(request);
            UNIT_ASSERT(res);
            UNIT_ASSERT(request.Type == EBlockStoreRequest::ZeroBlocks);
            UNIT_ASSERT(
                request.StartIndex * blockSize == firstSector * sectorSize);
            UNIT_ASSERT(
                request.BlocksCount * blockSize == totalSectors * sectorSize);
            UNIT_ASSERT(!environment.DequeueRequest(request));
        }
    }

    Y_UNIT_TEST(ShouldDropDiscardRequestsIfNeeded)
    {
        const ui32 blockSize = 4096;
        const ui64 firstSector = 8;
        const ui64 totalSectors = 32;
        const ui64 sectorSize = 512;

        UNIT_ASSERT(totalSectors * sectorSize % blockSize == 0);

        auto environment =
            TTestEnvironment(blockSize, true /* dropDiscardRequests */);
        auto device = environment.GetVhostDevice();

        {
            auto future = device->SendTestRequest(
                EBlockStoreRequest::ZeroBlocks,
                firstSector * sectorSize,
                totalSectors * sectorSize,
                {},
                true /* isDiscardRequest */);
            const auto& response = future.GetValue(TDuration::Seconds(5));
            UNIT_ASSERT(response == TVhostRequest::SUCCESS);

            // Should drop the discard request.
            TTestRequest request;
            UNIT_ASSERT(!environment.DequeueRequest(request));
        }

        {
            auto future = device->SendTestRequest(
                EBlockStoreRequest::ZeroBlocks,
                firstSector * sectorSize,
                totalSectors * sectorSize,
                {},
                false /* isDiscardRequest */);
            const auto& response = future.GetValue(TDuration::Seconds(5));
            UNIT_ASSERT(response == TVhostRequest::SUCCESS);

            // This is not a discard request but a write zeroes request.
            // Should not drop it.
            TTestRequest request;
            bool res = environment.DequeueRequest(request);
            UNIT_ASSERT(res);
            UNIT_ASSERT(request.Type == EBlockStoreRequest::ZeroBlocks);
            UNIT_ASSERT(
                request.StartIndex * blockSize == firstSector * sectorSize);
            UNIT_ASSERT(
                request.BlocksCount * blockSize == totalSectors * sectorSize);
            UNIT_ASSERT(!environment.DequeueRequest(request));
        }
    }

    Y_UNIT_TEST(ShouldServeSingleEndpointByMultipleExecutors)
    {
        const ui32 blockSize = 4096;
        const ui32 vhostQueuesCount = 4;
        const TString unixSocketPath = "testSocket";
        TTempFile tempFile(unixSocketPath);

        // Every request blocks its executor thread until all of them have
        // arrived. This can only be completed if the endpoint is served by
        // |vhostQueuesCount| executors simultaneously.
        std::atomic<ui32> arrivedCount = 0;
        TManualEvent allArrived;

        auto testStorage = std::make_shared<TTestStorage>();
        testStorage->ReadBlocksLocalHandler = [&] (
            TCallContextPtr ctx,
            std::shared_ptr<NProto::TReadBlocksLocalRequest> request)
        {
            Y_UNUSED(ctx);
            Y_UNUSED(request);

            if (arrivedCount.fetch_add(1) + 1 == vhostQueuesCount) {
                allArrived.Signal();
            }
            UNIT_ASSERT(allArrived.WaitT(TDuration::Seconds(30)));

            return MakeFuture(NProto::TReadBlocksLocalResponse());
        };

        auto queueFactory = std::make_shared<TTestVhostQueueFactory>();

        TServerConfig serverConfig;
        serverConfig.ThreadsCount = vhostQueuesCount;

        auto server = CreateServer(
            CreateLoggingService("console"),
            std::make_shared<TTestServerStats>(),
            queueFactory,
            CreateDefaultDeviceHandlerFactory(),
            serverConfig,
            TVhostCallbacks());

        server->Start();
        Y_DEFER {
            server->Stop();
        };

        UNIT_ASSERT_VALUES_EQUAL(
            serverConfig.ThreadsCount,
            queueFactory->Queues.size());

        TStorageOptions options;
        options.DiskId = "testDiskId";
        options.BlockSize = blockSize;
        options.BlocksCount = 256;
        options.VhostQueuesCount = vhostQueuesCount;
        options.ThreadCount = vhostQueuesCount;

        {
            auto future = server->StartEndpoint(
                unixSocketPath,
                testStorage,
                options);
            const auto& error = future.GetValue(TDuration::Seconds(5));
            UNIT_ASSERT_C(!HasError(error), error);
        }

        // The device has to be registered in every queue, otherwise its
        // virtqueues would all be served by a single executor.
        for (const auto& queue: queueFactory->Queues) {
            UNIT_ASSERT_VALUES_EQUAL(1, queue->GetDevices().size());
        }
        auto device = queueFactory->Queues.at(0)->GetDevices().at(0);

        TVector<TString> blocks;
        auto sgList = ResizeBlocks(blocks, 1, TString(blockSize, 'f'));

        TVector<TFuture<TVhostRequest::EResult>> futures;
        for (ui32 i = 0; i < vhostQueuesCount; ++i) {
            // Non-overlapping ranges, so that the requests aren't serialized
            // by the device handler.
            futures.push_back(device->SendTestRequest(
                EBlockStoreRequest::ReadBlocks,
                i * blockSize,
                blockSize,
                sgList));
        }

        for (auto& future: futures) {
            UNIT_ASSERT_VALUES_EQUAL(
                static_cast<int>(TVhostRequest::SUCCESS),
                static_cast<int>(future.GetValue(TDuration::Seconds(30))));
        }

        {
            auto future = server->StopEndpoint(unixSocketPath);
            const auto& error = future.GetValue(TDuration::Seconds(5));
            UNIT_ASSERT_C(!HasError(error), error);
        }
    }

    Y_UNIT_TEST(ShouldHandleRequestsCorrectlyWhenServedByMultipleExecutors)
    {
        const ui32 blockSize = 4096;
        const ui32 vhostQueuesCount = 4;
        const ui32 threadCount = 4;
        const ui32 blocksPerRequest = 4;
        const ui32 requestCount = 16;
        const ui64 blocksCount = requestCount * blocksPerRequest;
        const TString unixSocketPath = "testSocket";
        TTempFile tempFile(unixSocketPath);

        // In-memory disk image. It is accessed from all the executor threads of
        // the endpoint at once, hence the lock. Every request touches its own
        // block range, so requests never overlap.
        TVector<char> image(blocksCount * blockSize, 0);
        TAdaptiveLock imageLock;

        // The first |threadCount| requests block until all of them have
        // arrived, so the data below is verified for requests that were really
        // processed simultaneously.
        std::atomic<ui32> arrivedCount = 0;
        TManualEvent allArrived;

        auto waitForAllExecutors = [&] {
            if (arrivedCount.fetch_add(1) + 1 == threadCount) {
                allArrived.Signal();
            }
            UNIT_ASSERT(allArrived.WaitT(TDuration::Seconds(30)));
        };

        auto testStorage = std::make_shared<TTestStorage>();

        testStorage->WriteBlocksLocalHandler = [&] (
            TCallContextPtr ctx,
            std::shared_ptr<NProto::TWriteBlocksLocalRequest> request)
        {
            Y_UNUSED(ctx);

            waitForAllExecutors();

            auto guard = request->Sglist.Acquire();
            UNIT_ASSERT(guard);
            const auto& sgList = guard.Get();
            UNIT_ASSERT_VALUES_EQUAL(
                request->BlocksCount * blockSize,
                SgListGetSize(sgList));

            ui64 offset = request->GetStartIndex() * blockSize;
            with_lock (imageLock) {
                for (const auto& buffer: sgList) {
                    UNIT_ASSERT(offset + buffer.Size() <= image.size());
                    memcpy(image.data() + offset, buffer.Data(), buffer.Size());
                    offset += buffer.Size();
                }
            }

            return MakeFuture(NProto::TWriteBlocksLocalResponse());
        };

        testStorage->ReadBlocksLocalHandler = [&] (
            TCallContextPtr ctx,
            std::shared_ptr<NProto::TReadBlocksLocalRequest> request)
        {
            Y_UNUSED(ctx);

            waitForAllExecutors();

            auto guard = request->Sglist.Acquire();
            UNIT_ASSERT(guard);
            const auto& sgList = guard.Get();
            UNIT_ASSERT_VALUES_EQUAL(
                request->GetBlocksCount() * blockSize,
                SgListGetSize(sgList));

            ui64 offset = request->GetStartIndex() * blockSize;
            with_lock (imageLock) {
                for (const auto& buffer: sgList) {
                    UNIT_ASSERT(offset + buffer.Size() <= image.size());
                    memcpy(
                        const_cast<char*>(buffer.Data()),
                        image.data() + offset,
                        buffer.Size());
                    offset += buffer.Size();
                }
            }

            return MakeFuture(NProto::TReadBlocksLocalResponse());
        };

        auto queueFactory = std::make_shared<TTestVhostQueueFactory>();

        TServerConfig serverConfig;
        serverConfig.ThreadsCount = threadCount;

        auto server = CreateServer(
            CreateLoggingService("console"),
            std::make_shared<TTestServerStats>(),
            queueFactory,
            CreateDefaultDeviceHandlerFactory(),
            serverConfig,
            TVhostCallbacks());

        server->Start();
        Y_DEFER {
            server->Stop();
        };

        TStorageOptions options;
        options.DiskId = "testDiskId";
        options.BlockSize = blockSize;
        options.BlocksCount = blocksCount;
        options.VhostQueuesCount = vhostQueuesCount;
        options.ThreadCount = threadCount;

        {
            auto future = server->StartEndpoint(
                unixSocketPath,
                testStorage,
                options);
            const auto& error = future.GetValue(TDuration::Seconds(5));
            UNIT_ASSERT_C(!HasError(error), error);
        }

        auto device = queueFactory->Queues.at(0)->GetDevices().at(0);

        // Every request gets its own pattern, so that data landing at a wrong
        // offset or a response completing a wrong request is detected.
        auto blockData = [&] (ui32 requestIndex) {
            return TString(blockSize, 'a' + requestIndex % 26);
        };

        // All the requests are sent before any of them completes, so they are
        // spread over all the executors of the endpoint.
        auto sendRequests = [&] (
            EBlockStoreRequest requestType,
            TVector<TVector<TString>>& buffers)
        {
            TVector<TFuture<TVhostRequest::EResult>> futures;
            for (ui32 i = 0; i < requestCount; ++i) {
                futures.push_back(device->SendTestRequest(
                    requestType,
                    i * blocksPerRequest * blockSize,
                    blocksPerRequest * blockSize,
                    ResizeBlocks(
                        buffers[i],
                        blocksPerRequest,
                        requestType == EBlockStoreRequest::WriteBlocks
                            ? blockData(i)
                            : TString(blockSize, 0))));
            }

            for (auto& future: futures) {
                UNIT_ASSERT_VALUES_EQUAL(
                    static_cast<int>(TVhostRequest::SUCCESS),
                    static_cast<int>(future.GetValue(TDuration::Seconds(30))));
            }
        };

        {
            TVector<TVector<TString>> buffers(requestCount);
            sendRequests(EBlockStoreRequest::WriteBlocks, buffers);
        }

        // Each request has to be written at its own offset.
        for (ui32 i = 0; i < requestCount; ++i) {
            for (ui32 j = 0; j < blocksPerRequest; ++j) {
                const ui64 offset =
                    (i * blocksPerRequest + j) * ui64(blockSize);
                UNIT_ASSERT_VALUES_EQUAL_C(
                    TStringBuf(blockData(i)),
                    TStringBuf(image.data() + offset, blockSize),
                    "request " << i << ", block " << j);
            }
        }

        arrivedCount = 0;
        allArrived.Reset();

        TVector<TVector<TString>> readBuffers(requestCount);
        sendRequests(EBlockStoreRequest::ReadBlocks, readBuffers);

        // Each request has to read back the data written by itself.
        for (ui32 i = 0; i < requestCount; ++i) {
            UNIT_ASSERT_VALUES_EQUAL(blocksPerRequest, readBuffers[i].size());
            for (ui32 j = 0; j < blocksPerRequest; ++j) {
                UNIT_ASSERT_VALUES_EQUAL_C(
                    blockData(i),
                    readBuffers[i][j],
                    "request " << i << ", block " << j);
            }
        }

        {
            auto future = server->StopEndpoint(unixSocketPath);
            const auto& error = future.GetValue(TDuration::Seconds(5));
            UNIT_ASSERT_C(!HasError(error), error);
        }
    }

    Y_UNIT_TEST(ShouldBalanceVhostQueuesBetweenExecutors)
    {
        auto queueFactory = std::make_shared<TTestVhostQueueFactory>();
        TVector<std::unique_ptr<TTempFile>> socketFiles;

        TServerConfig serverConfig;
        serverConfig.ThreadsCount = 3;

        auto server = CreateServer(
            CreateLoggingService("console"),
            std::make_shared<TTestServerStats>(),
            queueFactory,
            CreateDefaultDeviceHandlerFactory(),
            serverConfig,
            TVhostCallbacks());

        server->Start();
        Y_DEFER {
            server->Stop();
        };

        const auto deadline = TInstant::Now() + TDuration::Seconds(5);
        for (const auto& queue: queueFactory->Queues) {
            while (!queue->IsRun() && TInstant::Now() < deadline) {
                Sleep(TDuration::MilliSeconds(10));
            }
            UNIT_ASSERT(queue->IsRun());
        }

        auto startEndpoint = [&] (ui32 vhostQueuesCount, ui32 threadCount) {
            const TString socketPath = CreateGuidAsString() + ".sock";
            socketFiles.push_back(std::make_unique<TTempFile>(socketPath));

            TStorageOptions options;
            options.DiskId = socketPath;
            options.BlockSize = 4096;
            options.BlocksCount = 256;
            options.VhostQueuesCount = vhostQueuesCount;
            options.ThreadCount = threadCount;

            auto future = server->StartEndpoint(
                socketPath,
                std::make_shared<TTestStorage>(),
                options);
            const auto& error = future.GetValue(TDuration::Seconds(5));
            UNIT_ASSERT_C(!HasError(error), error);
        };

        // The first contributes four queues to executor 0. The second endpoint
        // contributes three queues to executor 1 and three to executor 2.
        startEndpoint(4, 1);
        startEndpoint(5, 2);

        // Executor 1 is now the least loaded one (three queues), so it must get
        // the last endpoint.
        startEndpoint(1, 1);

        UNIT_ASSERT_VALUES_EQUAL(
            1,
            queueFactory->Queues[0]->GetDevices().size());
        UNIT_ASSERT_VALUES_EQUAL(
            2,
            queueFactory->Queues[1]->GetDevices().size());
        UNIT_ASSERT_VALUES_EQUAL(
            1,
            queueFactory->Queues[2]->GetDevices().size());
    }

    Y_UNIT_TEST(ShouldUseRequestedThreadCount)
    {
        // Neither the thread pool nor the guest's virtqueues are the limit
        // here, so the requested value is used as is.
        UNIT_ASSERT_VALUES_EQUAL(
            1,
            StartEndpointAndCountExecutors(4, 4, 1));
        UNIT_ASSERT_VALUES_EQUAL(
            2,
            StartEndpointAndCountExecutors(4, 4, 2));
        UNIT_ASSERT_VALUES_EQUAL(
            4,
            StartEndpointAndCountExecutors(4, 4, 4));
    }

    Y_UNIT_TEST(ShouldClampRequestedThreadCount)
    {
        // Not more threads than there are virtqueues.
        UNIT_ASSERT_VALUES_EQUAL(
            2,
            StartEndpointAndCountExecutors(4, 2, 4));
        UNIT_ASSERT_VALUES_EQUAL(
            1,
            StartEndpointAndCountExecutors(4, 1, 4));

        // Not more threads than there are in the thread pool.
        UNIT_ASSERT_VALUES_EQUAL(
            2,
            StartEndpointAndCountExecutors(2, 4, 4));
        UNIT_ASSERT_VALUES_EQUAL(
            1,
            StartEndpointAndCountExecutors(1, 4, 4));
    }
}

}   // namespace NCloud::NBlockStore::NVhost
