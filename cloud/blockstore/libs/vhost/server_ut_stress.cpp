#include "server.h"

#include "vhost_test.h"

#include <cloud/blockstore/libs/diagnostics/server_stats_test.h>
#include <cloud/blockstore/libs/service/device_handler.h>
#include <cloud/blockstore/libs/service/storage.h>
#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/thread/factory.h>
#include <util/thread/lfqueue.h>

#include <atomic>
#include <random>

namespace NCloud::NBlockStore::NVhost {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TScopedTasks
{
private:
    using TThread = THolder<IThreadFactory::IThread>;

    TVector<TThread> Workers;
    std::atomic_flag ShouldStart = false;

public:
    void Start()
    {
        ShouldStart.test_and_set();
    }

    void Stop()
    {
        for (auto& w: Workers) {
            w->Join();
        }
    }

    template <typename F>
    void Add(F f)
    {
        Workers.push_back(SystemThreadFactory()->Run(
            [this, f = std::move(f)]() {
                while (!ShouldStart.test()) {}
                f();
            }
        ));
    }
};

////////////////////////////////////////////////////////////////////////////////

class TStressStorage final
    : public IStorage
{
private:
    TLockFreeQueue<TPromise<NProto::TError>> Requests;

public:
    NThreading::TFuture<NProto::TZeroBlocksResponse> ZeroBlocks(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TZeroBlocksRequest> request) override
    {
        Y_UNUSED(callContext);
        Y_UNUSED(request);
        return RegisterRequest<NProto::TZeroBlocksResponse>();
    }

    NThreading::TFuture<NProto::TWriteBlocksLocalResponse> WriteBlocksLocal(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TWriteBlocksLocalRequest> request) override
    {
        Y_UNUSED(callContext);
        Y_UNUSED(request);
        return RegisterRequest<NProto::TWriteBlocksLocalResponse>();
    }

    NThreading::TFuture<NProto::TReadBlocksLocalResponse> ReadBlocksLocal(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TReadBlocksLocalRequest> request) override
    {
        Y_UNUSED(callContext);
        Y_UNUSED(request);
        return RegisterRequest<NProto::TReadBlocksLocalResponse>();
    }

    NThreading::TFuture<NProto::TError> EraseDevice(
        NProto::EDeviceEraseMethod method) override
    {
        Y_UNUSED(method);
        return NThreading::MakeFuture(NProto::TError());
    }

    TStorageBuffer AllocateBuffer(size_t bytesCount) override
    {
        Y_UNUSED(bytesCount);
        return nullptr;
    }

    void ReportIOError() override
    {}

    bool CompleteRequest(NProto::TError error)
    {
        TPromise<NProto::TError> promise;
        if (Requests.Dequeue(&promise)) {
            promise.SetValue(std::move(error));
            return true;
        }
        return false;
    }

private:
    template<typename T>
    TFuture<T> RegisterRequest()
    {
        auto promise = NewPromise<NProto::TError>();
        auto future = promise.GetFuture();
        Requests.Enqueue(std::move(promise));

        return future.Apply([] (const auto& f) {
            T response;
            *response.MutableError() = f.GetValue();
            return response;
        });
    }
};

////////////////////////////////////////////////////////////////////////////////

std::mt19937_64 CreateRandomEngine()
{
    std::random_device rd;
    std::array<ui32, std::mt19937_64::state_size> randomData;
    std::generate(std::begin(randomData), std::end(randomData), std::ref(rd));
    std::seed_seq seeds(std::begin(randomData), std::end(randomData));
    return std::mt19937_64(seeds);
}

void SendRandomRequest(ITestVhostDevice& device)
{
    thread_local auto eng = CreateRandomEngine();

    std::uniform_int_distribution<ui64> dist1(0, 1);
    EBlockStoreRequest type = dist1(eng) == 0
        ? EBlockStoreRequest::WriteBlocks
        : EBlockStoreRequest::ReadBlocks;

    std::uniform_int_distribution<ui64> dist2(0, 7999);
    ui64 from = dist2(eng) * 512;

    std::uniform_int_distribution<ui64> dist3(1, 4);
    ui64 length = dist3(eng) * 1024;

    TString buffer(length, '0');
    auto sglist = TSgList{ TBlockDataRef(buffer.data(), buffer.size()) };

    auto future = device.SendTestRequest(type, from, length, std::move(sglist));
    future.Apply([holder = std::move(buffer)] (const auto& f) {
        Y_UNUSED(holder);
        return f.GetValue();
    });
}

NProto::TError GetRandomError()
{
    thread_local auto eng = CreateRandomEngine();

    std::uniform_int_distribution<ui64> dist(0, 2);
    auto code = dist(eng);
    return MakeError(code == 0 ? S_OK : (code == 1 ? E_CANCELLED : E_FAIL));
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TServerStressTest)
{
    Y_UNIT_TEST(Stress)
    {
        size_t threadsCount = 2;
        size_t endpointCount = 4;
        size_t consumerCount = 16;
        size_t providerCount = 4;

        TString socketPath = "/tmp/socket.stress";
        std::atomic<int> inflight = 0;
        std::atomic<int> completed = 0;

        auto serverStats = std::make_shared<TTestServerStats>();
        serverStats->RequestStartedHandler = [&] (
            TLog&, TMetricRequest&, TCallContext&, const TString&)
        {
            inflight.fetch_add(1);
        };
        serverStats->RequestCompletedHandler = [&] (
            TLog&, TMetricRequest&, TCallContext&, const NProto::TError&)
        {
            inflight.fetch_sub(1);
            completed.fetch_add(1);
        };

        auto queueFactory = std::make_shared<TTestVhostQueueFactory>();

        auto server = CreateServer(
            CreateLoggingService("console"),
            std::move(serverStats),
            queueFactory,
            CreateDefaultDeviceHandlerFactory(),
            TServerConfig{.ThreadsCount = threadsCount},
            TVhostCallbacks());

        server->Start();
        Sleep(TDuration::MilliSeconds(300));
        UNIT_ASSERT(queueFactory->Queues.size() == threadsCount);

        TVector<std::shared_ptr<TStressStorage>> Storages;
        TVector<std::shared_ptr<ITestVhostDevice>> Devices;

        for (size_t i = 0; i < endpointCount; ++i) {
            auto storage = std::make_shared<TStressStorage>();
            Storages.push_back(storage);

            auto future = server->StartEndpoint(
                socketPath + ToString(i + 1),
                std::move(storage),
                TStorageOptions{
                    .DiskId = "disk" + ToString(i + 1),
                    .BlockSize = 4096,
                    .BlocksCount = 1024 * 1024,
                    .VhostQueuesCount = 2,
                    .UnalignedRequestsDisabled = false,
                });
            const auto& error = future.GetValue(TDuration::Seconds(5));
            UNIT_ASSERT_C(!HasError(error), error);
        }

        for (auto& queue: queueFactory->Queues) {
            auto devices = queue->GetDevices();
            Devices.insert(Devices.end(), devices.begin(), devices.end());
        }

        TScopedTasks tasks;
        std::atomic_flag shouldStop(false);

        // Providers
        for (ui32 i = 0; i < providerCount; ++i) {
            tasks.Add([&, index = i] {
                auto device = Devices[index % Devices.size()];
                while (!shouldStop.test()) {
                    if (inflight.load() < 256) {
                        SendRandomRequest(*device);
                    }
                };
            });
        }

        // Consumers
        for (ui32 i = 0; i < consumerCount; ++i) {
            tasks.Add([&, index = i] {
                auto storage = Storages[index % Storages.size()];
                while (completed.load() < 5000) {
                    storage->CompleteRequest(GetRandomError());
                }
                shouldStop.test_and_set();
            });
        }

        tasks.Start();
        tasks.Stop();

        for (size_t i = 0; i < endpointCount / 2; ++i) {
            auto future = server->StopEndpoint(socketPath + ToString(i + 1));
            const auto& error = future.GetValue(TDuration::Seconds(30));
            UNIT_ASSERT_C(!HasError(error), error);
        }

        server->Stop();

        UNIT_ASSERT_VALUES_EQUAL(0, inflight.load());
        Cerr << "Amount of completed requests: " << completed.load() << Endl;
    }
}

}   // namespace NCloud::NBlockStore::NVhost
