#include <cloud/blockstore/libs/rdma/helper.h>
#include <cloud/blockstore/libs/rdma_test/memory_test_storage.h>
#include <cloud/blockstore/libs/rdma_test/rdma_test_environment.h>
#include <cloud/blockstore/libs/service/context.h>
#include <cloud/blockstore/libs/service_local/rdma_protocol.h>
#include <cloud/blockstore/libs/storage/disk_agent/model/device_client.h>
#include <cloud/blockstore/libs/storage/disk_agent/rdma_target.h>

#include <cloud/storage/core/libs/common/context.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>
#include <cloud/storage/core/libs/diagnostics/monitoring.h>
#include <cloud/storage/core/libs/rdma/iface/client.h>
#include <cloud/storage/core/libs/rdma/iface/protobuf.h>
#include <cloud/storage/core/libs/rdma/iface/protocol.h>
#include <cloud/storage/core/libs/rdma/iface/server.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <library/cpp/threading/future/future.h>

#include <util/datetime/base.h>
#include <util/generic/scope.h>
#include <util/generic/yexception.h>
#include <util/stream/output.h>
#include <util/system/event.h>
#include <util/system/hostname.h>

#include <atomic>
#include <chrono>
#include <cstdlib>
#include <functional>
#include <memory>
#include <thread>

namespace {

using namespace NCloud;
using namespace NCloud::NBlockStore;
using namespace NCloud::NBlockStore::NStorage;
using namespace NCloud::NStorage::NRdma;
using namespace NMonitoring;
using namespace NThreading;

constexpr ui32 BlockSize = 4_KB;
constexpr ui32 BlocksPerRequest = 32;
constexpr ui32 RequestCount = 2048;
constexpr ui32 ClientRecvQueueSize = 256;
constexpr ui32 ServerSendQueueSize = 512;
constexpr TDuration WaitTimeout = TDuration::Seconds(30);
constexpr TDuration StallDuration = TDuration::Seconds(1);

const TString DeviceId = "rnr-repro-device";
const TString ClientId = "rnr-repro-client";

struct TCounterSet
{
    TDynamicCounters::TCounterPtr QueuedRequests;
    TDynamicCounters::TCounterPtr ActiveRequests;
    TDynamicCounters::TCounterPtr CompletedRequests;
    TDynamicCounters::TCounterPtr ActiveSend;
    TDynamicCounters::TCounterPtr ActiveRead;
    TDynamicCounters::TCounterPtr ActiveWrite;
    TDynamicCounters::TCounterPtr Errors;
};

struct TCounterSnapshot
{
    i64 QueuedRequests = 0;
    i64 ActiveRequests = 0;
    i64 CompletedRequests = 0;
    i64 ActiveSend = 0;
    i64 ActiveRead = 0;
    i64 ActiveWrite = 0;
    i64 Errors = 0;
};

TDynamicCountersPtr GetRdmaCounters(
    const IMonitoringServicePtr& monitoring,
    TStringBuf component)
{
    return monitoring->GetCounters()
        ->GetSubgroup("counters", "blockstore")
        ->GetSubgroup("component", TString(component));
}

TCounterSet MakeCounterSet(const TDynamicCountersPtr& counters, bool server)
{
    return {
        .QueuedRequests = counters->GetCounter("QueuedRequests"),
        .ActiveRequests = counters->GetCounter("ActiveRequests"),
        .CompletedRequests = counters->GetCounter("CompletedRequests", true),
        .ActiveSend = counters->GetCounter("ActiveSend"),
        .ActiveRead = server ? counters->GetCounter("ActiveRead") : nullptr,
        .ActiveWrite = server ? counters->GetCounter("ActiveWrite") : nullptr,
        .Errors = counters->GetCounter("Errors", true),
    };
}

TCounterSnapshot Snapshot(const TCounterSet& counters)
{
    return {
        .QueuedRequests = counters.QueuedRequests->Val(),
        .ActiveRequests = counters.ActiveRequests->Val(),
        .CompletedRequests = counters.CompletedRequests->Val(),
        .ActiveSend = counters.ActiveSend->Val(),
        .ActiveRead = counters.ActiveRead ? counters.ActiveRead->Val() : 0,
        .ActiveWrite = counters.ActiveWrite ? counters.ActiveWrite->Val() : 0,
        .Errors = counters.Errors->Val(),
    };
}

void PrintSnapshot(
    TStringBuf label,
    const TCounterSnapshot& server,
    const TCounterSnapshot& client)
{
    Cout << label << "\n"
         << "  server: QueuedRequests=" << server.QueuedRequests
         << " ActiveRequests=" << server.ActiveRequests
         << " CompletedRequests=" << server.CompletedRequests
         << " ActiveSend=" << server.ActiveSend
         << " ActiveRead=" << server.ActiveRead
         << " ActiveWrite=" << server.ActiveWrite
         << " Errors=" << server.Errors << "\n"
         << "  client: QueuedRequests=" << client.QueuedRequests
         << " ActiveRequests=" << client.ActiveRequests
         << " CompletedRequests=" << client.CompletedRequests
         << " ActiveSend=" << client.ActiveSend
         << " Errors=" << client.Errors << Endl;
}

bool WaitUntil(std::function<bool()> condition, TDuration timeout)
{
    const TInstant deadline = TInstant::Now() + timeout;
    while (TInstant::Now() < deadline) {
        if (condition()) {
            return true;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    return condition();
}

class TBlockingClientHandler final: public IClientHandler
{
private:
    const ui32 ExpectedResponses;
    TManualEvent Paused;
    TManualEvent ReleaseEvent;
    TManualEvent Done;
    std::atomic<ui32> Callbacks = 0;
    std::atomic<ui32> Completed = 0;
    std::atomic<ui32> Errors = 0;

public:
    explicit TBlockingClientHandler(ui32 expectedResponses)
        : ExpectedResponses(expectedResponses)
    {}

    void HandleResponse(
        TClientRequestPtr request,
        ui32 status,
        size_t responseBytes) override
    {
        Y_UNUSED(request);
        Y_UNUSED(responseBytes);

        if (status != RDMA_PROTO_OK) {
            ++Errors;
        }

        if (Callbacks.fetch_add(1) == 0) {
            Paused.Signal();
            ReleaseEvent.WaitI();
        }

        if (Completed.fetch_add(1) + 1 == ExpectedResponses) {
            Done.Signal();
        }
    }

    bool WaitUntilPaused(TDuration timeout)
    {
        return Paused.WaitT(timeout);
    }

    void Release()
    {
        ReleaseEvent.Signal();
    }

    bool WaitUntilDone(TDuration timeout)
    {
        return Done.WaitT(timeout);
    }

    ui32 GetCallbacks() const
    {
        return Callbacks.load();
    }

    ui32 GetErrors() const
    {
        return Errors.load();
    }
};

struct TEnvironment
{
    ILoggingServicePtr Logging;
    IMonitoringServicePtr ServerMonitoring;
    IMonitoringServicePtr ClientMonitoring;
    IServerPtr Server;
    IRdmaTargetPtr Target;
    IClientPtr Client;
    IClientEndpointPtr Endpoint;
    std::shared_ptr<TBlockingClientHandler> Handler;

    ~TEnvironment()
    {
        if (Handler) {
            Handler->Release();
        }
        if (Endpoint) {
            Endpoint->Stop().GetValueSync();
        }
        if (Client) {
            Client->Stop();
        }
        if (Target) {
            Target->Stop();
        } else if (Server) {
            Server->Stop();
        }
        if (ClientMonitoring) {
            ClientMonitoring->Stop();
        }
        if (ServerMonitoring) {
            ServerMonitoring->Stop();
        }
        if (Logging) {
            Logging->Stop();
        }
    }
};

void Run(TString host, ui32 port)
{
    TEnvironment env;
    env.Logging = CreateLoggingService("console", TLogSettings{TLOG_WARNING});
    env.ServerMonitoring = CreateMonitoringServiceStub();
    env.ClientMonitoring = CreateMonitoringServiceStub();
    env.Logging->Start();
    env.ServerMonitoring->Start();
    env.ClientMonitoring->Start();

    auto serverConfig = std::make_shared<TServerConfig>();
    serverConfig->SendQueueSize = ServerSendQueueSize;
    serverConfig->RecvQueueSize = RequestCount;
    serverConfig->StrictValidation = false;
    serverConfig->MaxBufferSize = 1_MB;
    serverConfig->PollerThreads = 1;
    serverConfig->WaitMode = EWaitMode::BusyWait;
    serverConfig->QpRnrRetryCount = 7;
    serverConfig->QpMinRnrTimer = 12;

    env.Server = NRdma::CreateRdmaServer(
        env.Logging,
        env.ServerMonitoring,
        std::move(serverConfig));
    env.Server->Start();

    auto storage = std::make_shared<TMemoryTestStorage>(
        BlockSize * BlocksPerRequest);
    auto storageGate = NewPromise<void>();
    storage->SetHandbrake(storageGate.GetFuture());
    bool storageReleased = false;
    Y_DEFER {
        if (!storageReleased) {
            storageGate.SetValue();
        }
    };

    TVector<std::pair<TString, TStorageAdapterPtr>> devices{
        {DeviceId,
         std::make_shared<TStorageAdapter>(
             storage,
             BlockSize,
             true,
             TDuration::Zero(),
             TDuration::Zero())}};

    auto deviceClient = std::make_shared<TDeviceClient>(
        TDuration::Minutes(1),
        std::move(devices),
        env.Logging->CreateLog("BLOCKSTORE_DISK_AGENT"),
        false);
    auto acquireResult = deviceClient->AcquireDevices(
        {DeviceId},
        ClientId,
        TInstant::Now(),
        NCloud::NBlockStore::NProto::VOLUME_ACCESS_READ_WRITE,
        0,
        "rnr-repro-volume",
        1);
    Y_ENSURE(
        !HasError(acquireResult),
        "failed to acquire test device: " << acquireResult.GetError());

    NCloud::NBlockStore::NProto::TRdmaTarget targetProto;
    targetProto.MutableEndpoint()->SetHost(host);
    targetProto.MutableEndpoint()->SetPort(port);
    targetProto.SetWorkerThreads(1);

    auto targetConfig = std::make_shared<TRdmaTargetConfig>(
        false,
        std::move(targetProto));
    auto targetCounters = MakeIntrusive<TDynamicCounters>();
    env.Target = CreateRdmaTarget(
        std::move(targetConfig),
        {
            targetCounters->GetCounter("Delayed"),
            targetCounters->GetCounter("Rejected"),
        },
        env.Logging,
        env.Server,
        std::move(deviceClient),
        std::make_shared<TTestMultiAgentWriteHandler>(),
        {DeviceId});
    env.Target->Start();

    auto clientConfig = std::make_shared<TClientConfig>();
    clientConfig->SendQueueSize = RequestCount;
    clientConfig->RecvQueueSize = ClientRecvQueueSize;
    clientConfig->MaxBufferSize = 1_MB;
    clientConfig->PollerThreads = 1;
    clientConfig->WaitMode = EWaitMode::BusyWait;
    clientConfig->MaxResponseDelay = TDuration::Minutes(5);
    clientConfig->QpRnrRetryCount = 7;
    clientConfig->QpMinRnrTimer = 12;

    env.Client = NRdma::CreateRdmaClient(
        env.Logging,
        env.ClientMonitoring,
        std::move(clientConfig));
    env.Client->Start();
    env.Endpoint = env.Client->StartEndpoint(host, port).ExtractValueSync();
    env.Handler = std::make_shared<TBlockingClientHandler>(RequestCount);

    auto serverCounters = MakeCounterSet(
        GetRdmaCounters(env.ServerMonitoring, "rdma_server"),
        true);
    auto clientCounters = MakeCounterSet(
        GetRdmaCounters(env.ClientMonitoring, "rdma_client"),
        false);

    NCloud::NBlockStore::NProto::TReadDeviceBlocksRequest proto;
    proto.SetDeviceUUID(DeviceId);
    proto.SetBlockSize(BlockSize);
    proto.SetStartIndex(0);
    proto.SetBlocksCount(BlocksPerRequest);
    proto.MutableHeaders()->SetClientId(ClientId);

    auto* serializer = TBlockStoreProtocol::Serializer();
    const size_t requestBytes = serializer->MessageByteSize(proto, 0);
    const size_t responseBytes = 4_KB + BlockSize * BlocksPerRequest;

    Cout << "Submitting " << RequestCount << " disk-agent ReadDeviceBlocks "
         << "requests of " << BlockSize * BlocksPerRequest / 1_KB
         << " KiB..." << Endl;

    for (ui32 i = 0; i < RequestCount; ++i) {
        auto result = env.Endpoint->AllocateRequest(
            env.Handler,
            std::make_unique<TNullContext>(),
            requestBytes,
            responseBytes);
        Y_ENSURE(
            !HasError(result),
            "AllocateRequest failed at request " << i << ": "
                                                  << result.GetError());

        auto request = result.ExtractResult();
        serializer->Serialize(
            request->RequestBuffer,
            TBlockStoreProtocol::ReadDeviceBlocksRequest,
            0,
            proto);
        env.Endpoint->SendRequest(
            std::move(request),
            MakeIntrusive<TCallContextBase>(i + 1));
    }

    Y_ENSURE(
        WaitUntil(
            [&] {
                return Snapshot(serverCounters).ActiveRequests == RequestCount;
            },
            WaitTimeout),
        "disk-agent server did not receive all requests");

    PrintSnapshot(
        "All requests reached the disk-agent target; releasing storage:",
        Snapshot(serverCounters),
        Snapshot(clientCounters));

    storageGate.SetValue();
    storageReleased = true;

    Y_ENSURE(
        env.Handler->WaitUntilPaused(WaitTimeout),
        "client response callback was not reached");

    Y_ENSURE(
        WaitUntil(
            [&] {
                const auto s = Snapshot(serverCounters);
                return s.ActiveSend + s.ActiveWrite == ServerSendQueueSize &&
                       s.QueuedRequests > 0;
            },
            WaitTimeout),
        "server send WR pool did not saturate");

    const auto stalledServer1 = Snapshot(serverCounters);
    const auto stalledClient1 = Snapshot(clientCounters);
    PrintSnapshot("Client CQ callback paused:", stalledServer1, stalledClient1);

    std::this_thread::sleep_for(
        std::chrono::milliseconds(StallDuration.MilliSeconds()));

    const auto stalledServer2 = Snapshot(serverCounters);
    const auto stalledClient2 = Snapshot(clientCounters);
    PrintSnapshot("After one second:", stalledServer2, stalledClient2);

    Y_ENSURE(
        stalledServer2.ActiveSend + stalledServer2.ActiveWrite ==
            ServerSendQueueSize,
        "server send WR pool did not remain saturated");
    Y_ENSURE(
        stalledServer2.ActiveRead == 0,
        "server still has active RDMA reads");
    Y_ENSURE(
        stalledServer2.QueuedRequests > 0,
        "server queue did not grow");
    Y_ENSURE(
        stalledServer2.Errors == 0 && stalledClient2.Errors == 0,
        "RDMA error was reported during the stall");
    Y_ENSURE(
        stalledClient2.ActiveRequests > 0 && stalledClient2.ActiveSend == 0,
        "client counters do not match receive starvation");
    Y_ENSURE(
        env.Handler->GetCallbacks() == 1,
        "more than one callback ran while the CQ thread was paused");

    Cout << "PASS: disk-agent traffic is stalled with all "
         << ServerSendQueueSize
         << " server WRs active and no RDMA error." << Endl;

    env.Handler->Release();
    Y_ENSURE(
        env.Handler->WaitUntilDone(WaitTimeout),
        "requests did not recover after releasing the client CQ callback");
    Y_ENSURE(
        env.Handler->GetErrors() == 0,
        "one or more disk-agent responses failed");
    Y_ENSURE(
        WaitUntil(
            [&] {
                const auto s = Snapshot(serverCounters);
                const auto c = Snapshot(clientCounters);
                return s.ActiveRequests == 0 && s.QueuedRequests == 0 &&
                       s.ActiveSend == 0 && s.ActiveRead == 0 &&
                       s.ActiveWrite == 0 && c.ActiveRequests == 0 &&
                       c.QueuedRequests == 0 && c.ActiveSend == 0;
            },
            WaitTimeout),
        "RDMA queues did not drain after recovery");

    PrintSnapshot(
        "Recovered after releasing the client CQ callback:",
        Snapshot(serverCounters),
        Snapshot(clientCounters));
    Cout << "REPRODUCED: the NBS disk-agent RDMA data path reached the same "
         << "512-WR head-of-line stall and recovered when receive processing "
         << "resumed." << Endl;
}

}   // namespace

int main(int argc, char** argv)
{
    if (argc < 2 || argc > 3) {
        Cerr << "usage: " << argv[0] << " <rxe-interface-ip> [port]" << Endl;
        return 2;
    }

    try {
        const ui32 port = argc == 3 ? std::strtoul(argv[2], nullptr, 10) : 18515;
        Run(argv[1], port);
        return 0;
    } catch (...) {
        Cerr << "ERROR: " << CurrentExceptionMessage() << Endl;
        return 1;
    }
}
