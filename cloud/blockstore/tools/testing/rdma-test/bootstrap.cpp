#include "bootstrap.h"

#include "initiator.h"
#include "options.h"
#include "probes.h"
#include "storage.h"
#include "target.h"

#include <cloud/blockstore/libs/rdma/iface/probes.h>
#include <cloud/blockstore/libs/rdma/impl/client.h>
#include <cloud/blockstore/libs/rdma/impl/server.h>
#include <cloud/blockstore/libs/rdma/impl/verbs.h>

#include <cloud/storage/core/libs/common/scheduler.h>
#include <cloud/storage/core/libs/common/task_queue.h>
#include <cloud/storage/core/libs/common/thread_pool.h>
#include <cloud/storage/core/libs/common/timer.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>
#include <cloud/storage/core/libs/diagnostics/monitoring.h>
#include <cloud/storage/core/libs/diagnostics/trace_processor_mon.h>
#include <cloud/storage/core/libs/diagnostics/trace_processor.h>
#include <cloud/storage/core/libs/diagnostics/trace_reader.h>

#include <library/cpp/lwtrace/mon/mon_lwtrace.h>

#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

const TString TraceLoggerId = "st_trace_logger";

const TVector<std::tuple<TString, TString>> StartProbes = {
    {"RequestStarted", "BLOCKSTORE_TEST_PROVIDER"},
};

////////////////////////////////////////////////////////////////////////////////

TBootstrap::TBootstrap(TOptionsPtr options)
    : Options(std::move(options))
{}

void TBootstrap::Init()
{
    Timer = CreateWallClockTimer();
    Scheduler = CreateScheduler(Timer);

    InitLogging();
    InitTracing();

    Monitoring = CreateMonitoringServiceStub();

    if (Options->ThreadPool) {
        ThreadPool = CreateThreadPool(
            Options->TestMode == ETestMode::Target ? "SRV" : "CLI",
            Options->ThreadPool);
    } else {
        ThreadPool = CreateTaskQueueStub();
    }

    if (Options->TestMode == ETestMode::Target) {
        auto config = std::make_unique<NRdma::TServerConfig>();
        config->Backlog = Options->Backlog;
        config->QueueSize = Options->QueueSize;
        config->PollerThreads = Options->PollerThreads;
        config->WaitMode = NRdma::EWaitMode(Options->WaitMode);

        Verbs = NRdma::NVerbs::CreateVerbs();

        Server = NRdma::CreateServer(
            Verbs,
            Logging,
            Monitoring,
            std::move(config));
    } else if (Options->StorageKind == EStorageKind::Rdma) {
        auto config = std::make_unique<NRdma::TClientConfig>();
        config->QueueSize = Options->QueueSize;
        config->PollerThreads = Options->PollerThreads;
        config->WaitMode = NRdma::EWaitMode(Options->WaitMode);
        config->IpTypeOfService = Options->Tos;
        config->SourceInterface = Options->SourceInterface;

        Verbs = NRdma::NVerbs::CreateVerbs();

        Client = NRdma::CreateClient(
            Verbs,
            Logging,
            Monitoring,
            std::move(config));
    }
}

void TBootstrap::InitLogging()
{
    TLogSettings settings;

    if (Options->VerboseLevel) {
        auto level = GetLogLevel(Options->VerboseLevel);
        Y_ENSURE(level, "unknown log level: " << Options->VerboseLevel.Quote());
        settings.FiltrationLevel = *level;
    }

    Logging = CreateLoggingService("console", settings);
}

void TBootstrap::InitTracing()
{
    auto& probes = NLwTraceMonPage::ProbeRegistry();
    probes.AddProbesList(LWTRACE_GET_PROBES(BLOCKSTORE_RDMA_PROVIDER));
    probes.AddProbesList(LWTRACE_GET_PROBES(BLOCKSTORE_TEST_PROVIDER));

    auto& traceManager = NLwTraceMonPage::TraceManager(false);

    TVector<ITraceReaderPtr> traceReaders;
    if (Options->TracePath) {
        traceManager.New(
            TraceLoggerId,
            ProbabilisticQuery(StartProbes, Options->TraceRate));

        traceReaders.emplace_back(SetupTraceReaderWithLog(
            TraceLoggerId,
            CreateLoggingService(Options->TracePath),
            "BLOCKSTORE_TRACE",
            "AllRequests"));
    }

    if (traceReaders) {
        TraceProcessor = CreateTraceProcessorMon(
            Monitoring,
            CreateTraceProcessor(
                Timer,
                Scheduler,
                Logging,
                "BLOCKSTORE_TRACE",
                traceManager,
                std::move(traceReaders)));
    }
}

void TBootstrap::Start()
{
#define START(c) if (c) (c)->Start();

    START(Scheduler);
    START(Logging);
    START(Monitoring);
    START(ThreadPool);
    START(TraceProcessor);
    START(Server);
    START(Client);
    START(Storage);

#undef START
}

void TBootstrap::Stop()
{
#define STOP(c) if (c) (c)->Stop();

    STOP(Storage);
    STOP(Client);
    STOP(Server);
    STOP(TraceProcessor);
    STOP(ThreadPool);
    STOP(Monitoring);
    STOP(Logging);
    STOP(Scheduler);

#undef STOP
}

IRunnablePtr TBootstrap::CreateTest()
{
    switch (Options->StorageKind) {
    case EStorageKind::Null:
        Storage = CreateNullStorage();
        break;

    case EStorageKind::Memory:
        Storage = CreateMemoryStorage(
            Options->BlockSize,
            Options->BlocksCount);
        break;

    case EStorageKind::LocalAIO:
        Storage = CreateLocalAIOStorage(
            Options->StoragePath,
            Options->BlockSize,
            Options->BlocksCount);
        break;

    case EStorageKind::LocalURing:
        Storage = CreateLocalURingStorage(
            Options->StoragePath,
            Options->BlockSize,
            Options->BlocksCount);
        break;

    case EStorageKind::Rdma:
        Y_ABORT_UNLESS(Options->TestMode == ETestMode::Initiator);
        Storage = CreateRdmaStorage(
            Client,
            ThreadPool,
            Options->Host,
            Options->Port,
            TDuration::Seconds(Options->ConnectTimeout));
        break;
    }

    Storage->Start();

    if (Options->TestMode == ETestMode::Target) {
        return CreateTestTarget(Options, ThreadPool, Storage, Server);
    } else {
        return CreateTestInitiator(Options, Storage);
    }
}

}   // namespace NCloud::NBlockStore
