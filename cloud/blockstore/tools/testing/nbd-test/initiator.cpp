#include "target.h"

#include "options.h"
#include "runnable.h"

#include <cloud/blockstore/libs/nbd/client.h>
#include <cloud/blockstore/libs/nbd/client_handler.h>
#include <cloud/blockstore/libs/service/service.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/format.h>
#include <cloud/storage/core/libs/common/sglist.h>
#include <cloud/storage/core/libs/diagnostics/histogram.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <library/cpp/deprecated/atomic/atomic.h>

#include <util/datetime/base.h>
#include <util/generic/intrlist.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/network/address.h>
#include <util/random/random.h>
#include <util/system/condvar.h>
#include <util/system/mutex.h>

namespace NCloud::NBlockStore {

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr TDuration ReportInterval = TDuration::Seconds(5);

////////////////////////////////////////////////////////////////////////////////

struct TRequest : TIntrusiveSListItem<TRequest>
{
    TString Buffer;

    bool WriteRequest;
    ui32 StartOffset;
    ui32 BlocksCount;

    TInstant SendTs;
};

////////////////////////////////////////////////////////////////////////////////

struct TRequestStats
{
    ui64 NumReadOps = 0;
    ui64 NumWriteOps = 0;
    ui64 BytesRead = 0;
    ui64 BytesWritten = 0;
};

TRequestStats operator -(const TRequestStats& l, const TRequestStats& r)
{
    return TRequestStats {
        .NumReadOps = l.NumReadOps - r.NumReadOps,
        .NumWriteOps = l.NumWriteOps - r.NumWriteOps,
        .BytesRead = l.BytesRead - r.BytesRead,
        .BytesWritten = l.BytesWritten - r.BytesWritten,
    };
}

////////////////////////////////////////////////////////////////////////////////

class TRequestSender
{
public:
    struct TOptions
    {
        ui32 BlockSize;
        ui64 BlocksCount;

        ui32 MaxIoDepth;
        ui32 MinBlocksCount;
        ui32 MaxBlocksCount;
        ui32 WriteRate;
    };

private:
    const IBlockStorePtr Endpoint;
    const TOptions Options;

    TVector<TRequest> Requests;
    TIntrusiveSList<TRequest> CompletedRequests;
    size_t CompletedRequestsCount = 0;

    TMutex Mutex;
    TCondVar CondVar;

public:
    TRequestStats Stats;
    TRequestStats PrevStats;
    TLatencyHistogram Latency;

public:
    TRequestSender(IBlockStorePtr endpoint, const TOptions& options)
        : Endpoint(std::move(endpoint))
        , Options(options)
    {
        Requests.resize(Options.MaxIoDepth);

        for (auto& request: Requests) {
            request.Buffer.resize(Options.BlockSize * Options.MaxBlocksCount);

            CompletedRequests.PushFront(&request);
            ++CompletedRequestsCount;
        }
    }

    void SendRequest(TRequest* request)
    {
        request->WriteRequest = RandomNumber(100ul) < Options.WriteRate;

        request->BlocksCount = Options.MinBlocksCount;
        if (Options.MinBlocksCount < Options.MaxBlocksCount) {
            request->BlocksCount += RandomNumber(
                Options.MaxBlocksCount - Options.MinBlocksCount);
        }

        request->StartOffset = RandomNumber(
            Options.BlocksCount - request->BlocksCount);

        request->SendTs = TInstant::Now();

        if (request->WriteRequest) {
            WriteBlocks(request);
        } else {
            ReadBlocks(request);
        }
    }

    TRequest* WaitForRequest(TDuration timeout)
    {
        with_lock (Mutex) {
            if (CompletedRequests) {
                auto* request = CompletedRequests.Front();

                CompletedRequests.PopFront();
                --CompletedRequestsCount;

                return request;
            }

            CondVar.WaitT(Mutex, timeout);
            return nullptr;
        }
    }

    bool WaitForCompletion(TDuration timeout)
    {
        with_lock (Mutex) {
            if (CompletedRequestsCount == Requests.size()) {
                return true;
            }

            CondVar.WaitT(Mutex, timeout);
            return false;
        }
    }

private:
    void WriteBlocks(TRequest* request)
    {
        auto callContext = MakeIntrusive<TCallContext>();

        auto req = std::make_shared<NProto::TWriteBlocksLocalRequest>();
        req->SetStartIndex(request->StartOffset);
        req->BlocksCount = request->BlocksCount;
        req->BlockSize = Options.BlockSize;

        auto sgList = TSgList {{
            request->Buffer.begin(),
            request->BlocksCount  * Options.BlockSize
        }};

        req->Sglist = TGuardedSgList(std::move(sgList));
        auto guardedSgList = req->Sglist;

        auto future = Endpoint->WriteBlocksLocal(
            std::move(callContext),
            std::move(req));

        future.Subscribe([=, this] (auto f) mutable {
            guardedSgList.Close();

            auto response = ExtractResponse(f);
            ReportProgress(request, response.GetError());
        });
    }

    void ReadBlocks(TRequest* request)
    {
        auto callContext = MakeIntrusive<TCallContext>();

        auto req = std::make_shared<NProto::TReadBlocksLocalRequest>();
        req->SetStartIndex(request->StartOffset);
        req->SetBlocksCount(request->BlocksCount);
        req->BlockSize = Options.BlockSize;

        TSgList sgList = {{
            request->Buffer.begin(),
            request->BlocksCount  * Options.BlockSize
        }};

        req->Sglist = TGuardedSgList(std::move(sgList));
        auto guardedSgList = req->Sglist;

        auto future = Endpoint->ReadBlocksLocal(
            std::move(callContext),
            std::move(req));

        future.Subscribe([=, this] (auto f) mutable {
            guardedSgList.Close();

            auto response = ExtractResponse(f);
            ReportProgress(request, response.GetError());
        });
    }

    void ReportProgress(TRequest* request, const NProto::TError& error)
    {
        // TODO
        Y_UNUSED(error);

        with_lock (Mutex) {
            if (request->WriteRequest) {
                ++Stats.NumWriteOps;
                Stats.BytesWritten += request->BlocksCount * Options.BlockSize;
            } else {
                ++Stats.NumReadOps;
                Stats.BytesRead += request->BlocksCount * Options.BlockSize;
            }

            Latency.RecordValue(Now() - request->SendTs);

            CompletedRequests.PushFront(request);
            ++CompletedRequestsCount;

            CondVar.Signal();
        }
    }
};

using TRequestSenderPtr = std::shared_ptr<TRequestSender>;

////////////////////////////////////////////////////////////////////////////////

class TTestInitiator final
    : public IRunnable
{
private:
    TAtomic ShouldStop = 0;
    TAtomic ExitCode = 0;

    TMutex WaitMutex;
    TCondVar WaitCondVar;

    TOptionsPtr Options;

    ILoggingServicePtr Logging;
    TLog Log;

    NBD::IClientPtr Client;
    NBD::IClientHandlerPtr ClientHandler;

    IBlockStorePtr Endpoint;
    TRequestSenderPtr RequestSender;

    TInstant LastReportTs;

public:
    TTestInitiator(TOptionsPtr options)
        : Options(std::move(options))
    {}

    int Run() override;
    void Stop(int exitCode) override;

private:
    void InitLogging();
    void Init();
    void Shoot();
    void WaitForShutdown();
    void ReportProgress();
    void Term();
};

////////////////////////////////////////////////////////////////////////////////

int TTestInitiator::Run()
{
    InitLogging();

    STORAGE_INFO("Initializing...");
    try {
        Init();
        Shoot();
        WaitForShutdown();
    } catch (...) {
        STORAGE_ERROR(
            "Error during initialization: " << CurrentExceptionMessage());
    }

    STORAGE_INFO("Terminating...");
    try {
        Term();
    } catch (...) {
        STORAGE_ERROR(
            "Error during shutdown: " << CurrentExceptionMessage());
    }

    return AtomicGet(ExitCode);
}

void TTestInitiator::InitLogging()
{
    TLogSettings settings;

    if (Options->VerboseLevel) {
        auto level = GetLogLevel(Options->VerboseLevel);
        Y_ENSURE(level, "unknown log level: " << Options->VerboseLevel.Quote());
        settings.FiltrationLevel = *level;
    }

    Logging = CreateLoggingService("console", settings);
    Logging->Start();

    Log = Logging->CreateLog("BLOCKSTORE_TEST");
}

void TTestInitiator::WaitForShutdown()
{
    with_lock (WaitMutex) {
        while (AtomicGet(ShouldStop) == 0) {
            WaitCondVar.WaitT(WaitMutex, WaitTimeout);
        }
    }
}

void TTestInitiator::Stop(int exitCode)
{
    AtomicSet(ExitCode, exitCode);
    AtomicSet(ShouldStop, 1);

    WaitCondVar.Signal();
}

void TTestInitiator::Init()
{
    Client = NBD::CreateClient(Logging, Options->ThreadsCount);
    Client->Start();

    ClientHandler = NBD::CreateClientHandler(
        Logging,
        Options->StructuredReply);

    auto connectAddress = TNetworkAddress(
        TUnixSocketPath(Options->SocketPath));

    Endpoint = Client->CreateEndpoint(
        connectAddress,
        ClientHandler,
        CreateBlockStoreStub());
    Endpoint->Start();
}

void TTestInitiator::Shoot()
{
    const auto startTime = Now();
    const auto deadline = Options->TestDuration
        ? Options->TestDuration.ToDeadLine(startTime)
        : TInstant::Max();

    LastReportTs = startTime;

    while (AtomicGet(ShouldStop) == 0) {
        if (auto* request = RequestSender->WaitForRequest(WaitTimeout)) {
            RequestSender->SendRequest(request);
        }

        if (deadline < Now()) {
            Stop(0);
        }

        ReportProgress();
    }

    while (!RequestSender->WaitForCompletion(WaitTimeout)) {
        ReportProgress();
    }

    const auto& stats = RequestSender->Stats;

    const auto seconds = (Now() - startTime).Seconds();
    const auto ops = stats.NumReadOps + stats.NumWriteOps;
    const auto bytes = stats.BytesRead + stats.BytesWritten;

    Cout << "===  Total  ===\n"
        << "Reads        : " << stats.NumReadOps << "\n"
        << "Writes       : " << stats.NumWriteOps << "\n"
        << "Bytes read   : " << stats.BytesRead << "\n"
        << "Bytes written: " << stats.BytesWritten << "\n\n";

    Cout << "=== Average ===\n"
        << "IOPS  : " << (ops / seconds) << "\n"
        << "BW(r) : " << FormatByteSize(stats.BytesRead / seconds) << "/s\n"
        << "BW(w) : " << FormatByteSize(stats.BytesWritten / seconds) << "/s\n"
        << "BW(rw): " << FormatByteSize(bytes / seconds) << "/s\n\n";

    const TVector<TPercentileDesc> percentiles {
        { 0.01,   "    1" },
        { 0.10,   "   10" },
        { 0.25,   "   25" },
        { 0.50,   "   50" },
        { 0.75,   "   75" },
        { 0.90,   "   90" },
        { 0.95,   "   95" },
        { 0.98,   "   98" },
        { 0.99,   "   99" },
        { 0.995,  " 99.5" },
        { 0.999,  " 99.9" },
        { 0.9999, "99.99" },
        { 1.0000, "100.0" },
    };

    Cout << "=== Latency (mcs) ===\n";
    for (const auto& [p, name]: percentiles) {
        Cout << name << " : " << RequestSender->Latency.GetValueAtPercentile(100 * p) << "\n";
    }
}

void TTestInitiator::ReportProgress()
{
    const auto now = Now();
    if (now - LastReportTs > ReportInterval) {
        const auto& stats = RequestSender->Stats - RequestSender->PrevStats;

        const auto seconds = (now - LastReportTs).Seconds();
        const auto ops = stats.NumReadOps + stats.NumWriteOps;
        const auto bytes = stats.BytesRead + stats.BytesWritten;

        STORAGE_INFO(
            "IOPS  : " << (ops / seconds) << " "
            "BW(r) : " << FormatByteSize(stats.BytesRead / seconds) << "/s "
            "BW(w) : " << FormatByteSize(stats.BytesWritten / seconds) << "/s "
            "BW(rw): " << FormatByteSize(bytes / seconds) << "/s"
        );

        RequestSender->PrevStats = RequestSender->Stats;
        LastReportTs = now;
    }
}

void TTestInitiator::Term()
{
    if (Endpoint) {
        Endpoint->Stop();
    }

    if (Client) {
        Client->Stop();
    }

    if (Logging) {
        Logging->Stop();
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IRunnablePtr CreateTestInitiator(TOptionsPtr options)
{
    return std::make_shared<TTestInitiator>(std::move(options));
}

}   // namespace NCloud::NBlockStore
