#include "target.h"

#include "options.h"
#include "probes.h"
#include "runnable.h"
#include "storage.h"

#include <cloud/blockstore/libs/service/context.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/format.h>
#include <cloud/storage/core/libs/diagnostics/histogram.h>

#include <util/datetime/base.h>
#include <util/datetime/cputimer.h>
#include <util/generic/intrlist.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/random/random.h>
#include <library/cpp/deprecated/atomic/atomic.h>
#include <util/system/condvar.h>
#include <util/system/mutex.h>

namespace NCloud::NBlockStore {

LWTRACE_USING(BLOCKSTORE_TEST_PROVIDER);

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr TDuration REPORT_INTERVAL = TDuration::Seconds(5);

////////////////////////////////////////////////////////////////////////////////

void DumpHistogram(const THistogramBase& hist)
{
    static const TVector<TPercentileDesc> percentiles {
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

    for (const auto& [p, name]: percentiles) {
        Cout << name << " : "
             << hist.GetValueAtPercentile(100 * p)
             << Endl;
    }
    Cout << Endl;
}

////////////////////////////////////////////////////////////////////////////////

struct TRequest : TIntrusiveSListItem<TRequest>
{
    TStorageBuffer Buffer;

    bool WriteRequest;
    ui32 BlockIndex;
    ui32 BlocksCount;

    ui64 SendTs;
    ui64 RecvTs;

    NProto::TError Response;
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
private:
    const TOptionsPtr Options;
    const IStoragePtr Storage;

    TVector<TRequest> Requests;
    TIntrusiveSList<TRequest> CompletedRequests;
    size_t CompletedRequestsCount = 0;

    TMutex Mutex;
    TCondVar CondVar;

public:
    TRequestStats Stats;
    TRequestStats PrevStats;

    THistogramBase ReadLatency { 1, 100000000, 3 };
    THistogramBase WriteLatency { 1, 100000000, 3 };

    const float CyclesPerNanosecond = GetCyclesPerMillisecond() / 1000000.;

public:
    TRequestSender(TOptionsPtr options, IStoragePtr storage)
        : Options(std::move(options))
        , Storage(std::move(storage))
    {
        Requests.resize(Options->MaxIoDepth);

        for (auto& request: Requests) {
            request.Buffer = Storage->AllocateBuffer(
                Options->BlockSize * Options->MaxBlocksCount);

            CompletedRequests.PushFront(&request);
            ++CompletedRequestsCount;
        }
    }

    void SendRequest(TRequest* request)
    {
        request->WriteRequest = RandomNumber(100ul) < Options->WriteRate;

        request->BlocksCount = Options->MinBlocksCount;
        if (Options->MinBlocksCount < Options->MaxBlocksCount) {
            request->BlocksCount += RandomNumber(
                Options->MaxBlocksCount - Options->MinBlocksCount);
        }

        request->BlockIndex = RandomNumber(
            Options->BlocksCount - request->BlocksCount);

        request->SendTs = GetCycleCount();

        if (request->WriteRequest) {
            WriteBlocks(request);
        } else {
            ReadBlocks(request);
        }
    }

    TRequest* WaitForRequest(TDuration timeout)
    {
        TRequest* request = nullptr;
        with_lock (Mutex) {
            if (CompletedRequests) {
                request = CompletedRequests.Front();

                CompletedRequests.PopFront();
                --CompletedRequestsCount;
            } else {
                CondVar.WaitT(Mutex, timeout);
            }
        }

        if (request) {
            ProcessResponse(request);
        }
        return request;
    }

    bool WaitForCompletion(TDuration timeout)
    {
        bool completed = false;
        with_lock (Mutex) {
            if (CompletedRequestsCount == Requests.size()) {
                completed = true;
            } else {
                CondVar.WaitT(Mutex, timeout);
            }
        }

        if (completed) {
            for (auto& request: CompletedRequests) {
                ProcessResponse(&request);
            }
        }
        return completed;
    }

private:
    void ReadBlocks(TRequest* request)
    {
        auto callContext = MakeIntrusive<TCallContext>(CreateRequestId());

        auto req = std::make_shared<NProto::TReadBlocksRequest>();
        req->SetBlockSize(Options->BlockSize);
        req->SetBlockIndex(request->BlockIndex);
        req->SetBlocksCount(request->BlocksCount);

        TGuardedSgList sglist({{
            request->Buffer.get(),
            request->BlocksCount * Options->BlockSize
        }});

        LWTRACK(
            RequestStarted,
            callContext->LWOrbit,
            callContext->RequestId,
            "ReadBlocks");

        auto future = Storage->ReadBlocks(
            callContext,
            std::move(req),
            sglist);

        future.Subscribe([
            =, this,
            callContext = std::move(callContext),
            sglist = std::move(sglist)
        ] (auto future) {
            Y_UNUSED(sglist);

            request->RecvTs = GetCycleCount();
            request->Response = ExtractResponse(future);

            LWTRACK(
                RequestCompleted,
                callContext->LWOrbit,
                callContext->RequestId);

            HandleResponse(request);
        });
    }

    void WriteBlocks(TRequest* request)
    {
        auto callContext = MakeIntrusive<TCallContext>(CreateRequestId());

        auto req = std::make_shared<NProto::TWriteBlocksRequest>();
        req->SetBlockSize(Options->BlockSize);
        req->SetBlockIndex(request->BlockIndex);
        req->SetBlocksCount(request->BlocksCount);

        TGuardedSgList sglist({{
            request->Buffer.get(),
            request->BlocksCount * Options->BlockSize
        }});

        LWTRACK(
            RequestStarted,
            callContext->LWOrbit,
            callContext->RequestId,
            "WriteBlocks");

        auto future = Storage->WriteBlocks(
            callContext,
            std::move(req),
            sglist);

        future.Subscribe([
            =, this,
            callContext = std::move(callContext),
            sglist = std::move(sglist)
        ] (auto future) {
            Y_UNUSED(sglist);

            request->RecvTs = GetCycleCount();
            request->Response = ExtractResponse(future);

            LWTRACK(
                RequestCompleted,
                callContext->LWOrbit,
                callContext->RequestId);

            HandleResponse(request);
        });
    }

    void HandleResponse(TRequest* request)
    {
        with_lock (Mutex) {
            CompletedRequests.PushFront(request);
            ++CompletedRequestsCount;

            CondVar.Signal();
        }
    }

    void ProcessResponse(TRequest* request)
    {
        if (!request->SendTs) {
            return;
        }

        Y_ABORT_UNLESS(!HasError(request->Response), "%s", FormatError(request->Response).c_str());

        if (request->WriteRequest) {
            ++Stats.NumWriteOps;
            Stats.BytesWritten += request->BlocksCount * Options->BlockSize;
            WriteLatency.RecordValue((request->RecvTs - request->SendTs) / CyclesPerNanosecond);
        } else {
            ++Stats.NumReadOps;
            Stats.BytesRead += request->BlocksCount * Options->BlockSize;
            ReadLatency.RecordValue((request->RecvTs - request->SendTs) / CyclesPerNanosecond);
        }
    }
};

using TRequestSenderPtr = std::shared_ptr<TRequestSender>;

////////////////////////////////////////////////////////////////////////////////

class TTestInitiator final
    : public IRunnable
{
private:
    const TOptionsPtr Options;
    const IStoragePtr Storage;

    TRequestSenderPtr RequestSender;

    TMutex WaitMutex;
    TCondVar WaitCondVar;

    TAtomic ShouldStop = 0;
    TAtomic ExitCode = 0;

public:
    TTestInitiator(TOptionsPtr options, IStoragePtr storage)
        : Options(std::move(options))
        , Storage(std::move(storage))
    {
        RequestSender = std::make_shared<TRequestSender>(Options, Storage);
    }

    int Run() override
    {
        Shoot();
        WaitForShutdown();

        return AtomicGet(ExitCode);
    }

    void Stop(int exitCode) override
    {
        AtomicSet(ExitCode, exitCode);
        AtomicSet(ShouldStop, 1);

        WaitCondVar.Signal();
    }

private:
    void Shoot()
    {
        const auto startTs = Now();
        const auto deadline = Options->TestDuration
            ? Options->TestDuration.ToDeadLine(startTs)
            : TInstant::Max();

        auto now = startTs;
        auto lastReportTs = startTs;

        while (AtomicGet(ShouldStop) == 0) {
            if (auto* request = RequestSender->WaitForRequest(WAIT_TIMEOUT)) {
                RequestSender->SendRequest(request);
            }

            now = Now();
            if (now - lastReportTs > REPORT_INTERVAL) {
                ReportProgress(now - lastReportTs);
                lastReportTs = now;
            }

            if (deadline < now) {
                Stop(0);
            }
        }

        while (!RequestSender->WaitForCompletion(WAIT_TIMEOUT)) {
            now = Now();
            if (now - lastReportTs > REPORT_INTERVAL) {
                ReportProgress(now - lastReportTs);
                lastReportTs = now;
            }
        }

        ReportFinalStats(Now() - startTs);
    }

    void ReportProgress(TDuration elapsed)
    {
        const auto& stats = RequestSender->Stats - RequestSender->PrevStats;
        const auto seconds = elapsed.SecondsFloat();

        Cout << "IOPS: " << round((stats.NumReadOps + stats.NumWriteOps) / seconds)
            << ", BW(r): " << FormatByteSize(stats.BytesRead / seconds) << "/s"
            << ", BW(w): " << FormatByteSize(stats.BytesWritten / seconds) << "/s"
            << ", BW(rw): " << FormatByteSize((stats.BytesRead + stats.BytesWritten) / seconds) << "/s"
            << Endl;

        RequestSender->PrevStats = RequestSender->Stats;
    }

    void ReportFinalStats(TDuration elapsed)
    {
        const auto& stats = RequestSender->Stats;
        const auto seconds = elapsed.SecondsFloat();

        Cout << "=== Total ===" << Endl
            << "Reads    : " << stats.NumReadOps << Endl
            << "Writes   : " << stats.NumWriteOps << Endl
            << "Bytes(r) : " << FormatByteSize(stats.BytesRead) << Endl
            << "Bytes(w) : " << FormatByteSize(stats.BytesWritten) << Endl
            << Endl;

        Cout << "=== Average ===" << Endl
            << "IOPS     : " << round((stats.NumReadOps + stats.NumWriteOps) / seconds) << Endl
            << "Lat(r)   : " << round(RequestSender->ReadLatency.GetMean()) << " ns" << Endl
            << "Lat(w)   : " << round(RequestSender->WriteLatency.GetMean()) << " ns" << Endl
            << "BW(r)    : " << FormatByteSize(stats.BytesRead / seconds) << "/s" << Endl
            << "BW(w)    : " << FormatByteSize(stats.BytesWritten / seconds) << "/s" << Endl
            << "BW(rw)   : " << FormatByteSize((stats.BytesRead + stats.BytesWritten) / seconds) << "/s" << Endl
            << Endl;

        if (stats.BytesRead) {
            Cout << "=== Read Latency (ns) ===" << Endl;
            DumpHistogram(RequestSender->ReadLatency);
        }

        if (stats.BytesWritten) {
            Cout << "=== Write Latency (ns) ===" << Endl;
            DumpHistogram(RequestSender->WriteLatency);
        }
    }

    void WaitForShutdown()
    {
        with_lock (WaitMutex) {
            while (AtomicGet(ShouldStop) == 0) {
                WaitCondVar.WaitT(WaitMutex, WAIT_TIMEOUT);
            }
        }
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IRunnablePtr CreateTestInitiator(
    TOptionsPtr options,
    IStoragePtr storage)
{
    return std::make_shared<TTestInitiator>(
        std::move(options),
        std::move(storage));
}

}   // namespace NCloud::NBlockStore
