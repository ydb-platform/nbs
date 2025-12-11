#include "app.h"

#include "options.h"

#include <cloud/blockstore/tools/testing/pd-metadata-bench/lib/table.h>

#include <util/datetime/base.h>
#include <util/generic/hash.h>
#include <util/generic/singleton.h>
#include <util/random/random.h>
#include <util/system/event.h>
#include <util/thread/lfstack.h>

#include <signal.h>

namespace NCloud::NBlockStore {

namespace {

////////////////////////////////////////////////////////////////////////////////

class TApp
{
private:
    std::unique_ptr<TMetaTable> Table;
    TAtomic ShouldStop = false;

public:
    static TApp* GetInstance()
    {
        return Singleton<TApp>();
    }

    int Run(TOptions options)
    {
        Table.reset(new TMetaTable(options.FilePath, options.BlockCount));
        TVector<NThreading::TFuture<void>> futures(options.IoDepth);
        TLockFreeStack<ui32> indices;
        for (ui32 i = 0; i < options.IoDepth; ++i) {
            indices.Enqueue(i);
        }
        TManualEvent ev;
        ev.Signal();

        struct TWriteResult
        {
            ui64 BlockIndex = 0;
            ui64 CommitId = 0;
        };

        TLockFreeStack<TWriteResult> writeResults;

        TVector<ui32> buf;
        ui64 requestCount = 0;
        ui64 lastRequestCount = 0;
        TInstant lastTs = Now();
        ui64 commitId = 0;

        TVector<TWriteResult> wrs;

        THashMap<ui64, ui64> block2commitId;

        while (!AtomicGet(ShouldStop)) {
            wrs.clear();
            writeResults.DequeueAllSingleConsumer(&wrs);
            for (const auto& wr: wrs) {
                block2commitId[wr.BlockIndex] = wr.CommitId;
            }

            buf.clear();
            indices.DequeueAllSingleConsumer(&buf);

            if (requestCount > lastRequestCount + 1000) {
                auto now = Now();
                auto diff = now - lastTs;
                if (diff > TDuration::Seconds(1)) {
                    auto iops = (requestCount - lastRequestCount) * 1'000'000 /
                                diff.MicroSeconds();
                    Cout << now << "\tIOPS=" << iops << Endl;

                    lastRequestCount = requestCount;
                    lastTs = now;
                }
            }

            for (auto i: buf) {
                ui64 blockIndex = RandomNumber(options.BlockCount);

                if (RandomNumber(100u) < options.WriteRate) {
                    ++commitId;
                    const ui64 checksum = 777;
                    TBlockMeta meta{commitId, checksum};
                    Cdbg << "write " << blockIndex << Endl;
                    futures[i] =
                        Table->Write(blockIndex, meta)
                            .Subscribe(
                                [&writeResults, blockIndex, commitId](auto)
                                {
                                    Cdbg
                                        << "adding write result: " << blockIndex
                                        << " -> " << commitId << Endl;
                                    writeResults.Enqueue(
                                        {blockIndex, commitId});
                                });
                } else {
                    ui64 expectedCommitId = 0;
                    if (auto p = block2commitId.FindPtr(blockIndex)) {
                        expectedCommitId = *p;
                    }
                    Cdbg << "read " << blockIndex << ", expect "
                         << expectedCommitId << Endl;
                    futures[i] =
                        Table->Read(blockIndex)
                            .Apply(
                                [blockIndex, expectedCommitId](const auto& f)
                                {
                                    auto actualCommitId = f.GetValue().CommitId;
                                    if (actualCommitId < expectedCommitId) {
                                        Cerr << "block " << blockIndex
                                             << ", stale commit id "
                                             << actualCommitId << " < "
                                             << expectedCommitId << Endl;

                                        Y_ABORT_UNLESS(0);
                                    }
                                });
                }

                futures[i].Subscribe(
                    [&indices, &ev, i](auto)
                    {
                        indices.Enqueue(i);
                        ev.Signal();
                    });

                ++requestCount;

                ev.Wait();
            }
        }

        WaitAll(futures).Wait();

        return 0;
    }

    void Stop()
    {
        AtomicSet(ShouldStop, true);
    }
};

////////////////////////////////////////////////////////////////////////////////

void ProcessSignal(int signum)
{
    if (signum == SIGINT || signum == SIGTERM) {
        AppStop();
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void ConfigureSignals()
{
    std::set_new_handler(abort);

    // make sure that errors can be seen by everybody :)
    setvbuf(stdout, nullptr, _IONBF, 0);
    setvbuf(stderr, nullptr, _IONBF, 0);

    // mask signals
    signal(SIGPIPE, SIG_IGN);
    signal(SIGINT, ProcessSignal);
    signal(SIGTERM, ProcessSignal);
}

int AppMain(TOptions options)
{
    return TApp::GetInstance()->Run(std::move(options));
}

void AppStop()
{
    TApp::GetInstance()->Stop();
}

}   // namespace NCloud::NBlockStore
