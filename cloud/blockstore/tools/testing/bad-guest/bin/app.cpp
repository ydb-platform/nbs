#include "app.h"

#include "options.h"

#include <cloud/blockstore/tools/testing/bad-guest/lib/io.h>

#include <util/generic/singleton.h>

#include <csignal>

namespace NCloud::NBlockStore {

namespace {

////////////////////////////////////////////////////////////////////////////////

class TApp
{
private:
    std::unique_ptr<TIO> IO;
    std::atomic<bool> ShouldStop = false;

public:
    static TApp* GetInstance()
    {
        return Singleton<TApp>();
    }

    int Run(const TOptions& options)
    {
        IO = std::make_unique<TIO>(options.FilePath, options.BlockSize);

        ui64 blockIndex = 0;
        ui32 i = 0;
        while (!ShouldStop.load() && blockIndex < options.TotalBlockCount) {
            const ui64 sz = options.BlockSize * options.ChunkBlockCount;
            const char c1 = 'a' + i % ('z' - 'a' + 1);
            const char c2 = 'A' + i % ('Z' - 'A' + 1);

            TString data1(sz, c1);
            TString data2(sz, c2);
            data1[sz - 1] = '\n';
            data2[sz - 1] = '\n';

            IO->AlternatingWrite(
                blockIndex,
                {TStringBuf(data1), TStringBuf(data2)});

            blockIndex += options.ChunkBlockCount;
            ++i;
        }

        return 0;
    }

    void Stop()
    {
        ShouldStop.store(true);
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
    (void)setvbuf(stdout, nullptr, _IONBF, 0);
    (void)setvbuf(stderr, nullptr, _IONBF, 0);

    // mask signals
    (void)signal(SIGPIPE, SIG_IGN);
    (void)signal(SIGINT, ProcessSignal);
    (void)signal(SIGTERM, ProcessSignal);
}

int AppMain(const TOptions& options)
{
    return TApp::GetInstance()->Run(options);
}

void AppStop()
{
    TApp::GetInstance()->Stop();
}

}   // namespace NCloud::NBlockStore
