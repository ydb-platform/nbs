#include "app.h"

#include "factory.h"

#include <cloud/filestore/libs/storage/fastshard/bootstrap/core.h>

#include <library/cpp/getopt/small/last_getopt.h>

#include <util/generic/scope.h>
#include <util/generic/singleton.h>
#include <util/generic/yexception.h>
#include <util/string/join.h>
#include <util/system/progname.h>

#include <sched.h>
#include <signal.h>
#include <unistd.h>

#include <cstdio>
#include <new>

namespace NCloud::NFileStore::NStorage::NFastShard::NClient {

using namespace NLastGetopt;

////////////////////////////////////////////////////////////////////////////////

TApp& TApp::Instance()
{
    return *Singleton<TApp>();
}

void TApp::Shutdown()
{
    if (Command) {
        Command->Shutdown();
    }
}

int TApp::Run(int argc, const char* argv[])
{
    TOpts opts;
    opts.AddHelpOption('h');
    opts.AddVersionOption();
    opts.SetFreeArgsNum(1);

    opts.SetTitle("Command line fastshard storage node client");
    opts.SetFreeArgTitle(0, "<command>", JoinSeq(" | ", GetCommandNames()));

    try {
        if (argc < 2) {
            ythrow TUsageException() << "not enough arguments";
        }

        TStringBuf first(argv[1]);
        if (first == "-h" || first == "--help") {
            opts.PrintUsage(GetProgramName());
            return 0;
        }
        if (first == "-V" || first == "--svnrevision") {
            PrintVersionAndExit(nullptr);
            return 0;
        }

        auto name = NormalizeCommand(argv[1]);
        Command = GetCommand(name);
        if (!Command) {
            ythrow yexception() << "unknown command: " << name;
        }

        Command->ParseOpts(argc - 1, argv + 1);

        const int cpu = sched_getcpu();
        if (cpu >= 0) {
            cpu_set_t cpuMask;
            CPU_ZERO(&cpuMask);
            CPU_SET(cpu, &cpuMask);
            Init(cpuMask);
        } else {
            Init();
        }
        Y_DEFER
        {
            Destroy();
        };

        const bool ok = Command->Run();
        if (Command->IsStopped()) {
            //
            // The fiber may still be blocked in I/O inside the sn client,
            // which must not be destroyed under it.
            //

            Cout.Flush();
            Cerr.Flush();
            ::_exit(1);
        }

        return ok ? 0 : 1;
    } catch (const NLastGetopt::TException&) {
        Cerr << GetProgramName() << " failed: " << CurrentExceptionMessage()
             << Endl;
        if (Command) {
            Command->PrintUsage();
        } else {
            opts.PrintUsage(GetProgramName());
        }
        return 1;
    } catch (...) {
        Cerr << GetProgramName() << " failed: " << CurrentExceptionMessage()
             << Endl;
        return 1;
    }
}

void Shutdown(int signum)
{
    Y_UNUSED(signum);
    TApp::Instance().Shutdown();
}

void ConfigureSignals()
{
    std::set_new_handler(abort);

    // make sure that errors can be seen by everybody :)
    setvbuf(stdout, nullptr, _IONBF, 0);
    setvbuf(stderr, nullptr, _IONBF, 0);

    // a storage node closing the connection must surface as a send error,
    // not kill the process
    signal(SIGPIPE, SIG_IGN);

    // Shutdown only sets a flag that Run polls, so nothing needs the
    // interrupted syscall to fail: without SA_RESTART a signal landing on
    // a silk scheduler thread mid-read of stdin surfaces as EINTR and the
    // command fails with an I/O error instead of "command was stopped".
    struct sigaction sa = {};
    sa.sa_handler = Shutdown;
    sa.sa_flags = SA_RESTART;

    sigaction(SIGINT, &sa, nullptr);
    sigaction(SIGTERM, &sa, nullptr);
}

}   // namespace NCloud::NFileStore::NStorage::NFastShard::NClient
