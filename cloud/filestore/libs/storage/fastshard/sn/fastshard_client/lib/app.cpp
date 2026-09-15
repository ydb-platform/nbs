#include "app.h"

#include "factory.h"

#include <cloud/filestore/libs/storage/fastshard/bootstrap/core.h>

#include <library/cpp/getopt/small/last_getopt.h>

#include <util/generic/scope.h>
#include <util/generic/singleton.h>
#include <util/generic/yexception.h>
#include <util/string/join.h>
#include <util/system/progname.h>

#include <signal.h>

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
    opts.SetFreeArgsNum(1);

    opts.SetTitle("Command line fastshard storage node client");
    opts.SetFreeArgTitle(0, "<command>", JoinSeq(" | ", GetCommandNames()));

    try {
        if (argc < 2) {
            ythrow TUsageException() << "not enough arguments";
        }

        if (argc == 2) {
            TStringBuf arg(argv[1]);
            if (arg == "-h" || arg == "--help") {
                opts.PrintUsage(GetProgramName());
                return 0;
            }
        }

        auto name = NormalizeCommand(argv[1]);
        Command = GetCommand(name);
        if (!Command) {
            ythrow yexception() << "unknown command: " << name;
        }

        Init();
        Y_DEFER
        {
            //
            // A stopped command may still have its fiber blocked in I/O;
            // the runtime is then left to die with the process.
            //

            if (!Command->IsStopped()) {
                Destroy();
            }
        };

        return Command->Run(argc - 1, argv + 1) ? 0 : 1;
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

    struct sigaction sa = {};
    sa.sa_handler = Shutdown;

    sigaction(SIGINT, &sa, nullptr);
    sigaction(SIGTERM, &sa, nullptr);
}

}   // namespace NCloud::NFileStore::NStorage::NFastShard::NClient
