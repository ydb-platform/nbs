#include "app.h"

#include "command.h"

#include <library/cpp/getopt/small/last_getopt.h>

#include <util/generic/singleton.h>
#include <util/generic/yexception.h>
#include <util/string/join.h>
#include <util/system/progname.h>

#include <signal.h>

#include <cstdio>
#include <new>

namespace NCloud::NFileStore::NClient {

using namespace NLastGetopt;

////////////////////////////////////////////////////////////////////////////////

TApp& TApp::Instance()
{
    return *Singleton<TApp>();
}

int TApp::Run(
    std::shared_ptr<TClientFactories> clientFactories,
    int argc,
    char** argv)
{
    TOpts opts;
    opts.AddHelpOption('h');
    opts.AddVersionOption();
    opts.SetFreeArgsNum(1);

    opts.SetTitle("Command line filestore client");
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
            if (arg == "-V" || arg == "--svnrevision") {
                NLastGetopt::PrintVersionAndExit(nullptr);
                return 0;
            }
        }

        auto name = NormalizeCommand(*std::next(argv));

        Command = GetCommand(name);
        if (!Command) {
            ythrow yexception() << "unknown command: " << name;
        }
        Command->SetClientFactories(std::move(clientFactories));
        return Command->Run(argc - 1, std::next(argv));
    } catch (const TUsageException& e) {
        Cerr << FormatCmdLine(argc, argv)
            << " failed: " << CurrentExceptionMessage()
            << Endl;

        if (Command) {
            Command->GetOpts().PrintUsage(GetProgramName());
        } else {
            opts.PrintUsage(GetProgramName());
        }
        return 1;
    } catch (...) {
        Cerr << FormatCmdLine(argc, argv)
            << " failed: " << CurrentExceptionMessage()
            << Endl;
        return 1;
    }
}

void TApp::Stop(int exitCode)
{
    if (Command) {
        Command->Stop(exitCode);
    }
}

TString TApp::FormatCmdLine(int argc, char** argv)
{
    TStringBuilder result;
    result << GetProgramName();

    if (argc) {
        result << " " << JoinRange(" ", &argv[0], &argv[argc]).Quote();
    }
    return result;
}

////////////////////////////////////////////////////////////////////////////////

void ProcessSignal(int signum)
{
    if (signum == SIGINT || signum == SIGTERM) {
        TApp::Instance().Stop(0);
    }
}

void ConfigureSignals()
{
    std::set_new_handler(abort);

    // make sure that errors can be seen by everybody :)
    setvbuf(stdout, nullptr, _IONBF, 0);
    setvbuf(stderr, nullptr, _IONBF, 0);

    // mask signals
    signal(SIGPIPE, SIG_IGN);

    struct sigaction sa = {};
    sa.sa_handler = ProcessSignal;

    sigaction(SIGINT, &sa, nullptr);
    sigaction(SIGTERM, &sa, nullptr);
    sigaction(SIGUSR1, &sa, nullptr);   // see fuse/driver for details
}

}   // namespace NCloud::NFileStore::NClient
