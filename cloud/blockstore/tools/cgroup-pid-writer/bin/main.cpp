#include <cloud/blockstore/tools/cgroup-pid-writer/lib/app.h>

#include <library/cpp/getopt/small/last_getopt.h>

////////////////////////////////////////////////////////////////////////////////

int main(int argc, char** argv)
{
    using namespace NLastGetopt;

    NCloud::NBlockStore::TOptions options;

    TOpts opts;
    opts.AddHelpOption();
    opts.SetFreeArgsMin(1);
    opts.SetFreeArgsMax(NLastGetopt::TOpts::UNLIMITED_ARGS);
    opts.GetTrailingArgSpec().Title("CGROUP").CompletionArgHelp("cgroup path");

    opts.AddLongOption('p', "pid", "pid of process")
        .RequiredArgument("PID")
        .StoreResult(&options.Pid);

    try {
        TOptsParseResultException res(&opts, argc, argv);
        for (std::string arg: res.GetFreeArgs()) {
            options.Cgroups.push_back(TString(arg.c_str()));
        }
    } catch (...) {
        Cerr << CurrentExceptionMessage() << Endl;
        return 1;
    }

    options.CgroupRootPath = "/sys/fs/cgroup";

    return NCloud::NBlockStore::AppMain(std::move(options));
}
