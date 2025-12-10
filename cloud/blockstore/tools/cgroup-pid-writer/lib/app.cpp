#include "app.h"

#include <library/cpp/getopt/small/last_getopt.h>

#include <util/folder/path.h>
#include <util/generic/vector.h>
#include <util/string/cast.h>
#include <util/string/printf.h>
#include <util/system/file.h>

#include <algorithm>

namespace NCloud::NBlockStore {

using namespace NLastGetopt;

namespace {

////////////////////////////////////////////////////////////////////////////////

bool BelongsTo(const TFsPath& prefix, const TFsPath& path)
{
    auto prefixSplit = prefix.PathSplit();
    auto pathSplit = path.PathSplit();
    auto [it, _] = std::ranges::mismatch(prefixSplit, pathSplit);

    return it == prefixSplit.end();
}

void AddToCGroups(pid_t pid, const TVector<TFsPath>& cgroups)
{
    const auto flags = EOpenModeFlag::OpenExisting | EOpenModeFlag::WrOnly |
                       EOpenModeFlag::ForAppend;

    const TString line = ToString(pid) + '\n';

    for (auto& cgroup: cgroups) {
        TFile file{cgroup / "cgroup.procs", flags};

        file.Write(line.data(), line.size());
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void TOptions::Parse(int argc, char** argv)
{
    TOpts opts;
    opts.AddHelpOption();
    opts.SetFreeArgsMin(1);
    opts.SetFreeArgsMax(TOpts::UNLIMITED_ARGS);
    opts.GetTrailingArgSpec().Title("CGROUP").CompletionArgHelp("cgroup path");

    opts.AddLongOption('p', "pid", "pid of process")
        .RequiredArgument("PID")
        .StoreResult(&Pid);

    TOptsParseResultException res(&opts, argc, argv);
    for (std::string arg: res.GetFreeArgs()) {
        Cgroups.push_back(arg.c_str());
    }
}

int AppMain(TFsPath cgroupRootPath, TOptions options)
{
    try {
        TVector<TFsPath> cgroups;
        for (const auto& cgroup: options.Cgroups) {
            cgroups.emplace_back(cgroup.RealPath());
        }

        Y_ENSURE(options.Pid);
        Y_ENSURE(AllOf(cgroups, std::bind_front(&BelongsTo, cgroupRootPath)));

        AddToCGroups(options.Pid, cgroups);
    } catch (...) {
        Cerr << "Can't add pid[" << options.Pid
             << "] to cgroup file: " << CurrentExceptionMessage() << Endl;
        return 1;
    }

    return 0;
}

}   // namespace NCloud::NBlockStore
