#include <library/cpp/getopt/small/last_getopt.h>

#include <util/generic/algorithm.h>
#include <util/generic/vector.h>
#include <util/system/file.h>

#include <filesystem>
#include <functional>

namespace {

namespace NFs = std::filesystem;

////////////////////////////////////////////////////////////////////////////////

void AppendToFile(const NFs::path& path, TStringBuf line)
{
    const auto flags = EOpenModeFlag::OpenExisting | EOpenModeFlag::WrOnly |
                       EOpenModeFlag::ForAppend;

    TFile file{path / "cgroup.procs", flags};
    file.Write(line.data(), line.size());
}

bool IsPrefix(const NFs::path& prefix, const NFs::path& path)
{
    auto [it, _] =
        std::mismatch(prefix.begin(), prefix.end(), path.begin(), path.end());

    return it == prefix.end();
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

int main(int argc, char** argv)
{
    using namespace NLastGetopt;

    pid_t pid = 0;
    TVector<NFs::path> cgroups;
    NFs::path cgroupPathPrefix("/sys/fs/cgroup");

    TOpts opts;
    opts.AddHelpOption();
    opts.SetFreeArgsMin(1);
    opts.SetFreeArgsMax(NLastGetopt::TOpts::UNLIMITED_ARGS);
    opts.GetTrailingArgSpec()
        .Title("CGROUP")
        .CompletionArgHelp("cgroup path");

    opts.AddLongOption("cgroup-root-folder", "cgroup root folder")
        .RequiredArgument("PATH")
        .StoreResultDef(&cgroupPathPrefix);

    opts.AddLongOption('p', "pid", "pid of process")
        .RequiredArgument("PID")
        .StoreResult(&pid);

    try {
        TOptsParseResultException res(&opts, argc, argv);
        for (std::string arg: res.GetFreeArgs()) {
            cgroups.push_back(NFs::path{arg});
        }
    } catch (...) {
        Cerr << CurrentExceptionMessage() << Endl;
        return 1;
    }

    try {
        Y_ENSURE(pid);
        Y_ENSURE(AllOf(cgroups, std::bind_front(IsPrefix, cgroupPathPrefix)));

        const TString line = ToString(pid) + '\n';

        for (const auto& cgroup: cgroups) {
            AppendToFile(cgroup, line);
        }
    } catch (...) {
        Cerr << "Error: " << CurrentExceptionMessage() << '\n';

        return 1;
    }

    return 0;
}
