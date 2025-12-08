#include <cloud/blockstore/libs/common/cgroups_helpers.h>

#include <library/cpp/getopt/small/last_getopt.h>

#include <util/folder/path.h>
#include <util/generic/vector.h>
#include <util/string/cast.h>
#include <util/string/printf.h>

namespace {

const TFsPath CgroupDirectory("/sys/fs/cgroup");

////////////////////////////////////////////////////////////////////////////////

}   // namespace

////////////////////////////////////////////////////////////////////////////////

int main(int argc, char** argv)
{
    using namespace NLastGetopt;

    pid_t pid = 0;
    TVector<TString> cgroups;

    TOpts opts;
    opts.AddHelpOption();
    opts.SetFreeArgsMin(1);
    opts.SetFreeArgsMax(NLastGetopt::TOpts::UNLIMITED_ARGS);
    opts.GetTrailingArgSpec().Title("CGROUP").CompletionArgHelp("cgroup path");

    opts.AddLongOption('p', "pid", "pid of process")
        .RequiredArgument("PID")
        .StoreResult(&pid);

    try {
        TOptsParseResultException res(&opts, argc, argv);
        for (std::string arg: res.GetFreeArgs()) {
            cgroups.push_back(TString(arg));
        }
    } catch (...) {
        Cerr << CurrentExceptionMessage() << Endl;
        return 1;
    }

    try {
        Y_ENSURE(pid);
        Y_ENSURE(AllOf(
            cgroups,
            std::bind_front(NCloud::NBlockStore::IsPrefix, CgroupDirectory)));

        NCloud::NBlockStore::AddToCGroups(pid, cgroups);
    } catch (...) {
        Cerr << "Can't add pid[" << pid
             << "] to cgroup file: " << CurrentExceptionMessage() << Endl;
        return 1;
    }
}
