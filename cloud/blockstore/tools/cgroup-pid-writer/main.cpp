#include <cloud/blockstore/libs/common/cgroups_helpers.h>

#include <util/generic/vector.h>
#include <util/string/cast.h>
#include <util/string/printf.h>

int main(int argc, char** argv)
{
    if (argc < 3) {
        Cerr << "Not enough arguments, Usage: " << argv[0]
             << " <pid> <cgroup_dir> [<cgroup_dir> ...]" << Endl;
        return 1;
    }

    auto pid = FromString<pid_t>(argv[1]);

    TVector<TString> cgroupFiles;
    for (ui64 i = 2; i < static_cast<ui64>(argc); ++i) {
        cgroupFiles.emplace_back(argv[i]);
    }

    try {
        NCloud::NBlockStore::AddToCGroups(pid, cgroupFiles);
    } catch (const std::exception& e) {
        Cerr << Sprintf("Can't add pid[%d] to cgroup file: %s", pid, e.what())
             << Endl;
        return 1;
    }
}
