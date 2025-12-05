#include <cloud/blockstore/libs/common/cgroups_helpers.h>

#include <util/folder/path.h>
#include <util/generic/vector.h>
#include <util/string/cast.h>
#include <util/string/printf.h>

////////////////////////////////////////////////////////////////////////////////

namespace {

const TFsPath CgroupDirectory("/sys/fs/cgroup");

}   // namespace

int main(int argc, char** argv)
{
    if (argc < 3) {
        Cerr << "Not enough arguments, Usage: " << argv[0]
             << " <pid> <cgroup_dir> [<cgroup_dir> ...]" << Endl;
        return 1;
    }

    auto pid = FromString<pid_t>(argv[1]);

    TVector<TString> cgroupFiles;
    for (int i = 2; i < argc; ++i) {
        Y_ABORT_UNLESS(NCloud::NBlockStore::IsPrefix(argv[i], CgroupDirectory));
        cgroupFiles.emplace_back(argv[i]);
    }

    try {
        NCloud::NBlockStore::AddToCGroups(pid, cgroupFiles);
    } catch (const std::exception& e) {
        Cerr << "Can't add pid[" << pid << "] to cgroup file: " << e.what()
             << Endl;
        return 1;
    }
}
