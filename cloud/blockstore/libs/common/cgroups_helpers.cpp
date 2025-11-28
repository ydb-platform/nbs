#include "cgroups_helpers.h"

#include <util/folder/path.h>
#include <util/string/cast.h>
#include <util/system/file.h>

#include <filesystem>

namespace NCloud::NBlockStore {

namespace {

const TFsPath CgroupDirectory("sys/fs/cgroup");

bool IsPrefix(const TFsPath& path, const TFsPath& prefix)
{
    auto pathSplit = path.PathSplit();
    auto prefixSplit = prefix.PathSplit();

    for (size_t i = 0; i < prefixSplit.size(); ++i) {
        if (prefixSplit[i] != pathSplit[i]) {
            return false;
        }
    }
    return true;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void AddToCGroups(pid_t pid, const TVector<TString>& cgroups)
{
    const auto flags = EOpenModeFlag::OpenExisting | EOpenModeFlag::WrOnly |
                       EOpenModeFlag::ForAppend;

    const TString line = ToString(pid) + '\n';

    for (auto& cgroup: cgroups) {
        auto pathToProcsFile = (TFsPath{cgroup} / "cgroup.procs").RealPath();

        if (!IsPrefix(pathToProcsFile, CgroupDirectory)) {
            throw yexception() << "cgroup.procs[" << pathToProcsFile
                               << "] file is not in " << CgroupDirectory;
        }

        TFile file{pathToProcsFile, flags};

        file.Write(line.data(), line.size());
    }
}

}   // namespace NCloud::NBlockStore
