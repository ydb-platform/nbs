#include "cgroups_helpers.h"

#include <util/folder/path.h>
#include <util/string/cast.h>
#include <util/system/file.h>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

bool IsPrefix(const TFsPath& prefix, const TFsPath& path)
{
    TFsPath realPath;
    try {
        realPath = path.RealPath();
    } catch (...) {
        return false;
    }

    TPathSplit pathSplit = realPath.PathSplit();

    auto prefixSplit = prefix.PathSplit();

    if (prefixSplit.size() > pathSplit.size()) {
        return false;
    }

    for (size_t i = 0; i < prefixSplit.size(); ++i) {
        if (prefixSplit[i] != pathSplit[i]) {
            return false;
        }
    }
    return true;
}

void AddToCGroups(pid_t pid, const TVector<TString>& cgroups)
{
    const auto flags = EOpenModeFlag::OpenExisting | EOpenModeFlag::WrOnly |
                       EOpenModeFlag::ForAppend;

    const TString line = ToString(pid) + '\n';

    for (auto& cgroup: cgroups) {
        auto pathToProcsFile = (TFsPath{cgroup} / "cgroup.procs").RealPath();

        TFile file{pathToProcsFile, flags};

        file.Write(line.data(), line.size());
    }
}

}   // namespace NCloud::NBlockStore
