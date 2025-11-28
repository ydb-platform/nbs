#include "cgroups_helpers.h"

#include <util/folder/path.h>
#include <util/string/cast.h>
#include <util/system/file.h>

namespace NCloud::NBlockStore {



void AddToCGroups(pid_t pid, const TVector<TString>& cgroups)
{
    const auto flags = EOpenModeFlag::OpenExisting | EOpenModeFlag::WrOnly |
                       EOpenModeFlag::ForAppend;

    const TString line = ToString(pid) + '\n';

    for (auto& cgroup: cgroups) {
        TFile file{TFsPath{cgroup} / "cgroup.procs", flags};

        file.Write(line.data(), line.size());
    }
}

}   // namespace NCloud::NBlockStore
