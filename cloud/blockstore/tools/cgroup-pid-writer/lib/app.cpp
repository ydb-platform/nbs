#include "app.h"

#include <library/cpp/getopt/small/last_getopt.h>

#include <util/folder/path.h>
#include <util/generic/vector.h>
#include <util/string/cast.h>
#include <util/string/printf.h>
#include <util/system/file.h>

#include <algorithm>

namespace NCloud::NBlockStore {

namespace {

////////////////////////////////////////////////////////////////////////////////

bool IsPrefix(const TFsPath& prefix, const TFsPath& path)
{
    auto prefixSplit = prefix.PathSplit();
    auto pathSplit = path.PathSplit();
    auto [it, _] = std::ranges::mismatch(prefixSplit, pathSplit);

    return it == prefixSplit.end();
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

}   // namespace

////////////////////////////////////////////////////////////////////////////////

int AppMain(TOptions options)
{
    using namespace NLastGetopt;

    try {
        Y_ENSURE(options.Pid);
        Y_ENSURE(AllOf(
            options.Cgroups,
            [&](const auto& dir)
            {
                TFsPath realPath;
                try {
                    realPath = TFsPath(dir).RealPath();
                } catch (...) {
                    return false;
                }

                return NCloud::NBlockStore::IsPrefix(
                    options.CgroupRootPath,
                    realPath);
            }));

        NCloud::NBlockStore::AddToCGroups(options.Pid, options.Cgroups);
    } catch (...) {
        Cerr << "Can't add pid[" << options.Pid
             << "] to cgroup file: " << CurrentExceptionMessage() << Endl;
        return 1;
    }

    return 0;
}

}   // namespace NCloud::NBlockStore
