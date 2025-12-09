#pragma once

#include <util/folder/path.h>
#include <util/generic/vector.h>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

struct TOptions
{
    pid_t Pid = 0;
    TVector<TFsPath> Cgroups;

    void Parse(int argc, char** argv);
};

////////////////////////////////////////////////////////////////////////////////

int AppMain(const TFsPath& cgroupRootPath, const TOptions& options);

}   // namespace NCloud::NBlockStore
