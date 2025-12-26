#pragma once

#include <util/folder/path.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

struct TOptions
{
    pid_t Pid = 0;
    TVector<TFsPath> Cgroups;

    void Parse(int argc, char** argv);
};

int AppMain(TFsPath cgroupRootPath, TOptions options);

}   // namespace NCloud::NBlockStore
