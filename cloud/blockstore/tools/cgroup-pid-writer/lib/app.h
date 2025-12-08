#pragma once

#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NCloud::NBlockStore {

struct TOptions
{
    pid_t Pid = 0;
    TVector<TString> Cgroups;
    TString CgroupRootPath;
};

int AppMain(TOptions options);

}   // namespace NCloud::NBlockStore
