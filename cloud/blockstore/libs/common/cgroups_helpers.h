#pragma once

#include <util/folder/path.h>
#include <util/generic/fwd.h>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

bool IsPrefix(const TFsPath& prefix, const TFsPath& path);

void AddToCGroups(pid_t pid, const TVector<TString>& cgroups);

}   // namespace NCloud::NBlockStore
