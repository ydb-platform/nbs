#pragma once

#include <util/generic/string.h>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

void AddToCGroups(pid_t pid, const TVector<TString>& cgroups);

}   // namespace NCloud::NBlockStore
